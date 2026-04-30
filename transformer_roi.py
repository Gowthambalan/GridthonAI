# ─────────────────────────────────────────────────────────────────────────────
# transformer_roi.py — ROI & Cost-Savings Endpoint
# ─────────────────────────────────────────────────────────────────────────────
#
# Monetises every cost-reduction model's output using APDCL (Indian) tariffs.
# Mirrors smart-meter response shape: { data: {stats, charts, tables}, ... }
#
# COST BUCKETS COMPUTED (each = one model's ROI lens):
#   1. MDI Penalty avoidance          (Model 2 — Peak Demand)
#   2. PF Penalty avoidance           (Model 3 — Reactive PF)
#   3. Phase-balancing copper loss    (Model 5 — Phase Imbalance)
#   4. Transformer right-sizing iron  (Model 10 — Load Factor)
#   5. Planned-vs-emergency LoL       (Model 9 — Loss of Life)
#   6. NTL / Theft revenue recovery   (Model 8 — NTL)
#   7. Overload failure prevention    (Model 7 + 13)
#
# ─────────────────────────────────────────────────────────────────────────────

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator, model_validator, Field
from typing import Optional
from datetime import datetime
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np

import state
from state import resolve_to_sections
from transformer_insights import (
    TARIFF_PER_KWH, MDI_PENALTY_PER_KVA, PF_PENALTY_PCT, PF_PENALTY_THRESHOLD,
    OVERLOAD_THRESH_PCT, LOL_CRITICAL_PCT,
    MODEL_KEY_MAP, MONGO_URI, DB_NAME, COL_AGG, COL_NTL, COL_LOL,
    _fetch_agg, _fetch_raw, _extract_dates, _build_series, paginate,
    resolve_transformers, _count_transformers,
)

router = APIRouter(tags=["ROI / Cost Savings"])

MAX_DATE_RANGE = 90

_mongo = MongoClient(MONGO_URI, maxPoolSize=15)
_db    = _mongo[DB_NAME]


# ═════════════════════════════════════════════════════════════════════════════
# APDCL TARIFF CONSTANTS (full breakdown)
# ═════════════════════════════════════════════════════════════════════════════
#
# These mirror APDCL's published industrial / commercial schedule. Adjust via
# env-vars in production. All rupee amounts are monthly unless noted.

COPPER_LOSS_SAVING_PCT        = 0.025   # 2.5% energy saving from phase balancing (typical)
IRON_LOSS_PER_KVA_PER_YEAR    = 450     # ₹/kVA/yr for oversized DT iron losses (no-load)
EMERGENCY_REPLACEMENT_COST    = 3_500_000  # ₹ — avg emergency DT replacement (incl. downtime)
PLANNED_REPLACEMENT_COST      = 1_000_000  # ₹ — planned/scheduled DT replacement
NTL_AVG_RECOVERY_PER_ALERT    = 50_000     # ₹ per confirmed NTL case (revenue recovery)
OVERLOAD_FAILURE_PROBABILITY  = 0.15       # 15% of sustained overloads → burnout w/o intervention


# ═════════════════════════════════════════════════════════════════════════════
# REQUEST MODEL
# ═════════════════════════════════════════════════════════════════════════════

class ROIRequest(BaseModel):
    fromDate: str = Field(..., alias="from_date")
    toDate:   str = Field(..., alias="to_date")
    office:   int
    assetId:  Optional[str] = None

    @field_validator("fromDate", "toDate")
    @classmethod
    def validate_date_format(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except Exception:
            raise ValueError("Date must be YYYY-MM-DD format")
        return v

    @model_validator(mode="after")
    def validate_date_range(self):
        fd = datetime.strptime(self.fromDate, "%Y-%m-%d")
        td = datetime.strptime(self.toDate,   "%Y-%m-%d")
        if fd > td:
            raise ValueError("fromDate must be <= toDate")
        if (td - fd).days > MAX_DATE_RANGE:
            raise ValueError(f"Date range cannot exceed {MAX_DATE_RANGE} days")
        return self

    class Config:
        populate_by_name = True


# ═════════════════════════════════════════════════════════════════════════════
# ENDPOINT: /roi
# ═════════════════════════════════════════════════════════════════════════════

@router.post("/roi-savings")
def get_roi_breakdown(req: ROIRequest):
    """
    Returns a full cost-savings & ROI breakdown driven by ALL cost-reduction
    transformer models, expressed in Indian Rupees under APDCL tariff.
    """
    office_id   = int(req.office)
    from_date   = req.fromDate
    to_date     = req.toDate
    section_ids = resolve_to_sections(state.HIERARCHY_TREE, office_id)
    office_filter = {"$in": [int(s) for s in section_ids]}

    # ── Parallel fetch needed aggregates ─────────────────────────────────────
    tasks = {
        "dt_count":    lambda: _count_transformers(req),
        "load":        lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["load"],        from_date, to_date),
        "peak":        lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["peak"],        from_date, to_date),
        "pf":          lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["pf"],          from_date, to_date),
        "imb":         lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["imbalance"],   from_date, to_date),
        "overload":    lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["overload"],    from_date, to_date),
        "ntl":         lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["ntl"],         from_date, to_date),
        "ntl_raw":     lambda: _fetch_raw(_db, COL_NTL, office_filter, from_date, to_date, "confidence"),
    }
    results = {}
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {pool.submit(fn): k for k, fn in tasks.items()}
        for fut in as_completed(futures):
            results[futures[fut]] = fut.result()

    if not results["load"]:
        raise HTTPException(status_code=503,
            detail=f"No cached forecast for office={office_id}. Run precompute first.")

    total_dts = results["dt_count"]
    dates     = _extract_dates(results["load"])

    s_load      = _build_series(results["load"],     dates, "sum")
    s_peak      = _build_series(results["peak"],     dates, "max")
    s_pf        = _build_series(results["pf"],       dates, "mean")
    s_imb       = _build_series(results["imb"],      dates, "max")
    s_overload  = _build_series(results["overload"], dates, "sum")
    s_ntl       = _build_series(results["ntl"],      dates, "sum")

    # ── Anchor-based live DT summary ─────────────────────────────────────────
    df = resolve_transformers(req)
    total_capacity_kva  = float(df["transformer_capacity_kva"].sum())   if not df.empty else 0
    pf_penalty_dts      = int((df.get("pf_value", pd.Series([1])) < PF_PENALTY_THRESHOLD).sum()) if not df.empty else 0
    overload_dts        = int((df.get("load_percentage", pd.Series([0])) > OVERLOAD_THRESH_PCT).sum()) if not df.empty else 0
    underloaded_dts     = int((df.get("load_factor", pd.Series([1])) < 0.30).sum()) if not df.empty else 0
    high_lol_dts        = int((df.get("loss_of_life_pct", pd.Series([0])) >= LOL_CRITICAL_PCT).sum()) if not df.empty else 0

    total_kwh    = sum(s_load)
    total_peak   = max(s_peak) if s_peak else 0
    energy_bill  = total_kwh * TARIFF_PER_KWH

    # ═══════════════ 1. PF penalty avoidance ═══════════════
    # APDCL applies ~1.5% surcharge if fleet-wide PF drops below 0.90.
    # Potential savings = (penalty surcharge on energy bill) × penalty-DT share.
    pf_fraction_in_penalty = pf_penalty_dts / max(total_dts, 1)
    pf_saving = energy_bill * (PF_PENALTY_PCT / 100) * pf_fraction_in_penalty

    # ═══════════════ 2. MDI penalty avoidance ═══════════════
    # Assumed contract demand = 85% of installed capacity (industry norm).
    mdi_excess_kw = max(total_peak - total_capacity_kva * 0.85, 0)
    mdi_saving    = mdi_excess_kw * MDI_PENALTY_PER_KVA

    # ═══════════════ 3. Phase-balancing copper loss ═══════════════
    # Rebalancing R/Y/B phases typically saves 2-5% on I²R losses for feeders
    # with >10% current imbalance. We use avg imbalance to scale the saving.
    avg_imb_pct  = float(np.mean(s_imb)) if s_imb else 0
    imb_severity = min(avg_imb_pct / 20.0, 1.0)
    copper_saving = energy_bill * COPPER_LOSS_SAVING_PCT * imb_severity

    # ═══════════════ 4. Right-sizing iron-loss saving ═══════════════
    # Oversized DTs bleed constant no-load iron losses 24×7. Load-factor model
    # identifies them; we estimate ₹450/kVA/yr savings × scaled to this period.
    days_in_range     = max((datetime.strptime(to_date, "%Y-%m-%d") -
                             datetime.strptime(from_date, "%Y-%m-%d")).days, 1)
    scaling           = days_in_range / 365.0
    avg_cap_per_dt    = total_capacity_kva / max(total_dts, 1)
    right_size_saving = underloaded_dts * avg_cap_per_dt * 0.4 * IRON_LOSS_PER_KVA_PER_YEAR * scaling

    # ═══════════════ 5. Loss-of-Life — planned-vs-emergency ═══════════════
    lol_saving = high_lol_dts * (EMERGENCY_REPLACEMENT_COST - PLANNED_REPLACEMENT_COST)

    # ═══════════════ 6. NTL / Theft revenue recovery ═══════════════
    # Each confirmed NTL alert with confidence ≥ 50% recovers avg ₹50k.
    ntl_confirmed_count = sum(1 for r in results["ntl_raw"]
                              if float(r.get("confidence", 0)) >= 50)
    ntl_saving          = ntl_confirmed_count * NTL_AVG_RECOVERY_PER_ALERT

    # ═══════════════ 7. Overload failure prevention ═══════════════
    # Historic data: ~15% of sustained overloads → burnout (₹35L emergency cost).
    overload_event_count = int(sum(s_overload))
    overload_saving      = (overload_event_count * OVERLOAD_FAILURE_PROBABILITY *
                            EMERGENCY_REPLACEMENT_COST * 0.05)   # 5% probability adjustment

    total_savings = (pf_saving + mdi_saving + copper_saving + right_size_saving +
                     lol_saving + ntl_saving + overload_saving)

    # ═════════════════════════════ CHARTS ═════════════════════════════

    # 1. Waterfall-style savings breakdown (ECharts bar with gradient)
    buckets = [
        ("MDI Penalty Avoided",        round(mdi_saving,        0), "#f59e0b"),
        ("PF Penalty Avoided",         round(pf_saving,         0), "#91cc75"),
        ("Phase-Balance Copper Loss",  round(copper_saving,     0), "#5470c6"),
        ("Right-Sizing Iron Loss",     round(right_size_saving, 0), "#2a9d8f"),
        ("Planned vs Emergency LoL",   round(lol_saving,        0), "#bf3131"),
        ("NTL / Theft Recovery",       round(ntl_saving,        0), "#fc8452"),
        ("Overload Prevention",        round(overload_saving,   0), "#9a60b4"),
    ]

    waterfall = {
        "title":       "ROI Breakdown — Savings by Category",
        "code":        "roi_waterfall",
        "type":        "chartCard",
        "info":        "How each ML model contributes to monthly ₹ savings (APDCL tariff)",
        "subtext":     f"Est. Total: ₹{total_savings:,.0f}",
        "data": {
            "option": {
                "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"},
                            "formatter": "{b}<br/>₹{c0}"},
                "grid":    {"left": "3%", "right": "4%", "bottom": "20%", "containLabel": True},
                "xAxis":   {"type": "category",
                             "data": [b[0] for b in buckets],
                             "axisLabel": {"rotate": 25, "fontSize": 11}},
                "yAxis":   {"type": "value", "name": "₹"},
                "series":  [{
                    "type":      "bar",
                    "data":      [{"value": b[1], "itemStyle": {"color": b[2]}} for b in buckets],
                    "barWidth":  "55%",
                    "label":     {"show": True, "position": "top", "formatter": "₹{c}"},
                    "itemStyle": {"borderRadius": [6, 6, 0, 0]},
                }],
            }
        },
    }

    # 2. Pie share (% of each saving bucket vs total)
    pie = {
        "title":       "Savings Composition",
        "code":        "roi_pie",
        "type":        "chartCard",
        "info":        "Which cost-reduction model contributes most to total savings",
        "data": {
            "option": {
                "tooltip": {"trigger": "item", "formatter": "{b}: ₹{c} ({d}%)"},
                "legend":  {"orient": "horizontal", "bottom": "2%", "type": "scroll"},
                "series":  [{
                    "type":     "pie",
                    "radius":   ["45%", "72%"],
                    "avoidLabelOverlap": True,
                    "itemStyle":{"borderRadius": 8, "borderColor": "#fff", "borderWidth": 2},
                    "label":    {"show": True, "formatter": "{b}\n₹{c}"},
                    "data":     [{"name": b[0], "value": b[1], "itemStyle": {"color": b[2]}}
                                 for b in buckets if b[1] > 0],
                }],
            }
        },
    }

    # 3. Daily cumulative savings projection (line chart)
    # Even-split across dates for visualization
    daily_total   = total_savings / max(len(dates), 1)
    cum           = []
    running       = 0
    for _ in dates:
        running += daily_total
        cum.append(round(running, 0))

    cumulative = {
        "title":       "Cumulative Savings Trajectory",
        "code":        "roi_cumulative",
        "type":        "chartCard",
        "info":        "How ₹ savings accumulate across the selected date range",
        "data": {
            "option": {
                "tooltip": {"trigger": "axis", "formatter": "{b}<br/>Cumulative ₹{c}"},
                "grid":    {"left": "3%", "right": "4%", "bottom": "12%", "containLabel": True},
                "xAxis":   {"type": "category", "data": dates, "axisLabel": {"rotate": 45}},
                "yAxis":   {"type": "value", "name": "₹"},
                "series":  [{
                    "name":      "Cumulative Savings",
                    "type":      "line",
                    "data":      cum,
                    "smooth":    True,
                    "symbol":    "none",
                    "lineStyle": {"width": 3, "color": "#2a9d8f"},
                    "itemStyle": {"color": "#2a9d8f"},
                    "areaStyle": {
                        "color": {
                            "type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
                            "colorStops": [
                                {"offset": 0, "color": "#2a9d8f"},
                                {"offset": 1, "color": "rgba(42,157,143,0)"},
                            ],
                        },
                        "opacity": 0.5,
                    },
                }],
            }
        },
    }

    charts = [waterfall, pie, cumulative]

    # ═════════════════════════════ STATS ═════════════════════════════

    def _fmt(val, unit):
        """Combine a value and its unit into a single display string.
        Currency units (₹, $, €, £) are prefixed; everything else is suffixed."""
        if unit == "":
            return val
        if unit in ("₹", "$", "€", "£"):
            return f"{unit}{val}"
        return f"{val} {unit}"

    stats = [
        {"label": "Total Estimated Savings",  "value": _fmt(int(total_savings),     "₹"), "icon": "ri-money-rupee-circle-line"},
        {"label": "Est. Energy Bill",         "value": _fmt(int(energy_bill),       "₹"), "icon": "ri-file-text-line"},
        {"label": "MDI Penalty Avoided",      "value": _fmt(int(mdi_saving),        "₹"), "icon": "ri-arrow-up-circle-line"},
        {"label": "PF Penalty Avoided",       "value": _fmt(int(pf_saving),         "₹"), "icon": "ri-pulse-line"},
        {"label": "Copper-Loss Saving",       "value": _fmt(int(copper_saving),     "₹"), "icon": "ri-flashlight-line"},
        {"label": "Right-Sizing Saving",      "value": _fmt(int(right_size_saving), "₹"), "icon": "ri-scales-3-line"},
        {"label": "LoL Planned-Replace Save", "value": _fmt(int(lol_saving),        "₹"), "icon": "ri-heart-pulse-line"},
        {"label": "NTL Revenue Recovery",     "value": _fmt(int(ntl_saving),        "₹"), "icon": "ri-spy-line"},
        {"label": "Overload Prev. Saving",    "value": _fmt(int(overload_saving),   "₹"), "icon": "ri-shield-check-line"},
        {"label": "PF Penalty DTs",           "value": _fmt(pf_penalty_dts,         ""),  "icon": "ri-error-warning-line"},
        {"label": "Underloaded DTs",          "value": _fmt(underloaded_dts,        ""),  "icon": "ri-download-2-line"},
        {"label": "High-LoL DTs",             "value": _fmt(high_lol_dts,           ""),  "icon": "ri-skull-2-line"},
        {"label": "NTL Confirmed Cases",      "value": _fmt(ntl_confirmed_count,    ""),  "icon": "ri-alarm-warning-line"},
    ]

    # ═════════════════════════════ TABLES ═════════════════════════════

    breakdown_table = {
        "tableId": "roi_breakdown",
        "title":   "Cost-Saving Breakdown by Model",
        "data": [
            {"_id": f"ROI-{i}", "category": b[0], "amount_rs": b[1],
             "share_pct": round(b[1] / max(total_savings, 1) * 100, 1),
             "model":     ["Peak Demand", "Reactive PF", "Phase Imbalance",
                          "Load Factor", "Loss-of-Life", "NTL Theft",
                          "Overload Risk"][i]}
            for i, b in enumerate(buckets)
        ],
        "pagination": {
            "hasNextPage": False, "hasPreviousPage": False,
            "currentPage": 1, "pageSize": 10,
            "totalPages": 1, "totalCount": len(buckets),
        },
    }

    return {
        "data": {
            "stats":   stats,
            "charts":  charts,
            "tables":  [breakdown_table],
            "summary": {
                "total_estimated_savings_rs": int(total_savings),
                "energy_bill_rs":             int(energy_bill),
                "savings_rate_pct":           round(total_savings / max(energy_bill, 1) * 100, 2),
                "date_range":                 f"{from_date} to {to_date}",
                "currency":                   "INR",
                "tariff_source":              "APDCL Industrial/Commercial Blended",
            },
        },
        "message": "success",
        "status":  True,
    }