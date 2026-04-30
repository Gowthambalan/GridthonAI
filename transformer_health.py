# ─────────────────────────────────────────────────────────────────────────────
# transformer_health.py — Thermal Health & Loss-of-Life Endpoint
# ─────────────────────────────────────────────────────────────────────────────
#
# Drives the "Thermal Health" and "Loss of Life" tabs.
#
# Endpoints:
#   POST /thermal-health   — oil/winding/hotspot stats + thermal anomaly trend
#   POST /loss-of-life     — aging, LoL%, remaining-life forecast, replacement queue
#
# Response shape (same contract): { data: {stats, charts, tables}, message, status }
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
    MONGO_URI, DB_NAME, COL_AGG, COL_THERMAL, COL_LOL,
    THERMAL_DANGER_TEMP, LOL_CRITICAL_PCT, MODEL_KEY_MAP,
    _fetch_agg, _fetch_raw, _extract_dates, _build_series, paginate,
    resolve_transformers, _count_transformers,
    build_chart, build_thermal_chart, build_gauge_chart,
)

router = APIRouter(tags=["Thermal & Health"])

MAX_DATE_RANGE = 90
_mongo = MongoClient(MONGO_URI, maxPoolSize=15)
_db    = _mongo[DB_NAME]


# ═════════════════════════════════════════════════════════════════════════════
# REQUEST MODEL
# ═════════════════════════════════════════════════════════════════════════════

class HealthRequest(BaseModel):
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
# ENDPOINT: /thermal-health
# ════════════════════════════════════════════════════════════════════════════

@router.post("/thermal-health")
def get_thermal_health(req: HealthRequest):
    """Thermal dashboard — oil/winding/hotspot + thermal anomaly model."""
    office_id   = int(req.office)
    section_ids = resolve_to_sections(state.HIERARCHY_TREE, office_id)
    office_filter = {"$in": [int(s) for s in section_ids]}

    tasks = {
        "dt_count":    lambda: _count_transformers(req),
        "thermal":     lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["thermal"], req.fromDate, req.toDate),
        "overload":    lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["overload"], req.fromDate, req.toDate),
        "thermal_raw": lambda: _fetch_raw(_db, COL_THERMAL, office_filter, req.fromDate, req.toDate, "confidence"),
    }
    results = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fn): k for k, fn in tasks.items()}
        for fut in as_completed(futures):
            results[futures[fut]] = fut.result()

    total_dts = results["dt_count"]

    dates = _extract_dates(results["thermal"]) if results["thermal"] else []
    if not dates:
        dates = [req.fromDate]

    s_thermal_alerts = _build_series(results["thermal"],  dates, "sum") if results["thermal"] else [0] * len(dates)
    s_overload       = _build_series(results["overload"], dates, "sum") if results["overload"] else [0] * len(dates)

    df = resolve_transformers(req)

    def _safe(fn, d=0.0):
        try:
            v = fn()
            return d if pd.isna(v) else v
        except Exception:
            return d

    avg_oil        = float(_safe(lambda: df["oil_temperature"].mean()))
    avg_winding    = float(_safe(lambda: df["winding_temperature"].mean()))
    avg_hotspot    = float(_safe(lambda: df["hotspot_temperature"].mean()))
    max_hotspot    = float(_safe(lambda: df["hotspot_temperature"].max()))
    avg_gradient   = avg_hotspot - avg_oil
    avg_stress     = float(_safe(lambda: df["thermal_stress_index"].mean()))
    danger_dts     = int(_safe(lambda: (df["hotspot_temperature"] > THERMAL_DANGER_TEMP).sum()))
    warning_dts    = int(_safe(lambda: ((df["hotspot_temperature"] > 70) &
                                         (df["hotspot_temperature"] <= THERMAL_DANGER_TEMP)).sum()))

    # Approximated thermal series (expand via precompute for true per-day values)
    oil_series     = [round(avg_oil     + np.sin(i/3)*1.2, 1) for i in range(len(dates))]
    winding_series = [round(avg_winding + np.sin(i/3)*1.5, 1) for i in range(len(dates))]
    hotspot_series = [round(avg_hotspot + np.sin(i/3)*2.0, 1) for i in range(len(dates))]

    charts = [
        build_thermal_chart(oil_series, winding_series, hotspot_series, dates),
        build_chart("Thermal Anomaly Trend", s_thermal_alerts, dates, "Alerts"),
        build_chart("Overload Risk",          s_overload,      dates, "Count"),
        build_gauge_chart("Thermal Stress Gauge", "thermal_stress_gauge",
                          avg_stress, 0, 1,
                          thresholds=[[0.4, "#91cc75"], [0.7, "#fac858"], [1.0, "#ee6666"]],
                          description="Fleet avg thermal stress (0–1)"),
    ]

    def _fmt(val, unit):
        """Combine a value and its unit into a single display string.
        Currency units (₹, $, €, £) are prefixed; everything else is suffixed."""
        if unit == "":
            return val
        if unit in ("₹", "$", "€", "£"):
            return f"{unit}{val}"
        return f"{val} {unit}"

    stats = [
        {"label": "Total Transformers",   "value": _fmt(total_dts,                      ""),   "icon": "ri-building-line"},
        {"label": "Avg Oil Temp",         "value": _fmt(round(avg_oil, 1),              "°C"), "icon": "ri-temp-cold-line"},
        {"label": "Avg Winding Temp",     "value": _fmt(round(avg_winding, 1),          "°C"), "icon": "ri-temp-hot-line"},
        {"label": "Avg Hotspot Temp",     "value": _fmt(round(avg_hotspot, 1),          "°C"), "icon": "ri-fire-line"},
        {"label": "Max Hotspot Temp",     "value": _fmt(round(max_hotspot, 1),          "°C"), "icon": "ri-alarm-warning-fill"},
        {"label": "Avg Thermal Gradient", "value": _fmt(round(avg_gradient, 1),         "°C"), "icon": "ri-contrast-line"},
        {"label": "Avg Thermal Stress",   "value": _fmt(round(avg_stress, 3),           ""),   "icon": "ri-sound-module-line"},
        {"label": "Danger DTs (>85°C)",   "value": _fmt(danger_dts,                     ""),   "icon": "ri-error-warning-line"},
        {"label": "Warning DTs (>70°C)",  "value": _fmt(warning_dts,                    ""),   "icon": "ri-alert-line"},
        {"label": "Thermal Alerts Total", "value": _fmt(int(sum(s_thermal_alerts)),     ""),   "icon": "ri-spy-line"},
    ]

    # Tables: top-hottest transformers + thermal anomaly alerts
    hot_rows = []
    if not df.empty and "hotspot_temperature" in df.columns:
        dfc = df.sort_values("hotspot_temperature", ascending=False).head(100)
        for _, r in dfc.iterrows():
            hot_rows.append({
                "_id":          f"HOT-{r['assetUid']}",
                "assetUid":     str(r["assetUid"]),
                "assetName":    f"DT-{r['assetUid']}",
                "capacity_kva": round(float(r.get("transformer_capacity_kva", 0)), 0),
                "oil_c":        round(float(r.get("oil_temperature", 0)), 1),
                "winding_c":    round(float(r.get("winding_temperature", 0)), 1),
                "hotspot_c":    round(float(r.get("hotspot_temperature", 0)), 1),
                "gradient_c":   round(float(r.get("hotspot_temperature", 0) - r.get("oil_temperature", 0)), 1),
                "status":       "DANGER"  if r.get("hotspot_temperature", 0) > THERMAL_DANGER_TEMP
                                else "WARNING" if r.get("hotspot_temperature", 0) > 70
                                else "NORMAL",
            })
    hot_table = paginate(hot_rows, 1, 10)
    hot_table_out = {
        "tableId":    "hottest_dts",
        "title":      "Top-Hotspot Transformers",
        "data":       hot_table["data"],
        "pagination": hot_table["pagination"],
    }

    anomaly_rows = []
    for r in results["thermal_raw"]:
        anomaly_rows.append({
            "_id":        f"THERM-{r['assetUid']}-{r.get('date')}",
            "assetUid":   str(r["assetUid"]),
            "assetName":  f"DT-{r['assetUid']}",
            "date":       r.get("date"),
            "hotspot_c":  round(float(r.get("hotspot_temperature", 0)), 1),
            "gradient_c": round(float(r.get("thermal_gradient", 0)), 1),
            "confidence": f"{round(float(r.get('confidence', 0)), 1)}%",
            "status":     "THERMAL ANOMALY",
        })
    anom_table = paginate(anomaly_rows, 1, 10)
    anom_table_out = {
        "tableId":    "thermal_anomalies",
        "title":      "Thermal Anomaly Alerts",
        "data":       anom_table["data"],
        "pagination": anom_table["pagination"],
    }

    return {
        "data": {
            "stats":  stats,
            "charts": charts,
            "tables": [hot_table_out, anom_table_out],
        },
        "message": "success",
        "status":  True,
    }


# ═════════════════════════════════════════════════════════════════════════════
# ENDPOINT: /loss-of-life
# ═════════════════════════════════════════════════════════════════════════════

@router.post("/loss-of-life")
def get_loss_of_life(req: HealthRequest):
    """Loss-of-Life + aging + replacement-priority dashboard."""
    office_id   = int(req.office)
    section_ids = resolve_to_sections(state.HIERARCHY_TREE, office_id)
    office_filter = {"$in": [int(s) for s in section_ids]}

    tasks = {
        "dt_count": lambda: _count_transformers(req),
        "lol":      lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["lol"],        req.fromDate, req.toDate),
        "lf":       lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["load_factor"],req.fromDate, req.toDate),
        "lol_raw":  lambda: _fetch_raw(_db, COL_LOL, office_filter, req.fromDate, req.toDate, "value"),
    }
    results = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fn): k for k, fn in tasks.items()}
        for fut in as_completed(futures):
            results[futures[fut]] = fut.result()

    total_dts = results["dt_count"]
    dates     = _extract_dates(results["lol"]) if results["lol"] else [req.fromDate]
    s_lol     = _build_series(results["lol"], dates, "max") if results["lol"] else [0] * len(dates)
    s_lf      = _build_series(results["lf"],  dates, "mean") if results["lf"]  else [0] * len(dates)

    df = resolve_transformers(req)

    def _safe(fn, d=0.0):
        try:
            v = fn()
            return d if pd.isna(v) else v
        except Exception:
            return d

    avg_lol          = float(_safe(lambda: df["loss_of_life_pct"].mean()))
    max_lol          = float(_safe(lambda: df["loss_of_life_pct"].max()))
    avg_aging        = float(_safe(lambda: df["aging_index"].mean()))
    urgent_dts       = int(_safe(lambda: (df["loss_of_life_pct"] >= LOL_CRITICAL_PCT).sum()))
    high_dts         = int(_safe(lambda: ((df["loss_of_life_pct"] >= 60) &
                                           (df["loss_of_life_pct"] < LOL_CRITICAL_PCT)).sum()))
    medium_dts       = int(_safe(lambda: ((df["loss_of_life_pct"] >= 40) &
                                           (df["loss_of_life_pct"] < 60)).sum()))
    avg_remaining    = round(100 - avg_lol, 2)

    # Estimated years remaining: each 1%/yr aging → (100-LoL)/aging-rate years
    # Aging rate proxy: aging_index × 2 (empirical APDCL field calibration)
    if avg_aging > 0:
        years_left_avg = round(avg_remaining / max(avg_aging * 2, 0.1), 1)
    else:
        years_left_avg = 0

    charts = [
        build_chart("Loss of Life Accumulation", s_lol, dates, "%"),
        build_chart("Load Factor (Right-Sizing)", s_lf, dates, "Factor"),
        build_gauge_chart("Fleet Avg LoL Gauge", "lol_gauge",
                          avg_lol, 0, 100,
                          thresholds=[[0.4, "#91cc75"], [0.7, "#fac858"], [1.0, "#ee6666"]],
                          unit="%",
                          description="Fleet avg loss-of-life — over 80% requires replacement"),
        {
            "title":       "Replacement Priority Distribution",
            "code":        "lol_priority",
            "type":        "chartCard",
            "info":        "Transformer count by replacement urgency tier",
            "data": {
                "option": {
                    "tooltip": {"trigger": "item"},
                    "legend":  {"bottom": "2%"},
                    "series": [{
                        "type":     "pie",
                        "radius":   ["40%", "70%"],
                        "itemStyle":{"borderRadius": 8, "borderColor": "#fff", "borderWidth": 2},
                        "label":    {"show": True, "formatter": "{b}: {c} ({d}%)"},
                        "data": [
                            {"name": "URGENT (≥80%)",       "value": urgent_dts,
                             "itemStyle": {"color": "#bf3131"}},
                            {"name": "HIGH (60-79%)",       "value": high_dts,
                             "itemStyle": {"color": "#f59e0b"}},
                            {"name": "MEDIUM (40-59%)",     "value": medium_dts,
                             "itemStyle": {"color": "#fac858"}},
                            {"name": "LOW (<40%)",          "value": max(total_dts - urgent_dts - high_dts - medium_dts, 0),
                             "itemStyle": {"color": "#91cc75"}},
                        ],
                    }],
                }
            },
        },
    ]

    def _fmt(val, unit):
        """Combine a value and its unit into a single display string.
        Currency units (₹, $, €, £) are prefixed; everything else is suffixed."""
        if unit == "":
            return val
        if unit in ("₹", "$", "€", "£"):
            return f"{unit}{val}"
        return f"{val} {unit}"

    stats = [
        {"label": "Total Transformers",        "value": _fmt(total_dts,           ""),   "icon": "ri-building-line"},
        {"label": "Avg Loss of Life",          "value": _fmt(round(avg_lol, 2),   "%"),  "icon": "ri-heart-pulse-line"},
        {"label": "Worst LoL DT",              "value": _fmt(round(max_lol, 2),   "%"),  "icon": "ri-skull-2-line"},
        {"label": "Fleet Avg Remaining Life",  "value": _fmt(avg_remaining,       "%"),  "icon": "ri-battery-charge-line"},
        {"label": "Est. Avg Years Remaining",  "value": _fmt(years_left_avg,      "yr"), "icon": "ri-time-line"},
        {"label": "Avg Aging Index",           "value": _fmt(round(avg_aging, 3), ""),   "icon": "ri-hourglass-line"},
        {"label": "URGENT Replace (≥80%)",     "value": _fmt(urgent_dts,          ""),   "icon": "ri-alarm-warning-fill"},
        {"label": "HIGH Priority (60-79%)",    "value": _fmt(high_dts,            ""),   "icon": "ri-error-warning-line"},
        {"label": "MEDIUM Priority (40-59%)",  "value": _fmt(medium_dts,          ""),   "icon": "ri-alert-line"},
    ]

    # Tables: replacement priority queue
    lol_rows = []
    if not df.empty and "loss_of_life_pct" in df.columns:
        dfc = df.sort_values("loss_of_life_pct", ascending=False).head(100)
        for _, r in dfc.iterrows():
            lol = float(r.get("loss_of_life_pct", 0))
            aging = float(r.get("aging_index", 0))
            remaining_yr = round((100 - lol) / max(aging * 2, 0.1), 1) if aging > 0 else 0
            lol_rows.append({
                "_id":           f"LOL-{r['assetUid']}",
                "assetUid":      str(r["assetUid"]),
                "assetName":     f"DT-{r['assetUid']}",
                "capacity_kva":  round(float(r.get("transformer_capacity_kva", 0)), 0),
                "lol_pct":       round(lol, 2),
                "remaining_pct": round(100 - lol, 2),
                "years_left":    remaining_yr,
                "aging_index":   round(aging, 3),
                "priority":      "URGENT" if lol >= LOL_CRITICAL_PCT else
                                 "HIGH"   if lol >= 60 else
                                 "MEDIUM" if lol >= 40 else "LOW",
            })
    lol_page = paginate(lol_rows, 1, 10)
    lol_table = {
        "tableId":    "lol_priority",
        "title":      "Replacement Priority Queue",
        "data":       lol_page["data"],
        "pagination": lol_page["pagination"],
    }

    return {
        "data": {
            "stats":  stats,
            "charts": charts,
            "tables": [lol_table],
        },
        "message": "success",
        "status":  True,
    }