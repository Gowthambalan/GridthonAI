# ─────────────────────────────────────────────────────────────────────────────
# transformer_anomalies.py — Consolidated Rectification Alerts Endpoint
# ─────────────────────────────────────────────────────────────────────────────
#
# Single-pane-of-glass for all 5 rectification (anomaly detection) models:
#   • Thermal anomaly      → insulation/cooling fault
#   • Voltage sag/swell    → tap-changer wear / upstream grid
#   • Overload risk score  → hours-ahead burnout prediction
#   • Frequency drift      → grid instability precursor
#   • Neutral anomaly      → earth fault / broken neutral (SAFETY CRITICAL)
#
# Drives the "Real-Time-Monitor" and "AI-Insights" tabs with cross-model alerts.
# Response shape: { data: {stats, charts, tables}, message, status }
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
    MONGO_URI, DB_NAME, COL_THERMAL, COL_OVERLOAD, COL_NEUTRAL, COL_FREQ,
    MODEL_KEY_MAP, _fetch_agg, _fetch_raw, _extract_dates, _build_series,
    paginate, resolve_transformers, _count_transformers, build_chart,
)

router = APIRouter(tags=["Anomaly Rectification"])

MAX_DATE_RANGE = 90
_mongo = MongoClient(MONGO_URI, maxPoolSize=15)
_db    = _mongo[DB_NAME]


# ═════════════════════════════════════════════════════════════════════════════
# REQUEST MODEL
# ═════════════════════════════════════════════════════════════════════════════

class AnomalyRequest(BaseModel):
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
# ENDPOINT: /anomalies
# ═════════════════════════════════════════════════════════════════════════════

@router.post("/anomalies")
def get_consolidated_anomalies(req: AnomalyRequest):
    """Single endpoint aggregating alerts from all 5 rectification models."""
    section_ids = resolve_to_sections(state.HIERARCHY_TREE, int(req.office))
    office_filter = {"$in": [int(s) for s in section_ids]}

    tasks = {
        "dt_count":        lambda: _count_transformers(req),
        "thermal":         lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["thermal"],        req.fromDate, req.toDate),
        "volt_sag":        lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["volt_sag"],       req.fromDate, req.toDate),
        "overload_score":  lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["overload_score"], req.fromDate, req.toDate),
        "freq_drift":      lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["freq_drift"],     req.fromDate, req.toDate),
        "neutral":         lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["neutral"],        req.fromDate, req.toDate),
        "thermal_raw":     lambda: _fetch_raw(_db, COL_THERMAL,  office_filter, req.fromDate, req.toDate, "confidence"),
        "overload_raw":    lambda: _fetch_raw(_db, COL_OVERLOAD, office_filter, req.fromDate, req.toDate, "confidence"),
        "neutral_raw":     lambda: _fetch_raw(_db, COL_NEUTRAL,  office_filter, req.fromDate, req.toDate, "confidence"),
        "freq_raw":        lambda: _fetch_raw(_db, COL_FREQ,     office_filter, req.fromDate, req.toDate, "confidence"),
    }
    results = {}
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {pool.submit(fn): k for k, fn in tasks.items()}
        for fut in as_completed(futures):
            results[futures[fut]] = fut.result()

    dates = _extract_dates(results["thermal"]) if results["thermal"] else [req.fromDate]

    s_thermal     = _build_series(results["thermal"],        dates, "sum")  if results["thermal"]        else [0]*len(dates)
    s_voltsag     = _build_series(results["volt_sag"],       dates, "max")  if results["volt_sag"]       else [0]*len(dates)
    s_oload_score = _build_series(results["overload_score"], dates, "mean") if results["overload_score"] else [0]*len(dates)
    s_freq        = _build_series(results["freq_drift"],     dates, "sum")  if results["freq_drift"]     else [0]*len(dates)
    s_neutral     = _build_series(results["neutral"],        dates, "sum")  if results["neutral"]        else [0]*len(dates)

    total_thermal  = int(sum(s_thermal))
    total_freq     = int(sum(s_freq))
    total_neutral  = int(sum(s_neutral))
    max_voltsag    = round(max(s_voltsag) if s_voltsag else 0, 2)
    avg_oload_sc   = round(float(np.mean(s_oload_score)) if s_oload_score else 0, 3)

    # ── Multi-series combined anomaly timeline chart ─────────────────────────
    def mk_series(name, data, color, chart_type="line"):
        s = {
            "name":      name,
            "type":      chart_type,
            "data":      data,
            "smooth":    True,
            "symbol":    "none",
            "lineStyle": {"width": 2.5, "color": color},
            "itemStyle": {"color": color},
        }
        return s

    combined_chart = {
        "title":       "Cross-Model Anomaly Timeline",
        "code":        "anomaly_timeline",
        "type":        "chartCard",
        "info":        "Daily alert counts across all 5 rectification models",
        "data": {
            "option": {
                "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
                "legend":  {"data": ["Thermal", "Frequency Drift", "Neutral", "Voltage Deviation"],
                            "top": "2%"},
                "grid":    {"left": "3%", "right": "4%", "bottom": "12%", "containLabel": True},
                "xAxis":   {"type": "category", "data": dates, "axisLabel": {"rotate": 45}},
                "yAxis":   {"type": "value", "name": "Alerts / Value"},
                "series":  [
                    mk_series("Thermal",           s_thermal,     "#e76f51", "bar"),
                    mk_series("Frequency Drift",   s_freq,        "#8854d0", "bar"),
                    mk_series("Neutral",           s_neutral,     "#c0392b", "bar"),
                    mk_series("Voltage Deviation", s_voltsag,     "#3d85c6"),
                ],
            }
        },
    }

    charts = [
        combined_chart,
        build_chart("Thermal Anomaly Trend",             s_thermal,     dates, "Alerts"),
        build_chart("Voltage Sag/Swell Prediction",      s_voltsag,     dates, "V"),
        build_chart("Overload Risk Score (Hours-Ahead)", s_oload_score, dates, "Score"),
        build_chart("Frequency Drift Alerts",            s_freq,        dates, "Events"),
        build_chart("Neutral Current Anomaly",           s_neutral,     dates, "Alerts"),
    ]

    # ── Stats ────────────────────────────────────────────────────────────────
    def _fmt(val, unit):
        """Combine a value and its unit into a single display string.
        Currency units (₹, $, €, £) are prefixed; everything else is suffixed."""
        if unit == "":
            return val
        if unit in ("₹", "$", "€", "£"):
            return f"{unit}{val}"
        return f"{val} {unit}"

    stats = [
        {"label": "Total Transformers",           "value": _fmt(results["dt_count"],                       ""),  "icon": "ri-building-line"},
        {"label": "Thermal Anomalies",            "value": _fmt(total_thermal,                             ""),  "icon": "ri-fire-line"},
        {"label": "Frequency Drift Events",       "value": _fmt(total_freq,                                ""),  "icon": "ri-pulse-line"},
        {"label": "Neutral Anomalies (Safety)",   "value": _fmt(total_neutral,                             ""),  "icon": "ri-alarm-warning-fill"},
        {"label": "Max Voltage Deviation",        "value": _fmt(max_voltsag,                               "V"), "icon": "ri-arrow-up-down-line"},
        {"label": "Avg Overload Risk Score",      "value": _fmt(avg_oload_sc,                              ""),  "icon": "ri-speed-line"},
        {"label": "Total Active Alerts",          "value": _fmt(total_thermal + total_freq + total_neutral, ""), "icon": "ri-notification-3-line"},
    ]

    # ── Tables: one table per model, plus consolidated priority queue ────────
    def _rows_from_raw(raw, label_key="status", hot_field=None, hot_label=None):
        rows = []
        for r in raw:
            conf = float(r.get("confidence", 0))
            row = {
                "_id":        f"{label_key}-{r['assetUid']}-{r.get('date')}",
                "assetUid":   str(r["assetUid"]),
                "assetName":  f"DT-{r['assetUid']}",
                "date":       r.get("date"),
                "timestamp":  r.get("timestamp"),
                "confidence": f"{round(conf, 1)}%",
                "status":     "ALERT" if conf >= 50 else "Monitor",
            }
            if hot_field:
                row[hot_label] = round(float(r.get(hot_field, 0)), 2)
            rows.append(row)
        return rows

    thermal_rows  = _rows_from_raw(results["thermal_raw"],  "THERM", "thermal_gradient", "gradient_c")
    overload_rows = _rows_from_raw(results["overload_raw"], "OLOAD", "load_percentage", "load_pct")
    neutral_rows  = _rows_from_raw(results["neutral_raw"],  "NEUT",  "neutral_current", "neutral_a")
    freq_rows     = _rows_from_raw(results["freq_raw"],     "FREQ",  "frequency_drift", "drift_hz")

    def _tbl(rows, table_id, title):
        p = paginate(rows, 1, 10)
        return {
            "tableId":    table_id,
            "title":      title,
            "data":       p["data"],
            "pagination": p["pagination"],
        }

    # ── Consolidated "Top Critical" rollup (sorted by severity across models)
    all_rows = []
    for r in thermal_rows:  r["alert_type"] = "THERMAL";        all_rows.append(r)
    for r in overload_rows: r["alert_type"] = "OVERLOAD";       all_rows.append(r)
    for r in neutral_rows:  r["alert_type"] = "NEUTRAL (SAFETY)"; all_rows.append(r)
    for r in freq_rows:     r["alert_type"] = "FREQ DRIFT";     all_rows.append(r)

    # Sort: neutral > thermal > overload > freq, then by confidence desc
    priority_order = {"NEUTRAL (SAFETY)": 0, "THERMAL": 1, "OVERLOAD": 2, "FREQ DRIFT": 3}
    all_rows.sort(key=lambda r: (
        priority_order.get(r["alert_type"], 9),
        -float(r["confidence"].rstrip("%") or 0),
    ))

    consolidated = paginate(all_rows, 1, 15)
    consolidated_tbl = {
        "tableId":    "critical_alerts",
        "title":      "Consolidated Critical Alerts (by Severity)",
        "data":       consolidated["data"],
        "pagination": consolidated["pagination"],
    }

    return {
        "data": {
            "stats":  stats,
            "charts": charts,
            "tables": [
                consolidated_tbl,
                _tbl(thermal_rows,  "thermal_alerts",  "Thermal Anomaly Alerts"),
                _tbl(neutral_rows,  "neutral_alerts",  "Neutral Current Anomalies (Safety)"),
                _tbl(overload_rows, "overload_alerts", "Overload Risk Alerts"),
                _tbl(freq_rows,     "freq_alerts",     "Frequency Drift Alerts"),
            ],
        },
        "message": "success",
        "status":  True,
    }