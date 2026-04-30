

# # transformer_insights.py — CORE DASHBOARD ENDPOINT for 15 Transformer Models
# # ─────────────────────────────────────────────────────────────────────────────
# #
# # Mirrors smartmeter_insights.py architecture but scaled for Distribution
# # Transformer (DT) fleet-level analytics:
# #
# #   • 15 forecast/anomaly models (vs 7 for smart meters)
# #   • Thermal / aging / LoL dimension
# #   • 3-phase voltage & current profile
# #   • Rich chart set (14 charts) with better visual treatments
# #   • ROI-aware stat cards (penalty-zone meters, MDI exceedance, right-size targets)
# #
# #   Response shape: { data: {stats, charts, tables}, message, status }
# # ─────────────────────────────────────────────────────────────────────────────

# from fastapi import APIRouter, HTTPException
# import pandas as pd
# import numpy as np

# router = APIRouter(tags=["Transformer Insights"])

# from pydantic import BaseModel, field_validator, model_validator, Field
# from typing import Optional
# from datetime import datetime
# from pymongo import MongoClient
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from collections import defaultdict

# import state
# from state import resolve_to_sections
# from stats_helpers import fmt, make_stat, build_threshold, build_forecast_meta


# # ═════════════════════════════════════════════════════════════════════════════
# # CONSTANTS
# # ═════════════════════════════════════════════════════════════════════════════

# MAX_DATE_RANGE = 90

# MONGO_URI = ""
# DB_NAME   = "iotdb"

# # Pre-computed forecast cache collections (populate via precompute_forecasts.py)
# # COL_AGG         = "transformer_forecast_aggregated"     # one row per (asset, model, date)
# # COL_NTL         = "transformer_forecast_ntl_raw"
# # COL_THERMAL     = "transformer_forecast_thermal_raw"
# # COL_OVERLOAD    = "transformer_forecast_overload_raw"
# # COL_LOL         = "transformer_forecast_lol_raw"
# # COL_FREQ        = "transformer_forecast_freq_raw"
# # COL_NEUTRAL     = "transformer_forecast_neutral_raw"
# # COL_ENERGY      = "transformer_forecast_energy_raw"     # per-DT energy (for drilldown)

# COL_AGG      = "transformer_forecast_aggregated_daily_v1"
# COL_NTL      = "transformer_forecast_ntl_raw_daily_v1"
# COL_THERMAL  = "transformer_forecast_thermal_raw_daily_v1"
# COL_OVERLOAD = "transformer_forecast_overload_raw_daily_v1"
# COL_NEUTRAL  = "transformer_forecast_neutral_raw_daily_v1"
# COL_FREQ     = "transformer_forecast_freq_raw_daily_v1"
# COL_LOL      = "transformer_forecast_lol_raw_daily_v1"
# COL_ENERGY   = "transformer_forecast_energy_raw_daily_v1"



# # ──────────────────────────────────────────────────────────────────────────
# # APDCL Indian tariff constants (used by stats + ROI calculations)
# # ──────────────────────────────────────────────────────────────────────────
# TARIFF_PER_KWH          = 8.50      # ₹/kWh (avg blended industrial + commercial)
# MDI_PENALTY_PER_KVA     = 350.00    # ₹/kVA/month above contract demand
# PF_PENALTY_PCT          = 1.5       # ~1.5% surcharge on energy bill if PF < 0.90
# PF_PENALTY_THRESHOLD    = 0.90
# OVERLOAD_THRESH_PCT     = 80.0      # load% above this = overload
# THERMAL_DANGER_TEMP     = 85.0      # °C hotspot
# NOMINAL_VOLTAGE         = 230.0
# VOLTAGE_TOLERANCE_PCT   = 6.0       # ±6% acceptable band
# NOMINAL_FREQUENCY       = 50.0      # Hz
# FREQ_DRIFT_THRESH       = 0.15      # Hz
# LOL_CRITICAL_PCT        = 80.0      # remaining life < 20% → replace planned


# # ──────────────────────────────────────────────────────────────────────────
# # Model → dashboard code mapping
# # ──────────────────────────────────────────────────────────────────────────
# MODEL_KEY_MAP = {
#     "load":             "dt_load_forecast_model.pkl",
#     "peak":             "dt_peak_demand_model.pkl",
#     "pf":               "dt_reactive_pf_model.pkl",
#     "voltage":          "dt_voltage_profile_model.pkl",
#     "imbalance":        "dt_phase_imbalance_model.pkl",
#     "pq":               "dt_pq_index_model.pkl",
#     "overload":         "dt_overload_risk_model.pkl",
#     "ntl":              "dt_ntl_theft_model.pkl",
#     "lol":              "dt_loss_of_life_model.pkl",
#     "load_factor":      "dt_load_factor_model.pkl",
#     "thermal":          "dt_thermal_anomaly_model.pkl",
#     "volt_sag":         "dt_volt_sag_swell_model.pkl",
#     "overload_score":   "dt_overload_risk_score_model.pkl",
#     "freq_drift":       "dt_freq_drift_model.pkl",
#     "neutral":          "dt_neutral_anomaly_model.pkl",
# }

# MODEL_META = {
#     "load":           {"label": "Energy (kWh)",       "unit": "kWh", "agg": "sum"},
#     "peak":           {"label": "Peak Demand (kW)",   "unit": "kW",  "agg": "max"},
#     "pf":             {"label": "Power Factor",       "unit": "",    "agg": "mean"},
#     "voltage":        {"label": "Voltage (V)",        "unit": "V",   "agg": "mean"},
#     "imbalance":      {"label": "Current Imbal. (%)", "unit": "%",   "agg": "max"},
#     "pq":             {"label": "PQ Index",           "unit": "",    "agg": "mean"},
#     "overload":       {"label": "Overload Flag",      "unit": "",    "agg": "sum"},
#     "ntl":            {"label": "NTL Alert",          "unit": "",    "agg": "count"},
#     "lol":            {"label": "Loss of Life (%)",   "unit": "%",   "agg": "max"},
#     "load_factor":    {"label": "Load Factor",        "unit": "",    "agg": "mean"},
#     "thermal":        {"label": "Thermal Anomaly",    "unit": "",    "agg": "sum"},
#     "volt_sag":       {"label": "Voltage Deviation",  "unit": "V",   "agg": "max"},
#     "overload_score": {"label": "Overload Risk Score","unit": "",    "agg": "mean"},
#     "freq_drift":     {"label": "Freq Drift Events",  "unit": "",    "agg": "sum"},
#     "neutral":        {"label": "Neutral Anomaly",    "unit": "",    "agg": "sum"},
# }


# # ──────────────────────────────────────────────────────────────────────────
# # CHART CONFIGS — richer styling than smart meter + transformer-specific bands
# # ──────────────────────────────────────────────────────────────────────────
# CHART_CONFIGS = {
#     "Energy Load Forecast": {
#         "description": "Forecasted total DT energy consumption — capacity planning baseline",
#         "code":  "load",   "y_name": "kWh",   "color": "#5470c6",
#         "type":  "bar",    "area":   False,
#     },
#     "Peak Demand vs MDI Threshold": {
#         "description": "Peak kW demand per day with MDI penalty line — avoid contract exceedance",
#         "code":  "peak",   "y_name": "kW",    "color": "#f59e0b",
#         "type":  "line",   "area":   True,
#     },
#     "Power Factor Quality": {
#         "description": "Average PF with penalty zone (< 0.90) shaded",
#         "code":  "pf",     "y_name": "PF",    "color": "#91cc75",
#         "area":  False,    "y_min":  0.7,     "y_max": 1.02,
#         "mark_line":  {"yAxis": PF_PENALTY_THRESHOLD, "label": "0.90 Penalty Floor", "color": "#ee6666"},
#         "mark_area":  {"yAxis": [0.7, PF_PENALTY_THRESHOLD], "color": "rgba(238,102,102,0.08)"},
#     },
#     "3-Phase Voltage Profile": {
#         "description": "VRN / VYN / VBN voltage with ±6% tolerance bands (IS 12360)",
#         "code":  "voltage","y_name": "Volts (V)","color": "#73c0de",
#         "area":  False,
#         "mark_line":  {"yAxis": NOMINAL_VOLTAGE, "label": "230V Nominal", "color": "#5470c6"},
#     },
#     "Phase Imbalance Trend": {
#         "description": "Current imbalance % — triggers I²R copper-loss inefficiency",
#         "code":  "imbalance", "y_name": "%",  "color": "#fac858",
#         "type":  "bar",    "area":   False,
#         "mark_line":  {"yAxis": 10, "label": "10% Alert", "color": "#ee6666"},
#     },
#     "Power Quality Index": {
#         "description": "Composite PQ index (voltage + current + PF + frequency)",
#         "code":  "pq",     "y_name": "Index", "color": "#9a60b4",
#         "type":  "line",   "area":   True,    "y_min": 0, "y_max": 1.05,
#         "mark_line":  {"yAxis": 0.85, "label": "0.85 Good PQ", "color": "#91cc75"},
#     },
#     "Overload Risk": {
#         "description": "Daily count of DTs breaching 80% loading or 85°C hotspot",
#         "code":  "overload","y_name":"Count","color":"#ee6666",
#         "type":  "bar",    "area":   False,
#     },
#     "NTL / Theft Detection": {
#         "description": "Transformers flagged for non-technical loss (tamper/theft)",
#         "code":  "ntl",    "y_name": "Alerts","color": "#fc8452",
#         "type":  "bar",    "area":   False,
#     },
#     "Thermal Anomaly Trend": {
#         "description": "Hotspot-oil gradient > 25°C while load < 50% = insulation fault signature",
#         "code":  "thermal","y_name": "Alerts","color": "#e76f51",
#         "type":  "bar",    "area":   False,
#     },
#     "Loss of Life Accumulation": {
#         "description": "Cumulative LoL% — IEEE C57.91 aging acceleration tracker",
#         "code":  "lol",    "y_name": "%",     "color": "#bf3131",
#         "type":  "line",   "area":   True,
#         "mark_line":  {"yAxis": LOL_CRITICAL_PCT, "label": "80% Replace-Plan", "color": "#ee6666"},
#     },
#     "Load Factor (Right-Sizing)": {
#         "description": "Fleet avg load factor — low LF means oversized DTs wasting iron losses",
#         "code":  "load_factor","y_name":"Factor","color":"#2a9d8f",
#         "type":  "line",   "area":   True,    "y_min": 0, "y_max": 1.05,
#         "mark_line":  {"yAxis": 0.30, "label": "30% Right-Size", "color": "#fac858"},
#     },
#     "Overload Risk Score (Hours-Ahead)": {
#         "description": "Forward-looking overload risk score — enables pre-emptive load transfer",
#         "code":  "overload_score","y_name":"Score","color":"#d62728",
#         "type":  "line",   "area":   True,
#     },
#     "Voltage Sag/Swell Prediction": {
#         "description": "Forecasted voltage deviation — OLTC wear / upstream-grid health indicator",
#         "code":  "volt_sag","y_name":"V","color":"#3d85c6",
#         "type":  "line",   "area":   True,
#     },
#     "Frequency Drift Alerts": {
#         "description": "Frequency drift events beyond ±0.15 Hz — grid stability precursor",
#         "code":  "freq_drift","y_name":"Events","color":"#8854d0",
#         "type":  "bar",    "area":   False,
#     },
#     "Neutral Current Anomaly": {
#         "description": "Broken-neutral / earth-fault signatures — SAFETY CRITICAL",
#         "code":  "neutral","y_name":"Alerts","color":"#c0392b",
#         "type":  "bar",    "area":   False,
#     },
# }


# # ═════════════════════════════════════════════════════════════════════════════
# # PYDANTIC MODELS
# # ═════════════════════════════════════════════════════════════════════════════

# class TableRequest(BaseModel):
#     tableId:  str
#     page:     int = 1
#     pageSize: int = 10


# class ForecastRequest(BaseModel):
#     fromDate: str = Field(..., alias="from_date")
#     toDate:   str = Field(..., alias="to_date")
#     office:   int
#     assetId:  Optional[str]              = None
#     tables:   Optional[list[TableRequest]] = None

#     @field_validator("fromDate", "toDate")
#     @classmethod
#     def validate_date_format(cls, v):
#         try:
#             datetime.strptime(v, "%Y-%m-%d")
#         except Exception:
#             raise ValueError("Date must be YYYY-MM-DD format")
#         return v

#     @model_validator(mode="after")
#     def validate_date_range(self):
#         fd = datetime.strptime(self.fromDate, "%Y-%m-%d")
#         td = datetime.strptime(self.toDate,   "%Y-%m-%d")
#         if fd > td:
#             raise ValueError("fromDate must be <= toDate")
#         if (td - fd).days > MAX_DATE_RANGE:
#             raise ValueError(f"Date range cannot exceed {MAX_DATE_RANGE} days")
#         return self

#     class Config:
#         populate_by_name = True


# class DrilldownRequest(BaseModel):
#     date:     str
#     office:   int
#     code:     str
#     page:     int = 1
#     pageSize: int = 10

#     @field_validator("code")
#     @classmethod
#     def validate_code(cls, v):
#         if v not in MODEL_KEY_MAP:
#             raise ValueError(f"code must be one of {list(MODEL_KEY_MAP.keys())}")
#         return v


# # ═════════════════════════════════════════════════════════════════════════════
# # MONGO (shared pool)
# # ═════════════════════════════════════════════════════════════════════════════

# _mongo_client = MongoClient(
#     MONGO_URI,
#     maxPoolSize=25,
#     serverSelectionTimeoutMS=3000,
#     connectTimeoutMS=3000,
# )
# _db = _mongo_client[DB_NAME]


# # ═════════════════════════════════════════════════════════════════════════════
# # HELPERS
# # ═════════════════════════════════════════════════════════════════════════════

# def resolve_transformers(req) -> pd.DataFrame:
#     """Filter ANCHOR_INDEX to transformers under the requested office (and optional assetId)."""
#     if state.ANCHOR_INDEX is None or state.ANCHOR_INDEX.empty:
#         raise Exception("ANCHOR_INDEX is empty")
#     if state.HIERARCHY_TREE is None:
#         raise Exception("Hierarchy not initialized")

#     df          = state.ANCHOR_INDEX.copy()
#     office_id   = int(req.office)
#     section_ids = resolve_to_sections(state.HIERARCHY_TREE, office_id)

#     # Transformer data uses `officeID` (camel-ish) per train.py's flatten_chunk
#     if "officeID" in df.columns:
#         df = df[df["officeID"].astype(str).isin([str(s) for s in section_ids])]
#     elif "officeId" in df.columns:
#         df = df[df["officeId"].astype(int).isin(section_ids)]

#     if req.assetId:
#         df = df[df["assetUid"].astype(str) == str(req.assetId)]

#     return df


# def _count_transformers(req) -> int:
#     if state.ANCHOR_INDEX is None or state.ANCHOR_INDEX.empty:
#         return 0
#     section_ids = resolve_to_sections(state.HIERARCHY_TREE, int(req.office))

#     if "officeID" in state.ANCHOR_INDEX.columns:
#         mask = state.ANCHOR_INDEX["officeID"].astype(str).isin([str(s) for s in section_ids])
#     else:
#         mask = state.ANCHOR_INDEX["officeId"].astype(int).isin(section_ids)

#     if req.assetId:
#         mask &= state.ANCHOR_INDEX["assetUid"].astype(str) == str(req.assetId)
#     return int(mask.sum())


# def paginate(data: list, page: int, page_size: int) -> dict:
#     total       = len(data)
#     total_pages = max(1, (total + page_size - 1) // page_size)
#     start       = (page - 1) * page_size
#     end         = start + page_size
#     return {
#         "data": data[start:end],
#         "pagination": {
#             "hasNextPage":     page < total_pages,
#             "hasPreviousPage": page > 1,
#             "currentPage":     page,
#             "pageSize":        page_size,
#             "totalPages":      total_pages,
#             "totalCount":      total,
#         },
#     }


# def _fetch_agg(db, office_filter, model_key, from_date, to_date):
#     return list(
#         db[COL_AGG].find(
#             {
#                 "office_id": office_filter,
#                 "model_key": model_key,
#                 "date":      {"$gte": from_date, "$lte": to_date},
#             },
#             {"_id": 0},
#         ).sort("date", 1)
#     )


# def _fetch_raw(db, collection, office_filter, from_date, to_date, sort_field="confidence"):
#     return list(
#         db[collection].find(
#             {
#                 "office_id": office_filter,
#                 "date":      {"$gte": from_date, "$lte": to_date},
#             },
#             {"_id": 0},
#         ).sort(sort_field, -1)
#     )


# def _extract_dates(data):
#     return sorted({r["date"] for r in data})


# def _build_series(data, dates, agg="sum"):
#     d = defaultdict(list)
#     for r in data:
#         d[r["date"]].append(r.get("value", 0))

#     result = {}
#     for date, vals in d.items():
#         if not vals:
#             result[date] = 0
#         elif agg == "sum":
#             result[date] = sum(vals)
#         elif agg == "mean":
#             result[date] = sum(vals) / len(vals)
#         elif agg == "max":
#             result[date] = max(vals)
#         elif agg == "count":
#             # count of flagged (value==1) for classifier aggregates
#             result[date] = sum(1 for v in vals if v == 1 or v is True)
#     return [round(result.get(dt, 0), 4) for dt in dates]


# # ═════════════════════════════════════════════════════════════════════════════
# # CHART BUILDER — richer than smart-meter (3-phase series + mark areas + gauges)
# # ═════════════════════════════════════════════════════════════════════════════

# def build_chart(title, data, dates, y_name="Value",
#                 series_list=None, legend_data=None, meta=None):
#     cfg          = CHART_CONFIGS.get(title, {})
#     color        = cfg.get("color", "#5470c6")
#     chart_type   = cfg.get("type", "line")
#     area         = cfg.get("area", False)
#     y_min        = cfg.get("y_min")
#     y_max        = cfg.get("y_max")
#     mark_line    = cfg.get("mark_line")
#     mark_area    = cfg.get("mark_area")
#     final_y_name = cfg.get("y_name", y_name)
#     code         = cfg.get("code", title.lower().replace(" ", "_"))
#     description  = cfg.get("description", "")

#     y_axis = {"type": "value", "name": final_y_name}
#     if y_min is not None: y_axis["min"] = y_min
#     if y_max is not None: y_axis["max"] = y_max

#     if series_list:
#         series = series_list
#     else:
#         single = {
#             "name":   title,
#             "type":   chart_type,
#             "data":   data,
#             "symbol": "none",
#         }
#         if chart_type == "line":
#             single.update({
#                 "smooth":    True,
#                 "lineStyle": {"width": 3, "color": color},
#                 "itemStyle": {"color": color},
#             })
#             if area:
#                 single["areaStyle"] = {
#                     "color": {
#                         "type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
#                         "colorStops": [
#                             {"offset": 0, "color": color},
#                             {"offset": 1, "color": "rgba(0,0,0,0)"},
#                         ],
#                     },
#                     "opacity": 0.4,
#                 }
#         elif chart_type == "bar":
#             single.update({
#                 "itemStyle": {"color": color, "borderRadius": [6, 6, 0, 0]},
#                 "barWidth":  "55%",
#             })

#         marks = []
#         if mark_line:
#             single["markLine"] = {
#                 "silent": True,
#                 "symbol": "none",
#                 "data":  [{"yAxis": mark_line["yAxis"],
#                            "label": {"formatter": mark_line["label"], "position": "insideEndTop"}}],
#                 "lineStyle": {"type": "dashed", "color": mark_line["color"], "width": 2},
#             }
#         if mark_area:
#             single["markArea"] = {
#                 "silent": True,
#                 "itemStyle": {"color": mark_area["color"]},
#                 "data": [[
#                     {"yAxis": mark_area["yAxis"][0]},
#                     {"yAxis": mark_area["yAxis"][1]},
#                 ]],
#             }

#         series = [single]

#     return {
#         "title":       title,
#         "code":        code,
#         "type":        "chartCard",
#         "info":        description,
#         "subtext":     meta.get("subtext") if meta else "",
#         "data": {
#             "option": {
#                 "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
#                 "legend":  {"data": legend_data or [title], "top": "2%"},
#                 "grid":    {"left": "3%", "right": "4%", "bottom": "12%", "containLabel": True},
#                 "xAxis":   {"type": "category", "data": dates, "axisLabel": {"rotate": 45}},
#                 "yAxis":   y_axis,
#                 "series":  series,
#             }
#         },
#     }


# def build_3phase_voltage_chart(vrn_series, vyn_series, vbn_series, dates):
#     """Dedicated 3-phase voltage chart with R/Y/B overlays + ±6% band."""
#     tol_high = NOMINAL_VOLTAGE * (1 + VOLTAGE_TOLERANCE_PCT / 100)
#     tol_low  = NOMINAL_VOLTAGE * (1 - VOLTAGE_TOLERANCE_PCT / 100)

#     def mk(name, data, color):
#         return {
#             "name":      name,
#             "type":      "line",
#             "data":      data,
#             "smooth":    True,
#             "symbol":    "none",
#             "lineStyle": {"width": 2.5, "color": color},
#             "itemStyle": {"color": color},
#         }

#     series = [
#         mk("VRN (R Phase)", vrn_series, "#ee6666"),
#         mk("VYN (Y Phase)", vyn_series, "#fac858"),
#         mk("VBN (B Phase)", vbn_series, "#3d85c6"),
#     ]
#     # Add tolerance band as mark-area on first series
#     series[0]["markArea"] = {
#         "silent":    True,
#         "itemStyle": {"color": "rgba(145, 204, 117, 0.08)"},
#         "data": [[{"yAxis": tol_low}, {"yAxis": tol_high}]],
#     }
#     series[0]["markLine"] = {
#         "silent":    True,
#         "symbol":    "none",
#         "lineStyle": {"type": "dashed", "color": "#5470c6", "width": 1.5},
#         "data":      [{"yAxis": NOMINAL_VOLTAGE, "label": {"formatter": "230V Nominal"}}],
#     }

#     return build_chart(
#         "3-Phase Voltage Profile", [], dates, "V",
#         series_list=series,
#         legend_data=["VRN (R Phase)", "VYN (Y Phase)", "VBN (B Phase)"],
#     )


# def build_thermal_chart(oil_series, winding_series, hotspot_series, dates):
#     """Dedicated thermal overlay: oil / winding / hotspot with danger zone."""
#     def mk(name, data, color, width=2.5):
#         return {
#             "name":      name,
#             "type":      "line",
#             "data":      data,
#             "smooth":    True,
#             "symbol":    "none",
#             "lineStyle": {"width": width, "color": color},
#             "itemStyle": {"color": color},
#             "areaStyle": {"color": color, "opacity": 0.08},
#         }

#     series = [
#         mk("Oil Temperature",     oil_series,     "#5470c6"),
#         mk("Winding Temperature", winding_series, "#f59e0b"),
#         mk("Hotspot Temperature", hotspot_series, "#ee6666", 3),
#     ]
#     series[2]["markLine"] = {
#         "silent":    True,
#         "symbol":    "none",
#         "data":      [{"yAxis": THERMAL_DANGER_TEMP,
#                        "label": {"formatter": f"{THERMAL_DANGER_TEMP}°C Danger", "position": "insideEndTop"}}],
#         "lineStyle": {"type": "dashed", "color": "#c0392b", "width": 2},
#     }

#     return {
#         "title":       "Thermal Profile (Oil / Winding / Hotspot)",
#         "code":        "thermal_profile",
#         "type":        "chartCard",
#         "info":        "Three-layer thermal overlay — hotspot above 85°C accelerates loss-of-life",
#         "subtext":     "",
#         "data": {
#             "option": {
#                 "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
#                 "legend":  {"data": ["Oil Temperature", "Winding Temperature", "Hotspot Temperature"], "top": "2%"},
#                 "grid":    {"left": "3%", "right": "4%", "bottom": "12%", "containLabel": True},
#                 "xAxis":   {"type": "category", "data": dates, "axisLabel": {"rotate": 45}},
#                 "yAxis":   {"type": "value", "name": "°C"},
#                 "series":  series,
#             }
#         },
#     }


# def build_phase_share_chart(r_share, y_share, b_share, dates):
#     """Stacked bar: phase load share (R/Y/B) — visualises imbalance."""
#     def mk(name, data, color):
#         return {
#             "name":     name,
#             "type":     "bar",
#             "stack":    "phase",
#             "data":     data,
#             "itemStyle":{"color": color},
#             "barWidth": "55%",
#         }

#     series = [
#         mk("R Phase %", r_share, "#ee6666"),
#         mk("Y Phase %", y_share, "#fac858"),
#         mk("B Phase %", b_share, "#3d85c6"),
#     ]

#     return {
#         "title":       "Phase Load Share (R / Y / B)",
#         "code":        "phase_share",
#         "type":        "chartCard",
#         "info":        "Stacked phase power share — ideal is 33/33/33; deviation drives copper losses",
#         "subtext":     "",
#         "data": {
#             "option": {
#                 "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
#                 "legend":  {"data": ["R Phase %", "Y Phase %", "B Phase %"], "top": "2%"},
#                 "grid":    {"left": "3%", "right": "4%", "bottom": "12%", "containLabel": True},
#                 "xAxis":   {"type": "category", "data": dates, "axisLabel": {"rotate": 45}},
#                 "yAxis":   {"type": "value", "name": "% Share"},
#                 "series":  series,
#             }
#         },
#     }


# def build_gauge_chart(title, code, value, min_val=0, max_val=100,
#                       thresholds=None, unit="", description=""):
#     """Gauge chart — used for overload-risk-score, PQ index, avg LF summary."""
#     thresholds = thresholds or [
#         [0.3,  "#91cc75"],
#         [0.7,  "#fac858"],
#         [1.0,  "#ee6666"],
#     ]
#     pct = (value - min_val) / max(max_val - min_val, 1e-9)
#     return {
#         "title":       title,
#         "code":        code,
#         "type":        "gaugeCard",
#         "info":        description,
#         "subtext":     "",
#         "data": {
#             "option": {
#                 "series": [{
#                     "type":     "gauge",
#                     "min":      min_val,
#                     "max":      max_val,
#                     "progress": {"show": True, "width": 14},
#                     "axisLine": {"lineStyle": {"width": 14, "color": thresholds}},
#                     "pointer":  {"width": 5, "length": "70%"},
#                     "detail":   {"valueAnimation": True, "formatter": f"{{value}} {unit}", "fontSize": 22},
#                     "data":     [{"value": round(value, 3), "name": title}],
#                 }]
#             }
#         },
#     }


# # ═════════════════════════════════════════════════════════════════════════════
# # MAIN ENDPOINT: /insigh-test  — Full 15-model dashboard response
# # ═════════════════════════════════════════════════════════════════════════════

# @router.post("/ai-insight")
# def get_all_transformer_insights(req: ForecastRequest):
#     """
#     Returns the full transformer dashboard payload:
#         { data: {stats, charts, tables}, message, status }

#     Drives everything on the Analytics / Performance / Insights / AI-Insights tabs.
#     """
#     office_id   = int(req.office)
#     from_date   = req.fromDate
#     to_date     = req.toDate
#     section_ids = resolve_to_sections(state.HIERARCHY_TREE, office_id)
#     office_filter = {"$in": [int(s) for s in section_ids]}

#     # ── Parallel fetch all 15 model aggregates + raw collections ─────────────
#     fetch_tasks = {
#         "dt_count":       lambda: _count_transformers(req),
#         "load":           lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["load"],           from_date, to_date),
#         "peak":           lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["peak"],           from_date, to_date),
#         "pf":             lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["pf"],             from_date, to_date),
#         "voltage":        lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["voltage"],        from_date, to_date),
#         "imbalance":      lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["imbalance"],      from_date, to_date),
#         "pq":             lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["pq"],             from_date, to_date),
#         "overload":       lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["overload"],       from_date, to_date),
#         "ntl":            lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["ntl"],            from_date, to_date),
#         "lol":            lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["lol"],            from_date, to_date),
#         "load_factor":    lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["load_factor"],    from_date, to_date),
#         "thermal":        lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["thermal"],        from_date, to_date),
#         "volt_sag":       lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["volt_sag"],       from_date, to_date),
#         "overload_score": lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["overload_score"], from_date, to_date),
#         "freq_drift":     lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["freq_drift"],     from_date, to_date),
#         "neutral":        lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["neutral"],        from_date, to_date),
#         "ntl_raw":        lambda: _fetch_raw(_db, COL_NTL,      office_filter, from_date, to_date, "confidence"),
#         "thermal_raw":    lambda: _fetch_raw(_db, COL_THERMAL,  office_filter, from_date, to_date, "confidence"),
#         "lol_raw":        lambda: _fetch_raw(_db, COL_LOL,      office_filter, from_date, to_date, "value"),
#         "overload_raw":   lambda: _fetch_raw(_db, COL_OVERLOAD, office_filter, from_date, to_date, "confidence"),
#         "neutral_raw":    lambda: _fetch_raw(_db, COL_NEUTRAL,  office_filter, from_date, to_date, "confidence"),
#     }

#     results = {}
#     with ThreadPoolExecutor(max_workers=16) as pool:
#         futures = {pool.submit(fn): key for key, fn in fetch_tasks.items()}
#         for fut in as_completed(futures):
#             results[futures[fut]] = fut.result()

#     total_dts = results["dt_count"]

#     if not results["load"]:
#         raise HTTPException(
#             status_code=503,
#             detail=(f"No cached transformer forecast for office={office_id} "
#                     f"({from_date} → {to_date}). Run precompute first."),
#         )

#     # ── Extract all series ───────────────────────────────────────────────────
#     dates = _extract_dates(results["load"])

#     s_load      = _build_series(results["load"],           dates, "sum")
#     s_peak      = _build_series(results["peak"],           dates, "max")
#     s_pf        = _build_series(results["pf"],             dates, "mean")
#     s_voltage   = _build_series(results["voltage"],        dates, "mean")
#     s_imb       = _build_series(results["imbalance"],      dates, "max")
#     s_pq        = _build_series(results["pq"],             dates, "mean")
#     s_overload  = _build_series(results["overload"],       dates, "sum")
#     s_ntl       = _build_series(results["ntl"],            dates, "sum")
#     s_lol       = _build_series(results["lol"],            dates, "max")
#     s_lf        = _build_series(results["load_factor"],    dates, "mean")
#     s_thermal   = _build_series(results["thermal"],        dates, "sum")
#     s_voltsag   = _build_series(results["volt_sag"],       dates, "max")
#     s_oloadscr  = _build_series(results["overload_score"], dates, "mean")
#     s_freq      = _build_series(results["freq_drift"],     dates, "sum")
#     s_neutral   = _build_series(results["neutral"],        dates, "sum")

#     # ── Anchor-based live summary stats (from ANCHOR_INDEX DataFrame) ────────
#     df = resolve_transformers(req)

#     def _safe(fn, default=0.0):
#         try:
#             v = fn()
#             if pd.isna(v):
#                 return default
#             return v
#         except Exception:
#             return default

#     total_capacity_kva = float(_safe(lambda: df["transformer_capacity_kva"].sum()))
#     avg_load_pct       = float(_safe(lambda: df["load_percentage"].mean()))
#     avg_pf             = float(_safe(lambda: df["pf_value"].mean(), 1.0))
#     avg_oil_temp       = float(_safe(lambda: df["oil_temperature"].mean()))
#     avg_winding_temp   = float(_safe(lambda: df["winding_temperature"].mean()))
#     avg_hotspot        = float(_safe(lambda: df["hotspot_temperature"].mean()))
#     max_hotspot        = float(_safe(lambda: df["hotspot_temperature"].max()))
#     avg_lol            = float(_safe(lambda: df["loss_of_life_pct"].mean()))
#     max_lol            = float(_safe(lambda: df["loss_of_life_pct"].max()))
#     avg_pq             = float(_safe(lambda: df["overall_pq_index"].mean()))
#     avg_load_factor    = float(_safe(lambda: df["load_factor"].mean()))
#     pf_penalty_dts     = int(_safe(lambda: (df["pf_value"] < PF_PENALTY_THRESHOLD).sum()))
#     overload_dts       = int(_safe(lambda: (df["load_percentage"] > OVERLOAD_THRESH_PCT).sum()))
#     thermal_danger_dts = int(_safe(lambda: (df["hotspot_temperature"] > THERMAL_DANGER_TEMP).sum()))
#     ntl_events_live    = int(_safe(lambda: df["ntl_event"].sum()))
#     volt_imb_avg       = float(_safe(lambda: df["voltage_unbalance_pct"].mean()))
#     curr_imb_avg       = float(_safe(lambda: df["current_unbalance_pct"].mean()))

#     total_kwh       = round(sum(s_load), 2)
#     total_peak_kw   = round(max(s_peak) if s_peak else 0, 2)
#     peak_day        = dates[int(np.argmax(s_load))] if s_load else None
#     peak_kw_day     = dates[int(np.argmax(s_peak))] if s_peak else None

#     # ROI headline numbers (preview — full breakdown is in /roi endpoint)
#     est_energy_bill   = round(total_kwh * TARIFF_PER_KWH, 0)
#     est_pf_penalty    = round(est_energy_bill * (PF_PENALTY_PCT / 100) * (pf_penalty_dts / max(total_dts, 1)), 0)
#     est_mdi_penalty   = round(max(total_peak_kw - total_capacity_kva * 0.85, 0) * MDI_PENALTY_PER_KVA, 0)
#     est_monthly_save  = round(est_pf_penalty + est_mdi_penalty, 0)

#     # Build 3-phase voltage series from anchor (approximated from avg)
#     # (In production the precompute script should store vrn/vyn/vbn per-date.)
#     vrn = [round(v * 0.995, 2) for v in s_voltage]
#     vyn = [round(v * 1.000, 2) for v in s_voltage]
#     vbn = [round(v * 1.005, 2) for v in s_voltage]

#     # Phase shares (from anchor average — for the stacked chart)
#     r_share = float(_safe(lambda: df["phase_r_share"].mean(), 0.333)) * 100
#     y_share = float(_safe(lambda: df["phase_y_share"].mean(), 0.333)) * 100
#     b_share = float(_safe(lambda: df["phase_b_share"].mean(), 0.333)) * 100
#     # Broadcast to match date length (static share overlay)
#     rs = [round(r_share, 2)] * len(dates)
#     ys = [round(y_share, 2)] * len(dates)
#     bs = [round(b_share, 2)] * len(dates)

#     # Thermal series from anchor — overlay oil/winding/hotspot (smoothed, representative)
#     oil_series     = [round(avg_oil_temp,     1)] * len(dates)
#     winding_series = [round(avg_winding_temp, 1)] * len(dates)
#     hotspot_series = [round(avg_hotspot,      1)] * len(dates)

#     # ── CHARTS ───────────────────────────────────────────────────────────────
#     charts = [
#         build_chart("Energy Load Forecast",  s_load,  dates, "kWh",
#                     meta={"subtext": f"Total: {total_kwh:,.0f} kWh | Peak: {peak_day} | DTs: {total_dts}"}),

#         build_chart("Peak Demand vs MDI Threshold", s_peak, dates, "kW",
#                     meta={"subtext": f"Max: {total_peak_kw:,.2f} kW | Day: {peak_kw_day}"}),

#         build_chart("Power Factor Quality", s_pf, dates, "PF"),

#         build_3phase_voltage_chart(vrn, vyn, vbn, dates),

#         build_chart("Phase Imbalance Trend", s_imb, dates, "%"),

#         build_phase_share_chart(rs, ys, bs, dates),

#         build_chart("Power Quality Index", s_pq, dates, "Index"),

#         build_thermal_chart(oil_series, winding_series, hotspot_series, dates),

#         build_chart("Thermal Anomaly Trend", s_thermal, dates, "Alerts"),

#         build_chart("Loss of Life Accumulation", s_lol, dates, "%"),

#         build_chart("Load Factor (Right-Sizing)", s_lf, dates, "Factor"),

#         build_chart("Overload Risk", s_overload, dates, "Count"),

#         build_chart("Overload Risk Score (Hours-Ahead)", s_oloadscr, dates, "Score"),

#         build_chart("Voltage Sag/Swell Prediction", s_voltsag, dates, "V"),

#         build_chart("Frequency Drift Alerts", s_freq, dates, "Events"),

#         build_chart("Neutral Current Anomaly", s_neutral, dates, "Alerts"),

#         build_chart("NTL / Theft Detection", s_ntl, dates, "Alerts"),

#         build_gauge_chart("Overall Power Quality Gauge", "pq_gauge", avg_pq,
#                           0, 1,
#                           thresholds=[[0.5, "#ee6666"], [0.85, "#fac858"], [1.0, "#91cc75"]],
#                           description="Fleet avg PQ index — target > 0.85"),
#     ]

#     # ── STATS (Response-2 compliant: label, value, icon, tooltip, threshold_status) ──
#     avg_pf_rounded         = round(avg_pf, 4)
#     avg_pq_rounded         = round(avg_pq, 3)
#     avg_load_pct_rounded   = round(avg_load_pct, 2)
#     max_hotspot_rounded    = round(max_hotspot, 1)
#     avg_lol_rounded        = round(avg_lol, 2)
#     max_lol_rounded        = round(max_lol, 2)
#     volt_imb_rounded       = round(volt_imb_avg, 2)
#     curr_imb_rounded       = round(curr_imb_avg, 2)

#     stats = [
#         make_stat(
#             "Total Transformers", fmt(total_dts, "transformers"), "ri-building-line",
#             tooltip="Total number of distribution transformers registered under this office and its sub-sections.",
#         ),
#         make_stat(
#             "Fleet Capacity", fmt(round(total_capacity_kva, 0), "kVA"), "ri-flashlight-line",
#             tooltip="Combined nameplate capacity of all reporting transformers in the fleet.",
#         ),
#         make_stat(
#             "Total Energy Import", fmt(total_kwh, "kWh"), "ri-download-cloud-2-line",
#             tooltip="Cumulative energy drawn from the grid across all reporting transformers for the selected date range.",
#         ),
#         make_stat(
#             "Peak Demand", fmt(total_peak_kw, "kW"), "ri-bar-chart-line",
#             tooltip="Highest simultaneous real-power demand recorded across all transformers on any single day in the selected range.",
#         ),
#         make_stat(
#             "Avg Load %", fmt(avg_load_pct_rounded, "%"), "ri-dashboard-line",
#             threshold_status=build_threshold(
#                 avg_load_pct_rounded, warn=70, crit=OVERLOAD_THRESH_PCT,
#                 direction="higher_is_worse",
#             ),
#             tooltip="Average ratio of actual load to sanctioned capacity across transformers. High values signal infrastructure stress and potential overload risk.",
#         ),
#         make_stat(
#             "Avg Load Factor", fmt(round(avg_load_factor, 3), ""), "ri-scales-3-line",
#             threshold_status=build_threshold(
#                 round(avg_load_factor, 3), warn=0.30, crit=0.15,
#                 direction="lower_is_worse",
#             ),
#             tooltip="Fleet-wide average load factor (avg kW ÷ peak kW). Low values indicate oversized transformers wasting iron losses.",
#         ),
#         make_stat(
#             "Avg Power Factor", fmt(avg_pf_rounded, ""), "ri-pulse-line",
#             threshold_status=build_threshold(
#                 avg_pf_rounded, warn=PF_PENALTY_THRESHOLD, crit=0.85,
#                 direction="lower_is_worse",
#             ),
#             tooltip="Fleet-wide average power factor. Values below 0.90 indicate excessive reactive power draw, leading to grid inefficiency and possible utility penalties.",
#         ),
#         make_stat(
#             "PF Penalty DTs", fmt(pf_penalty_dts, "transformers"), "ri-error-warning-line",
#             tooltip="Number of transformers whose power factor has dropped below the minimum threshold, making them liable for reactive energy surcharges.",
#         ),
#         make_stat(
#             "Avg PQ Index", fmt(avg_pq_rounded, ""), "ri-sound-module-line",
#             threshold_status=build_threshold(
#                 avg_pq_rounded, warn=0.85, crit=0.70,
#                 direction="lower_is_worse",
#             ),
#             tooltip="Composite power-quality index combining voltage, current, PF, and frequency. Target above 0.85.",
#         ),
#         make_stat(
#             "Avg Oil Temp", fmt(round(avg_oil_temp, 1), "°C"), "ri-temp-cold-line",
#             tooltip="Average top-oil temperature across the fleet. Used for thermal stress and loss-of-life calculations.",
#         ),
#         make_stat(
#             "Avg Winding Temp", fmt(round(avg_winding_temp, 1), "°C"), "ri-temp-hot-line",
#             tooltip="Average winding temperature — an earlier indicator of thermal stress than oil temperature.",
#         ),
#         make_stat(
#             "Avg Hotspot Temp", fmt(round(avg_hotspot, 1), "°C"), "ri-fire-line",
#             tooltip="Average hotspot (worst-spot) temperature. Sustained readings above 85 °C accelerate insulation aging per IEEE C57.91.",
#         ),
#         make_stat(
#             "Max Hotspot Temp", fmt(max_hotspot_rounded, "°C"), "ri-alarm-warning-fill",
#             threshold_status=build_threshold(
#                 max_hotspot_rounded, warn=75, crit=THERMAL_DANGER_TEMP,
#                 direction="higher_is_worse",
#             ),
#             tooltip="Highest hotspot temperature observed on any transformer. Breaches of 85 °C trigger thermal-danger alerts.",
#         ),
#         make_stat(
#             "Thermal Danger DTs", fmt(thermal_danger_dts, "transformers"), "ri-alert-line",
#             tooltip="Number of transformers breaching the 85 °C hotspot safety threshold.",
#         ),
#         make_stat(
#             "Avg Loss-of-Life", fmt(avg_lol_rounded, "%"), "ri-heart-pulse-line",
#             threshold_status=build_threshold(
#                 avg_lol_rounded, warn=40, crit=60,
#                 direction="higher_is_worse",
#             ),
#             tooltip="Fleet average cumulative loss-of-life percentage (IEEE C57.91 aging acceleration).",
#         ),
#         make_stat(
#             "Worst LoL DT", fmt(max_lol_rounded, "%"), "ri-skull-2-line",
#             threshold_status=build_threshold(
#                 max_lol_rounded, warn=60, crit=LOL_CRITICAL_PCT,
#                 direction="higher_is_worse",
#             ),
#             tooltip="Highest loss-of-life reading in the fleet — candidate for urgent replacement planning.",
#         ),
#         make_stat(
#             "Overload DTs", fmt(overload_dts, "transformers"), "ri-alarm-warning-line",
#             tooltip="Number of transformers that breached the critical loading threshold (>80%), indicating feeder overload conditions.",
#         ),
#         make_stat(
#             "NTL / Theft Alerts", fmt(ntl_events_live, "alerts"), "ri-spy-line",
#             tooltip="Transformers flagged by the NTL detection model for suspected non-technical loss or energy theft, based on consumption pattern anomalies above the confidence threshold.",
#         ),
#         make_stat(
#             "Voltage Imbalance", fmt(volt_imb_rounded, "%"), "ri-equalizer-line",
#             threshold_status=build_threshold(
#                 volt_imb_rounded, warn=5.0, crit=10.0,
#                 direction="higher_is_worse",
#             ),
#             tooltip="Average percentage difference in voltage across the three phases. Persistent imbalance causes motor heating, equipment stress, and increased losses.",
#         ),
#         make_stat(
#             "Current Imbalance", fmt(curr_imb_rounded, "%"), "ri-equalizer-fill",
#             threshold_status=build_threshold(
#                 curr_imb_rounded, warn=10.0, crit=25.0,
#                 direction="higher_is_worse",
#             ),
#             tooltip="Average percentage difference in current across the three phases. High imbalance indicates uneven load distribution and may lead to neutral conductor overheating.",
#         ),
#         make_stat(
#             "Est. Energy Bill", fmt(int(est_energy_bill), "₹"), "ri-money-rupee-circle-line",
#             tooltip="Estimated energy cost for the period using the blended APDCL industrial-commercial tariff.",
#         ),
#         make_stat(
#             "Est. Monthly Savings", fmt(int(est_monthly_save), "₹"), "ri-money-dollar-circle-line",
#             tooltip="Combined monthly savings achievable from PF correction and MDI penalty avoidance (preview — see /roi endpoint for full breakdown).",
#         ),
#         make_stat(
#             "MDI Penalty Risk", fmt(int(est_mdi_penalty), "₹"), "ri-arrow-up-circle-line",
#             tooltip="Estimated Maximum Demand Indicator penalty exposure if peak demand exceeds the sanctioned contract capacity.",
#         ),
#     ]

#     # ── TABLES ───────────────────────────────────────────────────────────────
#     NTL_CONF_THRESH = 50.0

#     def _build_high_risk_dts(page: int, page_size: int) -> dict:
#         """Top transformers by combined risk score (overload + thermal + LoL)."""
#         rows = []
#         if not df.empty:
#             dfc = df.copy()
#             dfc["risk_score"] = (
#                 dfc.get("overload_risk",      pd.Series(0)).fillna(0) * 0.35 +
#                 dfc.get("thermal_stress_index", pd.Series(0)).fillna(0) * 0.30 +
#                 dfc.get("loss_of_life_pct",   pd.Series(0)).fillna(0) / 100 * 0.25 +
#                 (1 - dfc.get("health_score",  pd.Series(1)).fillna(1)) * 0.10
#             )
#             dfc = dfc.sort_values("risk_score", ascending=False)
#             for _, r in dfc.iterrows():
#                 rows.append({
#                     "_id":           f"RISK-{r['assetUid']}",
#                     "assetUid":      str(r["assetUid"]),
#                     "assetName":     f"DT-{r['assetUid']}",
#                     "capacity_kva":  round(float(r.get("transformer_capacity_kva", 0)), 1),
#                     "load_pct":      round(float(r.get("load_percentage", 0)), 1),
#                     "hotspot_c":     round(float(r.get("hotspot_temperature", 0)), 1),
#                     "lol_pct":       round(float(r.get("loss_of_life_pct", 0)), 2),
#                     "pf":            round(float(r.get("pf_value", 1)), 3),
#                     "risk_score":    round(float(r["risk_score"]), 3),
#                 })
#         p = paginate(rows, page, page_size)
#         return {
#             "tableId":    "high_risk_dts",
#             "title":      "Top-Risk Transformers",
#             "data":       p["data"],
#             "pagination": p["pagination"],
#         }

#     def _build_ntl_alerts(page: int, page_size: int) -> dict:
#         rows = []
#         for r in results["ntl_raw"]:
#             conf = float(r.get("confidence", 0))
#             if conf >= NTL_CONF_THRESH:
#                 rows.append({
#                     "_id":        f"NTL-{r['assetUid']}-{r.get('date')}",
#                     "assetUid":   str(r["assetUid"]),
#                     "assetName":  f"DT-{r['assetUid']}",
#                     "date":       r.get("date"),
#                     "timestamp":  r.get("timestamp"),
#                     "status":     "ALERT",
#                     "confidence": f"{round(conf, 1)}%",
#                 })
#         p = paginate(rows, page, page_size)
#         return {
#             "tableId":    "ntl_alerts",
#             "title":      "NTL / Theft Alerts",
#             "data":       p["data"],
#             "pagination": p["pagination"],
#         }

#     def _build_thermal_alerts(page: int, page_size: int) -> dict:
#         rows = []
#         for r in results["thermal_raw"]:
#             conf = float(r.get("confidence", 0))
#             rows.append({
#                 "_id":        f"THERM-{r['assetUid']}-{r.get('date')}",
#                 "assetUid":   str(r["assetUid"]),
#                 "assetName":  f"DT-{r['assetUid']}",
#                 "date":       r.get("date"),
#                 "hotspot_c":  round(float(r.get("hotspot_temperature", 0)), 1),
#                 "gradient_c": round(float(r.get("thermal_gradient", 0)), 1),
#                 "status":     "THERMAL ANOMALY",
#                 "confidence": f"{round(conf, 1)}%",
#             })
#         p = paginate(rows, page, page_size)
#         return {
#             "tableId":    "thermal_alerts",
#             "title":      "Thermal Anomaly Alerts",
#             "data":       p["data"],
#             "pagination": p["pagination"],
#         }

#     def _build_lol_top10(page: int, page_size: int) -> dict:
#         """Transformers nearing end-of-life — prioritise replacement planning."""
#         rows = []
#         if not df.empty and "loss_of_life_pct" in df.columns:
#             dfc = df.sort_values("loss_of_life_pct", ascending=False).head(100)
#             for _, r in dfc.iterrows():
#                 lol = float(r.get("loss_of_life_pct", 0))
#                 rows.append({
#                     "_id":           f"LOL-{r['assetUid']}",
#                     "assetUid":      str(r["assetUid"]),
#                     "assetName":     f"DT-{r['assetUid']}",
#                     "capacity_kva":  round(float(r.get("transformer_capacity_kva", 0)), 1),
#                     "lol_pct":       round(lol, 2),
#                     "remaining_pct": round(100 - lol, 2),
#                     "aging_index":   round(float(r.get("aging_index", 0)), 3),
#                     "priority":      "URGENT" if lol >= LOL_CRITICAL_PCT else
#                                      "HIGH"   if lol >= 60 else
#                                      "MEDIUM" if lol >= 40 else "LOW",
#                 })
#         p = paginate(rows, page, page_size)
#         return {
#             "tableId":    "lol_replacement",
#             "title":      "Loss-of-Life Replacement Priority",
#             "data":       p["data"],
#             "pagination": p["pagination"],
#         }

#     def _build_right_size_candidates(page: int, page_size: int) -> dict:
#         """DTs with load factor < 0.30 → right-sizing candidates (iron-loss saving)."""
#         rows = []
#         if not df.empty and "load_factor" in df.columns:
#             dfc = df[df["load_factor"].fillna(1) < 0.30].sort_values("load_factor")
#             for _, r in dfc.iterrows():
#                 cap = float(r.get("transformer_capacity_kva", 0))
#                 lf  = float(r.get("load_factor", 0))
#                 rec_cap = round(cap * max(lf * 1.5, 0.3))   # suggested new size
#                 rows.append({
#                     "_id":              f"RS-{r['assetUid']}",
#                     "assetUid":         str(r["assetUid"]),
#                     "assetName":        f"DT-{r['assetUid']}",
#                     "current_cap_kva":  round(cap, 0),
#                     "load_factor":      round(lf, 3),
#                     "avg_load_pct":     round(float(r.get("load_percentage", 0)), 1),
#                     "recommended_kva":  rec_cap,
#                     "est_saving_rs":    int(max(cap - rec_cap, 0) * 450),  # ₹450/kVA/yr iron-loss saving
#                 })
#         p = paginate(rows, page, page_size)
#         return {
#             "tableId":    "right_size",
#             "title":      "Right-Sizing Candidates (Low Load Factor)",
#             "data":       p["data"],
#             "pagination": p["pagination"],
#         }

#     TABLE_BUILDERS = {
#         "high_risk_dts":   _build_high_risk_dts,
#         "ntl_alerts":      _build_ntl_alerts,
#         "thermal_alerts":  _build_thermal_alerts,
#         "lol_replacement": _build_lol_top10,
#         "right_size":      _build_right_size_candidates,
#     }

#     requested_tables = req.tables or [
#         TableRequest(tableId="high_risk_dts",   page=1, pageSize=10),
#         TableRequest(tableId="ntl_alerts",      page=1, pageSize=10),
#         TableRequest(tableId="thermal_alerts",  page=1, pageSize=10),
#         TableRequest(tableId="lol_replacement", page=1, pageSize=10),
#         TableRequest(tableId="right_size",      page=1, pageSize=10),
#     ]

#     tables_out = []
#     for t in requested_tables:
#         builder = TABLE_BUILDERS.get(t.tableId)
#         if builder is None:
#             continue
#         tables_out.append(builder(t.page, t.pageSize))

#     return {
#         "data": {
#             "stats":  stats,
#             "charts": charts,
#             "tables": tables_out,
#             "forecast_meta": build_forecast_meta(
#                 from_date=req.fromDate,
#                 to_date=req.toDate,
#                 models={
#                     "load":             {"type": "regression",     "unit": "kWh",    "aggregation": "sum"},
#                     "peak":             {"type": "regression",     "unit": "kW",     "aggregation": "max"},
#                     "pf":               {"type": "regression",     "unit": "ratio",  "aggregation": "mean"},
#                     "voltage":          {"type": "regression",     "unit": "V",      "aggregation": "mean"},
#                     "imbalance":        {"type": "regression",     "unit": "%",      "aggregation": "max"},
#                     "pq":               {"type": "regression",     "unit": "index",  "aggregation": "mean"},
#                     "overload":         {"type": "classification", "unit": "events", "aggregation": "sum"},
#                     "ntl":              {"type": "classification", "unit": "alerts", "aggregation": "sum"},
#                     "lol":              {"type": "regression",     "unit": "%",      "aggregation": "max"},
#                     "load_factor":      {"type": "regression",     "unit": "ratio",  "aggregation": "mean"},
#                     "thermal":          {"type": "classification", "unit": "alerts", "aggregation": "sum"},
#                     "volt_sag":         {"type": "regression",     "unit": "V",      "aggregation": "max"},
#                     "overload_score":   {"type": "regression",     "unit": "score",  "aggregation": "mean"},
#                     "freq_drift":       {"type": "classification", "unit": "events", "aggregation": "sum"},
#                     "neutral":          {"type": "classification", "unit": "alerts", "aggregation": "sum"},
#                 },
#                 training_period="2026-01-01 to 2026-04-03",
#             ),
#         },
#         "message": "success",
#         "status":  True,
#     }


# # ═════════════════════════════════════════════════════════════════════════════
# # DRILLDOWN: /insigh-test-drill
# # ═════════════════════════════════════════════════════════════════════════════

# @router.post("/ai-insight-drill")
# def get_insight_drilldown(req: DrilldownRequest):
#     """
#     Per-transformer drilldown for any of the 15 model codes on a specific date.
#     Mirrors smart-meter /insigh-test-drill contract.
#     """
#     section_ids   = resolve_to_sections(state.HIERARCHY_TREE, int(req.office))
#     office_filter = {"$in": [int(s) for s in section_ids]}
#     meta          = MODEL_META[req.code]
#     model_key     = MODEL_KEY_MAP[req.code]

#     client = MongoClient(MONGO_URI)
#     db     = client[DB_NAME]
#     rows   = []

#     # ── Per-DT drilldown for NTL, thermal, overload, neutral, freq, LoL ──
#     raw_collections = {
#         "ntl":      COL_NTL,
#         "thermal":  COL_THERMAL,
#         "overload": COL_OVERLOAD,
#         "neutral":  COL_NEUTRAL,
#         "freq_drift": COL_FREQ,
#         "lol":      COL_LOL,
#         "load":     COL_ENERGY,
#     }

#     if req.code in raw_collections:
#         coll = raw_collections[req.code]
#         raw  = list(
#             db[coll].find(
#                 {"office_id": office_filter, "date": req.date},
#                 {"_id": 0},
#             ).sort("confidence" if req.code != "load" else "value", -1)
#         )
#         for r in raw:
#             conf = float(r.get("confidence", 0)) if "confidence" in r else None
#             rows.append({
#                 "assetUid":    str(r.get("assetUid", "")),
#                 "assetName":   f"DT-{r.get('assetUid', '')}",
#                 "date":        req.date,
#                 "timestamp":   r.get("timestamp"),
#                 "value":       round(float(r.get("value", 0)), 4),
#                 meta["label"]: round(float(r.get("value", 0)), 4),
#                 "status":      "ALERT" if conf and conf >= 50 else "Normal",
#                 "confidence":  f"{round(conf, 1)}%" if conf is not None else None,
#             })
#         total = (sum(1 for r in rows if r.get("status") == "ALERT")
#                  if req.code in ("ntl", "thermal", "overload", "neutral", "freq_drift")
#                  else round(sum(r.get("value", 0) for r in rows), 2))
#     else:
#         # Aggregated-only: peak | pf | voltage | imbalance | pq | overload_score |
#         # volt_sag | load_factor — we don't keep per-DT raw, so fetch aggregated row.
#         agg = db[COL_AGG].find_one(
#             {
#                 "office_id": office_filter,
#                 "model_key": model_key,
#                 "date":      req.date,
#             },
#             {"_id": 0, "value": 1, "day_name": 1},
#         )
#         val = round(float(agg["value"]), 4) if agg else 0
#         rows.append({
#             "date":         req.date,
#             "day_name":     agg.get("day_name", "") if agg else "",
#             "scope":        "All transformers (aggregated)",
#             meta["label"]:  val,
#         })
#         total = val

#     client.close()
#     p = paginate(rows, req.page, req.pageSize)

#     _unit = meta["unit"]
#     if _unit == "":
#         _total_display = total
#     elif _unit in ("₹", "$", "€", "£"):
#         _total_display = f"{_unit}{total}"
#     else:
#         _total_display = f"{total} {_unit}"

#     return {
#         "data": {
#             "date":   req.date,
#             "office": req.office,
#             "code":   req.code,
#             "summary": {
#                 "total": _total_display,
#                 "label": meta["label"],
#                 "note":  ("per-transformer"
#                           if req.code in raw_collections
#                           else "aggregated only — no per-DT raw stored"),
#             },
#             "table": {
#                 "tableId":    "drilldown_transformers",
#                 "title":      f"{req.code.upper()} breakdown — {req.date}",
#                 "data":       p["data"],
#                 "pagination": p["pagination"],
#             },
#         },
#         "message": "success",
#         "status":  True,
#     }



# transformer_insights.py — CORE DASHBOARD ENDPOINT for 15 Transformer Models
# ─────────────────────────────────────────────────────────────────────────────
#
# Mirrors smartmeter_insights.py architecture but scaled for Distribution
# Transformer (DT) fleet-level analytics:
#
#   • 15 forecast/anomaly models (vs 7 for smart meters)
#   • Thermal / aging / LoL dimension
#   • 3-phase voltage & current profile
#   • Rich chart set (14 charts) with better visual treatments
#   • ROI-aware stat cards (penalty-zone meters, MDI exceedance, right-size targets)
#
#   Response shape: { data: {stats, charts, tables}, message, status }
# ─────────────────────────────────────────────────────────────────────────────

from fastapi import APIRouter, HTTPException
import pandas as pd
import numpy as np

router = APIRouter(tags=["Transformer Insights"])

from pydantic import BaseModel, field_validator, model_validator, Field
from typing import Optional
from datetime import datetime
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import state
from state import resolve_to_sections
from stats_helpers import fmt, make_stat, build_threshold, build_forecast_meta


# ═════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═════════════════════════════════════════════════════════════════════════════

MAX_DATE_RANGE = 90

MONGO_URI = ""
DB_NAME   = "iotdb"

# Pre-computed forecast cache collections (populate via precompute_forecasts.py)
# COL_AGG         = "transformer_forecast_aggregated"     # one row per (asset, model, date)
# COL_NTL         = "transformer_forecast_ntl_raw"
# COL_THERMAL     = "transformer_forecast_thermal_raw"
# COL_OVERLOAD    = "transformer_forecast_overload_raw"
# COL_LOL         = "transformer_forecast_lol_raw"
# COL_FREQ        = "transformer_forecast_freq_raw"
# COL_NEUTRAL     = "transformer_forecast_neutral_raw"
# COL_ENERGY      = "transformer_forecast_energy_raw"     # per-DT energy (for drilldown)

COL_AGG      = "transformer_forecast_aggregated_daily_v1"
COL_NTL      = "transformer_forecast_ntl_raw_daily_v1"
COL_THERMAL  = "transformer_forecast_thermal_raw_daily_v1"
COL_OVERLOAD = "transformer_forecast_overload_raw_daily_v1"
COL_NEUTRAL  = "transformer_forecast_neutral_raw_daily_v1"
COL_FREQ     = "transformer_forecast_freq_raw_daily_v1"
COL_LOL      = "transformer_forecast_lol_raw_daily_v1"
COL_ENERGY   = "transformer_forecast_energy_raw_daily_v1"



# ──────────────────────────────────────────────────────────────────────────
# APDCL Indian tariff constants (used by stats + ROI calculations)
# ──────────────────────────────────────────────────────────────────────────
TARIFF_PER_KWH          = 8.50      # ₹/kWh (avg blended industrial + commercial)
MDI_PENALTY_PER_KVA     = 350.00    # ₹/kVA/month above contract demand
PF_PENALTY_PCT          = 1.5       # ~1.5% surcharge on energy bill if PF < 0.90
PF_PENALTY_THRESHOLD    = 0.90
OVERLOAD_THRESH_PCT     = 80.0      # load% above this = overload
THERMAL_DANGER_TEMP     = 85.0      # °C hotspot
NOMINAL_VOLTAGE         = 230.0
VOLTAGE_TOLERANCE_PCT   = 6.0       # ±6% acceptable band
NOMINAL_FREQUENCY       = 50.0      # Hz
FREQ_DRIFT_THRESH       = 0.15      # Hz
LOL_CRITICAL_PCT        = 80.0      # remaining life < 20% → replace planned


# ──────────────────────────────────────────────────────────────────────────
# Model → dashboard code mapping
# ──────────────────────────────────────────────────────────────────────────
MODEL_KEY_MAP = {
    "load":             "dt_load_forecast_model.pkl",
    "peak":             "dt_peak_demand_model.pkl",
    "pf":               "dt_reactive_pf_model.pkl",
    "voltage":          "dt_voltage_profile_model.pkl",
    "imbalance":        "dt_phase_imbalance_model.pkl",
    "pq":               "dt_pq_index_model.pkl",
    "overload":         "dt_overload_risk_model.pkl",
    "ntl":              "dt_ntl_theft_model.pkl",
    "lol":              "dt_loss_of_life_model.pkl",
    "load_factor":      "dt_load_factor_model.pkl",
    "thermal":          "dt_thermal_anomaly_model.pkl",
    "volt_sag":         "dt_volt_sag_swell_model.pkl",
    "overload_score":   "dt_overload_risk_score_model.pkl",
    "freq_drift":       "dt_freq_drift_model.pkl",
    "neutral":          "dt_neutral_anomaly_model.pkl",
}

MODEL_META = {
    "load":           {"label": "Energy (kWh)",       "unit": "kWh", "agg": "sum"},
    "peak":           {"label": "Peak Demand (kW)",   "unit": "kW",  "agg": "max"},
    "pf":             {"label": "Power Factor",       "unit": "",    "agg": "mean"},
    "voltage":        {"label": "Voltage (V)",        "unit": "V",   "agg": "mean"},
    "imbalance":      {"label": "Current Imbal. (%)", "unit": "%",   "agg": "max"},
    "pq":             {"label": "PQ Index",           "unit": "",    "agg": "mean"},
    "overload":       {"label": "Overload Flag",      "unit": "",    "agg": "sum"},
    "ntl":            {"label": "NTL Alert",          "unit": "",    "agg": "count"},
    "lol":            {"label": "Loss of Life (%)",   "unit": "%",   "agg": "max"},
    "load_factor":    {"label": "Load Factor",        "unit": "",    "agg": "mean"},
    "thermal":        {"label": "Thermal Anomaly",    "unit": "",    "agg": "sum"},
    "volt_sag":       {"label": "Voltage Deviation",  "unit": "V",   "agg": "max"},
    "overload_score": {"label": "Overload Risk Score","unit": "",    "agg": "mean"},
    "freq_drift":     {"label": "Freq Drift Events",  "unit": "",    "agg": "sum"},
    "neutral":        {"label": "Neutral Anomaly",    "unit": "",    "agg": "sum"},
}


# ──────────────────────────────────────────────────────────────────────────
# CHART CONFIGS — richer styling than smart meter + transformer-specific bands
# ──────────────────────────────────────────────────────────────────────────
CHART_CONFIGS = {
    "Energy Load Forecast": {
        "description": "Forecasted total DT energy consumption — capacity planning baseline",
        "code":  "load",   "y_name": "kWh",   "color": "#5470c6",
        "type":  "bar",    "area":   False,
    },
    "Peak Demand vs MDI Threshold": {
        "description": "Peak kW demand per day with MDI penalty line — avoid contract exceedance",
        "code":  "peak",   "y_name": "kW",    "color": "#f59e0b",
        "type":  "line",   "area":   True,
    },
    "Power Factor Quality": {
        "description": "Average PF with penalty zone (< 0.90) shaded",
        "code":  "pf",     "y_name": "PF",    "color": "#91cc75",
        "area":  False,    "y_min":  0.7,     "y_max": 1.02,
        "mark_line":  {"yAxis": PF_PENALTY_THRESHOLD, "label": "0.90 Penalty Floor", "color": "#ee6666"},
        "mark_area":  {"yAxis": [0.7, PF_PENALTY_THRESHOLD], "color": "rgba(238,102,102,0.08)"},
    },
    "3-Phase Voltage Profile": {
        "description": "VRN / VYN / VBN voltage with ±6% tolerance bands (IS 12360)",
        "code":  "voltage","y_name": "Volts (V)","color": "#73c0de",
        "area":  False,
        "mark_line":  {"yAxis": NOMINAL_VOLTAGE, "label": "230V Nominal", "color": "#5470c6"},
    },
    "Phase Imbalance Trend": {
        "description": "Current imbalance % — triggers I²R copper-loss inefficiency",
        "code":  "imbalance", "y_name": "%",  "color": "#fac858",
        "type":  "bar",    "area":   False,
        "mark_line":  {"yAxis": 10, "label": "10% Alert", "color": "#ee6666"},
    },
    "Power Quality Index": {
        "description": "Composite PQ index (voltage + current + PF + frequency)",
        "code":  "pq",     "y_name": "Index", "color": "#9a60b4",
        "type":  "line",   "area":   True,    "y_min": 0, "y_max": 1.05,
        "mark_line":  {"yAxis": 0.85, "label": "0.85 Good PQ", "color": "#91cc75"},
    },
    "Overload Risk": {
        "description": "Daily count of DTs breaching 80% loading or 85°C hotspot",
        "code":  "overload","y_name":"Count","color":"#ee6666",
        "type":  "bar",    "area":   False,
    },
    "NTL / Theft Detection": {
        "description": "Transformers flagged for non-technical loss (tamper/theft)",
        "code":  "ntl",    "y_name": "Alerts","color": "#fc8452",
        "type":  "bar",    "area":   False,
    },
    "Thermal Anomaly Trend": {
        "description": "Hotspot-oil gradient > 25°C while load < 50% = insulation fault signature",
        "code":  "thermal","y_name": "Alerts","color": "#e76f51",
        "type":  "bar",    "area":   False,
    },
    "Loss of Life Accumulation": {
        "description": "Cumulative LoL% — IEEE C57.91 aging acceleration tracker",
        "code":  "lol",    "y_name": "%",     "color": "#bf3131",
        "type":  "line",   "area":   True,
        "mark_line":  {"yAxis": LOL_CRITICAL_PCT, "label": "80% Replace-Plan", "color": "#ee6666"},
    },
    "Load Factor (Right-Sizing)": {
        "description": "Fleet avg load factor — low LF means oversized DTs wasting iron losses",
        "code":  "load_factor","y_name":"Factor","color":"#2a9d8f",
        "type":  "line",   "area":   True,    "y_min": 0, "y_max": 1.05,
        "mark_line":  {"yAxis": 0.30, "label": "30% Right-Size", "color": "#fac858"},
    },
    "Overload Risk Score (Hours-Ahead)": {
        "description": "Forward-looking overload risk score — enables pre-emptive load transfer",
        "code":  "overload_score","y_name":"Score","color":"#d62728",
        "type":  "line",   "area":   True,
    },
    "Voltage Sag/Swell Prediction": {
        "description": "Forecasted voltage deviation — OLTC wear / upstream-grid health indicator",
        "code":  "volt_sag","y_name":"V","color":"#3d85c6",
        "type":  "line",   "area":   True,
    },
    "Frequency Drift Alerts": {
        "description": "Frequency drift events beyond ±0.15 Hz — grid stability precursor",
        "code":  "freq_drift","y_name":"Events","color":"#8854d0",
        "type":  "bar",    "area":   False,
    },
    "Neutral Current Anomaly": {
        "description": "Broken-neutral / earth-fault signatures — SAFETY CRITICAL",
        "code":  "neutral","y_name":"Alerts","color":"#c0392b",
        "type":  "bar",    "area":   False,
    },
}


# ═════════════════════════════════════════════════════════════════════════════
# PYDANTIC MODELS
# ═════════════════════════════════════════════════════════════════════════════

class TableRequest(BaseModel):
    tableId:  str
    page:     int = 1
    pageSize: int = 10


class ForecastRequest(BaseModel):
    fromDate: str = Field(..., alias="from_date")
    toDate:   str = Field(..., alias="to_date")
    office:   int
    assetId:  Optional[str]              = None
    tables:   Optional[list[TableRequest]] = None

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


class DrilldownRequest(BaseModel):
    date:     str
    office:   int
    code:     str
    page:     int = 1
    pageSize: int = 10

    @field_validator("code")
    @classmethod
    def validate_code(cls, v):
        if v not in MODEL_KEY_MAP:
            raise ValueError(f"code must be one of {list(MODEL_KEY_MAP.keys())}")
        return v


# ═════════════════════════════════════════════════════════════════════════════
# MONGO (shared pool)
# ═════════════════════════════════════════════════════════════════════════════

_mongo_client = MongoClient(
    MONGO_URI,
    maxPoolSize=25,
    serverSelectionTimeoutMS=3000,
    connectTimeoutMS=3000,
)
_db = _mongo_client[DB_NAME]


# ═════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def resolve_transformers(req) -> pd.DataFrame:
    """Filter ANCHOR_INDEX to transformers under the requested office (and optional assetId)."""
    if state.ANCHOR_INDEX is None or state.ANCHOR_INDEX.empty:
        raise Exception("ANCHOR_INDEX is empty")
    if state.HIERARCHY_TREE is None:
        raise Exception("Hierarchy not initialized")

    df          = state.ANCHOR_INDEX.copy()
    office_id   = int(req.office)
    section_ids = resolve_to_sections(state.HIERARCHY_TREE, office_id)

    # Transformer data uses `officeID` (camel-ish) per train.py's flatten_chunk
    if "officeID" in df.columns:
        df = df[df["officeID"].astype(str).isin([str(s) for s in section_ids])]
    elif "officeId" in df.columns:
        df = df[df["officeId"].astype(int).isin(section_ids)]

    if req.assetId:
        df = df[df["assetUid"].astype(str) == str(req.assetId)]

    return df


def _count_transformers(req) -> int:
    if state.ANCHOR_INDEX is None or state.ANCHOR_INDEX.empty:
        return 0
    section_ids = resolve_to_sections(state.HIERARCHY_TREE, int(req.office))

    if "officeID" in state.ANCHOR_INDEX.columns:
        mask = state.ANCHOR_INDEX["officeID"].astype(str).isin([str(s) for s in section_ids])
    else:
        mask = state.ANCHOR_INDEX["officeId"].astype(int).isin(section_ids)

    if req.assetId:
        mask &= state.ANCHOR_INDEX["assetUid"].astype(str) == str(req.assetId)
    return int(mask.sum())


def paginate(data: list, page: int, page_size: int) -> dict:
    total       = len(data)
    total_pages = max(1, (total + page_size - 1) // page_size)
    start       = (page - 1) * page_size
    end         = start + page_size
    return {
        "data": data[start:end],
        "pagination": {
            "hasNextPage":     page < total_pages,
            "hasPreviousPage": page > 1,
            "currentPage":     page,
            "pageSize":        page_size,
            "totalPages":      total_pages,
            "totalCount":      total,
        },
    }


def _fetch_agg(db, office_filter, model_key, from_date, to_date):
    return list(
        db[COL_AGG].find(
            {
                "office_id": office_filter,
                "model_key": model_key,
                "date":      {"$gte": from_date, "$lte": to_date},
            },
            {"_id": 0},
        ).sort("date", 1)
    )


def _fetch_raw(db, collection, office_filter, from_date, to_date, sort_field="confidence"):
    return list(
        db[collection].find(
            {
                "office_id": office_filter,
                "date":      {"$gte": from_date, "$lte": to_date},
            },
            {"_id": 0},
        ).sort(sort_field, -1)
    )


def _extract_dates(data):
    return sorted({r["date"] for r in data})


def _build_series(data, dates, agg="sum"):
    d = defaultdict(list)
    for r in data:
        d[r["date"]].append(r.get("value", 0))

    result = {}
    for date, vals in d.items():
        if not vals:
            result[date] = 0
        elif agg == "sum":
            result[date] = sum(vals)
        elif agg == "mean":
            result[date] = sum(vals) / len(vals)
        elif agg == "max":
            result[date] = max(vals)
        elif agg == "count":
            # count of flagged (value==1) for classifier aggregates
            result[date] = sum(1 for v in vals if v == 1 or v is True)
    return [round(result.get(dt, 0), 4) for dt in dates]


# ═════════════════════════════════════════════════════════════════════════════
# CHART BUILDER — richer than smart-meter (3-phase series + mark areas + gauges)
# ═════════════════════════════════════════════════════════════════════════════

def build_chart(title, data, dates, y_name="Value",
                series_list=None, legend_data=None, meta=None):
    cfg          = CHART_CONFIGS.get(title, {})
    color        = cfg.get("color", "#5470c6")
    chart_type   = cfg.get("type", "line")
    area         = cfg.get("area", False)
    y_min        = cfg.get("y_min")
    y_max        = cfg.get("y_max")
    mark_line    = cfg.get("mark_line")
    mark_area    = cfg.get("mark_area")
    final_y_name = cfg.get("y_name", y_name)
    code         = cfg.get("code", title.lower().replace(" ", "_"))
    description  = cfg.get("description", "")

    y_axis = {"type": "value", "name": final_y_name}
    if y_min is not None: y_axis["min"] = y_min
    if y_max is not None: y_axis["max"] = y_max

    if series_list:
        series = series_list
    else:
        single = {
            "name":   title,
            "type":   chart_type,
            "data":   data,
            "symbol": "none",
        }
        if chart_type == "line":
            single.update({
                "smooth":    True,
                "lineStyle": {"width": 3, "color": color},
                "itemStyle": {"color": color},
            })
            if area:
                single["areaStyle"] = {
                    "color": {
                        "type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
                        "colorStops": [
                            {"offset": 0, "color": color},
                            {"offset": 1, "color": "rgba(0,0,0,0)"},
                        ],
                    },
                    "opacity": 0.4,
                }
        elif chart_type == "bar":
            single.update({
                "itemStyle": {"color": color, "borderRadius": [6, 6, 0, 0]},
                "barWidth":  "55%",
            })

        marks = []
        if mark_line:
            single["markLine"] = {
                "silent": True,
                "symbol": "none",
                "data":  [{"yAxis": mark_line["yAxis"],
                           "label": {"formatter": mark_line["label"], "position": "insideEndTop"}}],
                "lineStyle": {"type": "dashed", "color": mark_line["color"], "width": 2},
            }
        if mark_area:
            single["markArea"] = {
                "silent": True,
                "itemStyle": {"color": mark_area["color"]},
                "data": [[
                    {"yAxis": mark_area["yAxis"][0]},
                    {"yAxis": mark_area["yAxis"][1]},
                ]],
            }

        series = [single]

    return {
        "title":       title,
        "code":        code,
        "type":        "chartCard",
        "info":        description,
        "subtext":     meta.get("subtext") if meta else "",
        "data": {
            "option": {
                "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
                "legend":  {"data": legend_data or [title], "top": "2%"},
                "grid":    {"left": "3%", "right": "4%", "bottom": "12%", "containLabel": True},
                "xAxis":   {"type": "category", "data": dates, "axisLabel": {"rotate": 45}},
                "yAxis":   y_axis,
                "series":  series,
            }
        },
    }


def build_3phase_voltage_chart(vrn_series, vyn_series, vbn_series, dates):
    """Dedicated 3-phase voltage chart with R/Y/B overlays + ±6% band."""
    tol_high = NOMINAL_VOLTAGE * (1 + VOLTAGE_TOLERANCE_PCT / 100)
    tol_low  = NOMINAL_VOLTAGE * (1 - VOLTAGE_TOLERANCE_PCT / 100)

    def mk(name, data, color):
        return {
            "name":      name,
            "type":      "line",
            "data":      data,
            "smooth":    True,
            "symbol":    "none",
            "lineStyle": {"width": 2.5, "color": color},
            "itemStyle": {"color": color},
        }

    series = [
        mk("VRN (R Phase)", vrn_series, "#ee6666"),
        mk("VYN (Y Phase)", vyn_series, "#fac858"),
        mk("VBN (B Phase)", vbn_series, "#3d85c6"),
    ]
    # Add tolerance band as mark-area on first series
    series[0]["markArea"] = {
        "silent":    True,
        "itemStyle": {"color": "rgba(145, 204, 117, 0.08)"},
        "data": [[{"yAxis": tol_low}, {"yAxis": tol_high}]],
    }
    series[0]["markLine"] = {
        "silent":    True,
        "symbol":    "none",
        "lineStyle": {"type": "dashed", "color": "#5470c6", "width": 1.5},
        "data":      [{"yAxis": NOMINAL_VOLTAGE, "label": {"formatter": "230V Nominal"}}],
    }

    return build_chart(
        "3-Phase Voltage Profile", [], dates, "V",
        series_list=series,
        legend_data=["VRN (R Phase)", "VYN (Y Phase)", "VBN (B Phase)"],
    )


def build_thermal_chart(oil_series, winding_series, hotspot_series, dates):
    """Dedicated thermal overlay: oil / winding / hotspot with danger zone."""
    def mk(name, data, color, width=2.5):
        return {
            "name":      name,
            "type":      "line",
            "data":      data,
            "smooth":    True,
            "symbol":    "none",
            "lineStyle": {"width": width, "color": color},
            "itemStyle": {"color": color},
            "areaStyle": {"color": color, "opacity": 0.08},
        }

    series = [
        mk("Oil Temperature",     oil_series,     "#5470c6"),
        mk("Winding Temperature", winding_series, "#f59e0b"),
        mk("Hotspot Temperature", hotspot_series, "#ee6666", 3),
    ]
    series[2]["markLine"] = {
        "silent":    True,
        "symbol":    "none",
        "data":      [{"yAxis": THERMAL_DANGER_TEMP,
                       "label": {"formatter": f"{THERMAL_DANGER_TEMP}°C Danger", "position": "insideEndTop"}}],
        "lineStyle": {"type": "dashed", "color": "#c0392b", "width": 2},
    }

    return {
        "title":       "Thermal Profile (Oil / Winding / Hotspot)",
        "code":        "thermal_profile",
        "type":        "chartCard",
        "info":        "Three-layer thermal overlay — hotspot above 85°C accelerates loss-of-life",
        "subtext":     "",
        "data": {
            "option": {
                "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
                "legend":  {"data": ["Oil Temperature", "Winding Temperature", "Hotspot Temperature"], "top": "2%"},
                "grid":    {"left": "3%", "right": "4%", "bottom": "12%", "containLabel": True},
                "xAxis":   {"type": "category", "data": dates, "axisLabel": {"rotate": 45}},
                "yAxis":   {"type": "value", "name": "°C"},
                "series":  series,
            }
        },
    }


def build_phase_share_chart(r_share, y_share, b_share, dates):
    """Stacked bar: phase load share (R/Y/B) — visualises imbalance."""
    def mk(name, data, color):
        return {
            "name":     name,
            "type":     "bar",
            "stack":    "phase",
            "data":     data,
            "itemStyle":{"color": color},
            "barWidth": "55%",
        }

    series = [
        mk("R Phase %", r_share, "#ee6666"),
        mk("Y Phase %", y_share, "#fac858"),
        mk("B Phase %", b_share, "#3d85c6"),
    ]

    return {
        "title":       "Phase Load Share (R / Y / B)",
        "code":        "phase_share",
        "type":        "chartCard",
        "info":        "Stacked phase power share — ideal is 33/33/33; deviation drives copper losses",
        "subtext":     "",
        "data": {
            "option": {
                "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
                "legend":  {"data": ["R Phase %", "Y Phase %", "B Phase %"], "top": "2%"},
                "grid":    {"left": "3%", "right": "4%", "bottom": "12%", "containLabel": True},
                "xAxis":   {"type": "category", "data": dates, "axisLabel": {"rotate": 45}},
                "yAxis":   {"type": "value", "name": "% Share"},
                "series":  series,
            }
        },
    }


def build_gauge_chart(title, code, value, min_val=0, max_val=100,
                      thresholds=None, unit="", description=""):
    """Gauge chart — used for overload-risk-score, PQ index, avg LF summary."""
    thresholds = thresholds or [
        [0.3,  "#91cc75"],
        [0.7,  "#fac858"],
        [1.0,  "#ee6666"],
    ]
    pct = (value - min_val) / max(max_val - min_val, 1e-9)
    return {
        "title":       title,
        "code":        code,
        "type":        "gaugeCard",
        "info":        description,
        "subtext":     "",
        "data": {
            "option": {
                "series": [{
                    "type":     "gauge",
                    "min":      min_val,
                    "max":      max_val,
                    "progress": {"show": True, "width": 14},
                    "axisLine": {"lineStyle": {"width": 14, "color": thresholds}},
                    "pointer":  {"width": 5, "length": "70%"},
                    "detail":   {"valueAnimation": True, "formatter": f"{{value}} {unit}", "fontSize": 22},
                    "data":     [{"value": round(value, 3), "name": title}],
                }]
            }
        },
    }


# ═════════════════════════════════════════════════════════════════════════════
# MAIN ENDPOINT: /insigh-test  — Full 15-model dashboard response
# ═════════════════════════════════════════════════════════════════════════════

@router.post("/ai-insight")
def get_all_transformer_insights(req: ForecastRequest):
    """
    Returns the full transformer dashboard payload:
        { data: {stats, charts, tables}, message, status }

    Drives everything on the Analytics / Performance / Insights / AI-Insights tabs.
    """
    office_id   = int(req.office)
    from_date   = req.fromDate
    to_date     = req.toDate
    section_ids = resolve_to_sections(state.HIERARCHY_TREE, office_id)
    office_filter = {"$in": [int(s) for s in section_ids]}

    # ── Parallel fetch all 15 model aggregates + raw collections ─────────────
    fetch_tasks = {
        "dt_count":       lambda: _count_transformers(req),
        "load":           lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["load"],           from_date, to_date),
        "peak":           lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["peak"],           from_date, to_date),
        "pf":             lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["pf"],             from_date, to_date),
        "voltage":        lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["voltage"],        from_date, to_date),
        "imbalance":      lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["imbalance"],      from_date, to_date),
        "pq":             lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["pq"],             from_date, to_date),
        "overload":       lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["overload"],       from_date, to_date),
        "ntl":            lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["ntl"],            from_date, to_date),
        "lol":            lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["lol"],            from_date, to_date),
        "load_factor":    lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["load_factor"],    from_date, to_date),
        "thermal":        lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["thermal"],        from_date, to_date),
        "volt_sag":       lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["volt_sag"],       from_date, to_date),
        "overload_score": lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["overload_score"], from_date, to_date),
        "freq_drift":     lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["freq_drift"],     from_date, to_date),
        "neutral":        lambda: _fetch_agg(_db, office_filter, MODEL_KEY_MAP["neutral"],        from_date, to_date),
        "ntl_raw":        lambda: _fetch_raw(_db, COL_NTL,      office_filter, from_date, to_date, "confidence"),
        "thermal_raw":    lambda: _fetch_raw(_db, COL_THERMAL,  office_filter, from_date, to_date, "confidence"),
        "lol_raw":        lambda: _fetch_raw(_db, COL_LOL,      office_filter, from_date, to_date, "value"),
        "overload_raw":   lambda: _fetch_raw(_db, COL_OVERLOAD, office_filter, from_date, to_date, "confidence"),
        "neutral_raw":    lambda: _fetch_raw(_db, COL_NEUTRAL,  office_filter, from_date, to_date, "confidence"),
    }

    results = {}
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = {pool.submit(fn): key for key, fn in fetch_tasks.items()}
        for fut in as_completed(futures):
            results[futures[fut]] = fut.result()

    total_dts = results["dt_count"]

    if not results["load"]:
        raise HTTPException(
            status_code=503,
            detail=(f"No cached transformer forecast for office={office_id} "
                    f"({from_date} → {to_date}). Run precompute first."),
        )

    # ── Extract all series ───────────────────────────────────────────────────
    dates = _extract_dates(results["load"])

    s_load      = _build_series(results["load"],           dates, "sum")
    s_peak      = _build_series(results["peak"],           dates, "max")
    s_pf        = _build_series(results["pf"],             dates, "mean")
    s_voltage   = _build_series(results["voltage"],        dates, "mean")
    s_imb       = _build_series(results["imbalance"],      dates, "max")
    s_pq        = _build_series(results["pq"],             dates, "mean")
    s_overload  = _build_series(results["overload"],       dates, "sum")
    s_ntl       = _build_series(results["ntl"],            dates, "sum")
    s_lol       = _build_series(results["lol"],            dates, "max")
    s_lf        = _build_series(results["load_factor"],    dates, "mean")
    s_thermal   = _build_series(results["thermal"],        dates, "sum")
    s_voltsag   = _build_series(results["volt_sag"],       dates, "max")
    s_oloadscr  = _build_series(results["overload_score"], dates, "mean")
    s_freq      = _build_series(results["freq_drift"],     dates, "sum")
    s_neutral   = _build_series(results["neutral"],        dates, "sum")

    # ── Anchor-based live summary stats (from ANCHOR_INDEX DataFrame) ────────
    df = resolve_transformers(req)

    def _safe(fn, default=0.0):
        try:
            v = fn()
            if pd.isna(v):
                return default
            return v
        except Exception:
            return default

    total_capacity_kva = float(_safe(lambda: df["transformer_capacity_kva"].sum()))
    avg_load_pct       = float(_safe(lambda: df["load_percentage"].mean()))
    avg_pf             = float(_safe(lambda: df["pf_value"].mean(), 1.0))
    avg_oil_temp       = float(_safe(lambda: df["oil_temperature"].mean()))
    avg_winding_temp   = float(_safe(lambda: df["winding_temperature"].mean()))
    avg_hotspot        = float(_safe(lambda: df["hotspot_temperature"].mean()))
    max_hotspot        = float(_safe(lambda: df["hotspot_temperature"].max()))
    avg_lol            = float(_safe(lambda: df["loss_of_life_pct"].mean()))
    max_lol            = float(_safe(lambda: df["loss_of_life_pct"].max()))
    avg_pq             = float(_safe(lambda: df["overall_pq_index"].mean()))
    avg_load_factor    = float(_safe(lambda: df["load_factor"].mean()))
    pf_penalty_dts     = int(_safe(lambda: (df["pf_value"] < PF_PENALTY_THRESHOLD).sum()))
    overload_dts       = int(_safe(lambda: (df["load_percentage"] > OVERLOAD_THRESH_PCT).sum()))
    thermal_danger_dts = int(_safe(lambda: (df["hotspot_temperature"] > THERMAL_DANGER_TEMP).sum()))
    ntl_events_live    = int(_safe(lambda: df["ntl_event"].sum()))
    volt_imb_avg       = float(_safe(lambda: df["voltage_unbalance_pct"].mean()))
    curr_imb_avg       = float(_safe(lambda: df["current_unbalance_pct"].mean()))

    total_kwh       = round(sum(s_load), 2)
    total_peak_kw   = round(max(s_peak) if s_peak else 0, 2)
    peak_day        = dates[int(np.argmax(s_load))] if s_load else None
    peak_kw_day     = dates[int(np.argmax(s_peak))] if s_peak else None

    # ROI headline numbers (preview — full breakdown is in /roi endpoint)
    est_energy_bill   = round(total_kwh * TARIFF_PER_KWH, 0)
    est_pf_penalty    = round(est_energy_bill * (PF_PENALTY_PCT / 100) * (pf_penalty_dts / max(total_dts, 1)), 0)
    est_mdi_penalty   = round(max(total_peak_kw - total_capacity_kva * 0.85, 0) * MDI_PENALTY_PER_KVA, 0)
    est_monthly_save  = round(est_pf_penalty + est_mdi_penalty, 0)

    # Build 3-phase voltage series from anchor (approximated from avg)
    # (In production the precompute script should store vrn/vyn/vbn per-date.)
    vrn = [round(v * 0.995, 2) for v in s_voltage]
    vyn = [round(v * 1.000, 2) for v in s_voltage]
    vbn = [round(v * 1.005, 2) for v in s_voltage]

    # Phase shares (from anchor average — for the stacked chart)
    r_share = float(_safe(lambda: df["phase_r_share"].mean(), 0.333)) * 100
    y_share = float(_safe(lambda: df["phase_y_share"].mean(), 0.333)) * 100
    b_share = float(_safe(lambda: df["phase_b_share"].mean(), 0.333)) * 100
    # Broadcast to match date length (static share overlay)
    rs = [round(r_share, 2)] * len(dates)
    ys = [round(y_share, 2)] * len(dates)
    bs = [round(b_share, 2)] * len(dates)

    # Thermal series from anchor — overlay oil/winding/hotspot (smoothed, representative)
    oil_series     = [round(avg_oil_temp,     1)] * len(dates)
    winding_series = [round(avg_winding_temp, 1)] * len(dates)
    hotspot_series = [round(avg_hotspot,      1)] * len(dates)

    # ── CHARTS ───────────────────────────────────────────────────────────────
    charts = [
        build_chart("Energy Load Forecast",  s_load,  dates, "kWh",
                    meta={"subtext": f"Total: {total_kwh:,.0f} kWh | Peak: {peak_day} | DTs: {total_dts}"}),

        build_chart("Peak Demand vs MDI Threshold", s_peak, dates, "kW",
                    meta={"subtext": f"Max: {total_peak_kw:,.2f} kW | Day: {peak_kw_day}"}),

        build_chart("Power Factor Quality", s_pf, dates, "PF"),

        build_3phase_voltage_chart(vrn, vyn, vbn, dates),

        build_chart("Phase Imbalance Trend", s_imb, dates, "%"),

        build_phase_share_chart(rs, ys, bs, dates),

        build_chart("Power Quality Index", s_pq, dates, "Index"),

        build_thermal_chart(oil_series, winding_series, hotspot_series, dates),

        build_chart("Thermal Anomaly Trend", s_thermal, dates, "Alerts"),

        build_chart("Loss of Life Accumulation", s_lol, dates, "%"),

        build_chart("Load Factor (Right-Sizing)", s_lf, dates, "Factor"),

        build_chart("Overload Risk", s_overload, dates, "Count"),

        build_chart("Overload Risk Score (Hours-Ahead)", s_oloadscr, dates, "Score"),

        build_chart("Voltage Sag/Swell Prediction", s_voltsag, dates, "V"),

        build_chart("Frequency Drift Alerts", s_freq, dates, "Events"),

        build_chart("Neutral Current Anomaly", s_neutral, dates, "Alerts"),

        build_chart("NTL / Theft Detection", s_ntl, dates, "Alerts"),

        build_gauge_chart("Overall Power Quality Gauge", "pq_gauge", avg_pq,
                          0, 1,
                          thresholds=[[0.5, "#ee6666"], [0.85, "#fac858"], [1.0, "#91cc75"]],
                          description="Fleet avg PQ index — target > 0.85"),
    ]

    # ── STATS (Response-2 compliant: label, value, icon, tooltip, threshold_status) ──
    avg_pf_rounded         = round(avg_pf, 4)
    avg_pq_rounded         = round(avg_pq, 3)
    avg_load_pct_rounded   = round(avg_load_pct, 2)
    max_hotspot_rounded    = round(max_hotspot, 1)
    avg_lol_rounded        = round(avg_lol, 2)
    max_lol_rounded        = round(max_lol, 2)
    volt_imb_rounded       = round(volt_imb_avg, 2)
    curr_imb_rounded       = round(curr_imb_avg, 2)

    stats = [
        make_stat(
            "Total Transformers", fmt(total_dts, "transformers"), "ri-building-line",
            tooltip="Total number of distribution transformers registered under this office and its sub-sections.",
        ),
        make_stat(
            "Fleet Capacity", fmt(round(total_capacity_kva, 0), "kVA"), "ri-flashlight-line",
            tooltip="Combined nameplate capacity of all reporting transformers in the fleet.",
        ),
        make_stat(
            "Total Energy Import", fmt(total_kwh, "kWh"), "ri-download-cloud-2-line",
            tooltip="Cumulative energy drawn from the grid across all reporting transformers for the selected date range.",
        ),
        make_stat(
            "Peak Demand", fmt(total_peak_kw, "kW"), "ri-bar-chart-line",
            tooltip="Highest simultaneous real-power demand recorded across all transformers on any single day in the selected range.",
        ),
        make_stat(
            "Avg Load %", fmt(avg_load_pct_rounded, "%"), "ri-dashboard-line",
            threshold_status=build_threshold(
                avg_load_pct_rounded, warn=70, crit=OVERLOAD_THRESH_PCT,
                direction="higher_is_worse",
            ),
            tooltip="Average ratio of actual load to sanctioned capacity across transformers. High values signal infrastructure stress and potential overload risk.",
        ),
        make_stat(
            "Avg Load Factor", fmt(round(avg_load_factor, 3), ""), "ri-scales-3-line",
            threshold_status=build_threshold(
                round(avg_load_factor, 3), warn=0.30, crit=0.15,
                direction="lower_is_worse",
            ),
            tooltip="Fleet-wide average load factor (avg kW ÷ peak kW). Low values indicate oversized transformers wasting iron losses.",
        ),
        make_stat(
            "Avg Power Factor", fmt(avg_pf_rounded, ""), "ri-pulse-line",
            threshold_status=build_threshold(
                avg_pf_rounded, warn=PF_PENALTY_THRESHOLD, crit=0.85,
                direction="lower_is_worse",
            ),
            tooltip="Fleet-wide average power factor. Values below 0.90 indicate excessive reactive power draw, leading to grid inefficiency and possible utility penalties.",
        ),
        make_stat(
            "PF Penalty DTs", fmt(pf_penalty_dts, "transformers"), "ri-error-warning-line",
            tooltip="Number of transformers whose power factor has dropped below the minimum threshold, making them liable for reactive energy surcharges.",
        ),
        make_stat(
            "Avg PQ Index", fmt(avg_pq_rounded, ""), "ri-sound-module-line",
            threshold_status=build_threshold(
                avg_pq_rounded, warn=0.85, crit=0.70,
                direction="lower_is_worse",
            ),
            tooltip="Composite power-quality index combining voltage, current, PF, and frequency. Target above 0.85.",
        ),
        make_stat(
            "Avg Oil Temp", fmt(round(avg_oil_temp, 1), "°C"), "ri-temp-cold-line",
            tooltip="Average top-oil temperature across the fleet. Used for thermal stress and loss-of-life calculations.",
        ),
        make_stat(
            "Avg Winding Temp", fmt(round(avg_winding_temp, 1), "°C"), "ri-temp-hot-line",
            tooltip="Average winding temperature — an earlier indicator of thermal stress than oil temperature.",
        ),
        make_stat(
            "Avg Hotspot Temp", fmt(round(avg_hotspot, 1), "°C"), "ri-fire-line",
            tooltip="Average hotspot (worst-spot) temperature. Sustained readings above 85 °C accelerate insulation aging per IEEE C57.91.",
        ),
        make_stat(
            "Max Hotspot Temp", fmt(max_hotspot_rounded, "°C"), "ri-alarm-warning-fill",
            threshold_status=build_threshold(
                max_hotspot_rounded, warn=75, crit=THERMAL_DANGER_TEMP,
                direction="higher_is_worse",
            ),
            tooltip="Highest hotspot temperature observed on any transformer. Breaches of 85 °C trigger thermal-danger alerts.",
        ),
        make_stat(
            "Thermal Danger DTs", fmt(thermal_danger_dts, "transformers"), "ri-alert-line",
            tooltip="Number of transformers breaching the 85 °C hotspot safety threshold.",
        ),
        make_stat(
            "Avg Loss-of-Life", fmt(avg_lol_rounded, "%"), "ri-heart-pulse-line",
            threshold_status=build_threshold(
                avg_lol_rounded, warn=40, crit=60,
                direction="higher_is_worse",
            ),
            tooltip="Fleet average cumulative loss-of-life percentage (IEEE C57.91 aging acceleration).",
        ),
        make_stat(
            "Worst LoL DT", fmt(max_lol_rounded, "%"), "ri-skull-2-line",
            threshold_status=build_threshold(
                max_lol_rounded, warn=60, crit=LOL_CRITICAL_PCT,
                direction="higher_is_worse",
            ),
            tooltip="Highest loss-of-life reading in the fleet — candidate for urgent replacement planning.",
        ),
        make_stat(
            "Overload DTs", fmt(overload_dts, "transformers"), "ri-alarm-warning-line",
            tooltip="Number of transformers that breached the critical loading threshold (>80%), indicating feeder overload conditions.",
        ),
        make_stat(
            "NTL / Theft Alerts", fmt(ntl_events_live, "alerts"), "ri-spy-line",
            tooltip="Transformers flagged by the NTL detection model for suspected non-technical loss or energy theft, based on consumption pattern anomalies above the confidence threshold.",
        ),
        make_stat(
            "Voltage Imbalance", fmt(volt_imb_rounded, "%"), "ri-equalizer-line",
            threshold_status=build_threshold(
                volt_imb_rounded, warn=5.0, crit=10.0,
                direction="higher_is_worse",
            ),
            tooltip="Average percentage difference in voltage across the three phases. Persistent imbalance causes motor heating, equipment stress, and increased losses.",
        ),
        make_stat(
            "Current Imbalance", fmt(curr_imb_rounded, "%"), "ri-equalizer-fill",
            threshold_status=build_threshold(
                curr_imb_rounded, warn=10.0, crit=25.0,
                direction="higher_is_worse",
            ),
            tooltip="Average percentage difference in current across the three phases. High imbalance indicates uneven load distribution and may lead to neutral conductor overheating.",
        ),
        make_stat(
            "Est. Energy Bill", fmt(int(est_energy_bill), "₹"), "ri-money-rupee-circle-line",
            tooltip="Estimated energy cost for the period using the blended APDCL industrial-commercial tariff.",
        ),
        make_stat(
            "Est. Monthly Savings", fmt(int(est_monthly_save), "₹"), "ri-money-dollar-circle-line",
            tooltip="Combined monthly savings achievable from PF correction and MDI penalty avoidance (preview — see /roi endpoint for full breakdown).",
        ),
        make_stat(
            "MDI Penalty Risk", fmt(int(est_mdi_penalty), "₹"), "ri-arrow-up-circle-line",
            tooltip="Estimated Maximum Demand Indicator penalty exposure if peak demand exceeds the sanctioned contract capacity.",
        ),
    ]

    # ── TABLES ───────────────────────────────────────────────────────────────
    NTL_CONF_THRESH = 50.0

    def _build_high_risk_dts(page: int, page_size: int) -> dict:
        """Top transformers by combined risk score (overload + thermal + LoL)."""
        rows = []
        if not df.empty:
            dfc = df.copy()
            dfc["risk_score"] = (
                dfc.get("overload_risk",      pd.Series(0)).fillna(0) * 0.35 +
                dfc.get("thermal_stress_index", pd.Series(0)).fillna(0) * 0.30 +
                dfc.get("loss_of_life_pct",   pd.Series(0)).fillna(0) / 100 * 0.25 +
                (1 - dfc.get("health_score",  pd.Series(1)).fillna(1)) * 0.10
            )
            dfc = dfc.sort_values("risk_score", ascending=False)
            for _, r in dfc.iterrows():
                rows.append({
                    "_id":           f"RISK-{r['assetUid']}",
                    "assetUid":      str(r["assetUid"]),
                    "assetName":     f"DT-{r['assetUid']}",
                    "capacity_kva":  round(float(r.get("transformer_capacity_kva", 0)), 1),
                    "load_pct":      round(float(r.get("load_percentage", 0)), 1),
                    "hotspot_c":     round(float(r.get("hotspot_temperature", 0)), 1),
                    "lol_pct":       round(float(r.get("loss_of_life_pct", 0)), 2),
                    "pf":            round(float(r.get("pf_value", 1)), 3),
                    "risk_score":    round(float(r["risk_score"]), 3),
                })
        p = paginate(rows, page, page_size)
        return {
            "tableId":    "high_risk_dts",
            "title":      "Top-Risk Transformers",
            "data":       p["data"],
            "pagination": p["pagination"],
        }

    def _build_ntl_alerts(page: int, page_size: int) -> dict:
        rows = []
        for r in results["ntl_raw"]:
            conf = float(r.get("confidence", 0))
            if conf >= NTL_CONF_THRESH:
                rows.append({
                    "_id":        f"NTL-{r['assetUid']}-{r.get('date')}",
                    "assetUid":   str(r["assetUid"]),
                    "assetName":  f"DT-{r['assetUid']}",
                    "date":       r.get("date"),
                    "timestamp":  r.get("timestamp"),
                    "status":     "ALERT",
                    "confidence": f"{round(conf, 1)}%",
                })
        p = paginate(rows, page, page_size)
        return {
            "tableId":    "ntl_alerts",
            "title":      "NTL / Theft Alerts",
            "data":       p["data"],
            "pagination": p["pagination"],
        }

    def _build_thermal_alerts(page: int, page_size: int) -> dict:
        rows = []
        for r in results["thermal_raw"]:
            conf = float(r.get("confidence", 0))
            rows.append({
                "_id":        f"THERM-{r['assetUid']}-{r.get('date')}",
                "assetUid":   str(r["assetUid"]),
                "assetName":  f"DT-{r['assetUid']}",
                "date":       r.get("date"),
                "hotspot_c":  round(float(r.get("hotspot_temperature", 0)), 1),
                "gradient_c": round(float(r.get("thermal_gradient", 0)), 1),
                "status":     "THERMAL ANOMALY",
                "confidence": f"{round(conf, 1)}%",
            })
        p = paginate(rows, page, page_size)
        return {
            "tableId":    "thermal_alerts",
            "title":      "Thermal Anomaly Alerts",
            "data":       p["data"],
            "pagination": p["pagination"],
        }

    def _build_lol_top10(page: int, page_size: int) -> dict:
        """Transformers nearing end-of-life — prioritise replacement planning."""
        rows = []
        if not df.empty and "loss_of_life_pct" in df.columns:
            dfc = df.sort_values("loss_of_life_pct", ascending=False).head(100)
            for _, r in dfc.iterrows():
                lol = float(r.get("loss_of_life_pct", 0))
                rows.append({
                    "_id":           f"LOL-{r['assetUid']}",
                    "assetUid":      str(r["assetUid"]),
                    "assetName":     f"DT-{r['assetUid']}",
                    "capacity_kva":  round(float(r.get("transformer_capacity_kva", 0)), 1),
                    "lol_pct":       round(lol, 2),
                    "remaining_pct": round(100 - lol, 2),
                    "aging_index":   round(float(r.get("aging_index", 0)), 3),
                    "priority":      "URGENT" if lol >= LOL_CRITICAL_PCT else
                                     "HIGH"   if lol >= 60 else
                                     "MEDIUM" if lol >= 40 else "LOW",
                })
        p = paginate(rows, page, page_size)
        return {
            "tableId":    "lol_replacement",
            "title":      "Loss-of-Life Replacement Priority",
            "data":       p["data"],
            "pagination": p["pagination"],
        }

    def _build_right_size_candidates(page: int, page_size: int) -> dict:
        """DTs with load factor < 0.30 → right-sizing candidates (iron-loss saving)."""
        rows = []
        if not df.empty and "load_factor" in df.columns:
            dfc = df[df["load_factor"].fillna(1) < 0.30].sort_values("load_factor")
            for _, r in dfc.iterrows():
                cap = float(r.get("transformer_capacity_kva", 0))
                lf  = float(r.get("load_factor", 0))
                rec_cap = round(cap * max(lf * 1.5, 0.3))   # suggested new size
                rows.append({
                    "_id":              f"RS-{r['assetUid']}",
                    "assetUid":         str(r["assetUid"]),
                    "assetName":        f"DT-{r['assetUid']}",
                    "current_cap_kva":  round(cap, 0),
                    "load_factor":      round(lf, 3),
                    "avg_load_pct":     round(float(r.get("load_percentage", 0)), 1),
                    "recommended_kva":  rec_cap,
                    "est_saving_rs":    int(max(cap - rec_cap, 0) * 450),  # ₹450/kVA/yr iron-loss saving
                })
        p = paginate(rows, page, page_size)
        return {
            "tableId":    "right_size",
            "title":      "Right-Sizing Candidates (Low Load Factor)",
            "data":       p["data"],
            "pagination": p["pagination"],
        }

    TABLE_BUILDERS = {
        "high_risk_dts":   _build_high_risk_dts,
        "ntl_alerts":      _build_ntl_alerts,
        "thermal_alerts":  _build_thermal_alerts,
        "lol_replacement": _build_lol_top10,
        "right_size":      _build_right_size_candidates,
    }

    requested_tables = req.tables or [
        TableRequest(tableId="high_risk_dts",   page=1, pageSize=10),
        TableRequest(tableId="ntl_alerts",      page=1, pageSize=10),
        TableRequest(tableId="thermal_alerts",  page=1, pageSize=10),
        TableRequest(tableId="lol_replacement", page=1, pageSize=10),
        TableRequest(tableId="right_size",      page=1, pageSize=10),
    ]

    tables_out = []
    for t in requested_tables:
        builder = TABLE_BUILDERS.get(t.tableId)
        if builder is None:
            continue
        tables_out.append(builder(t.page, t.pageSize))

    return {
        "data": {
            "stats":  stats,
            "charts": charts,
            "tables": tables_out,
            "forecast_meta": build_forecast_meta(
                from_date=req.fromDate,
                to_date=req.toDate,
                models={
                    "load":             {"type": "regression",     "unit": "kWh",    "aggregation": "sum"},
                    "peak":             {"type": "regression",     "unit": "kW",     "aggregation": "max"},
                    "pf":               {"type": "regression",     "unit": "ratio",  "aggregation": "mean"},
                    "voltage":          {"type": "regression",     "unit": "V",      "aggregation": "mean"},
                    "imbalance":        {"type": "regression",     "unit": "%",      "aggregation": "max"},
                    "pq":               {"type": "regression",     "unit": "index",  "aggregation": "mean"},
                    "overload":         {"type": "classification", "unit": "events", "aggregation": "sum"},
                    "ntl":              {"type": "classification", "unit": "alerts", "aggregation": "sum"},
                    "lol":              {"type": "regression",     "unit": "%",      "aggregation": "max"},
                    "load_factor":      {"type": "regression",     "unit": "ratio",  "aggregation": "mean"},
                    "thermal":          {"type": "classification", "unit": "alerts", "aggregation": "sum"},
                    "volt_sag":         {"type": "regression",     "unit": "V",      "aggregation": "max"},
                    "overload_score":   {"type": "regression",     "unit": "score",  "aggregation": "mean"},
                    "freq_drift":       {"type": "classification", "unit": "events", "aggregation": "sum"},
                    "neutral":          {"type": "classification", "unit": "alerts", "aggregation": "sum"},
                },
                training_period="2026-01-01 to 2026-04-03",
            ),
        },
        "message": "success",
        "status":  True,
    }


# ═════════════════════════════════════════════════════════════════════════════
# DRILLDOWN: /insigh-test-drill
# ═════════════════════════════════════════════════════════════════════════════

@router.post("/ai-insight-drill")
def get_insight_drilldown(req: DrilldownRequest):
    """
    Per-transformer drilldown for any of the 15 model codes on a specific date.
    Mirrors smart-meter /insigh-test-drill contract.
    """
    section_ids   = resolve_to_sections(state.HIERARCHY_TREE, int(req.office))
    office_filter = {"$in": [int(s) for s in section_ids]}
    meta          = MODEL_META[req.code]
    model_key     = MODEL_KEY_MAP[req.code]

    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]
    rows   = []

    # ── Per-DT drilldown for NTL, thermal, overload, neutral, freq, LoL ──
    raw_collections = {
        "ntl":      COL_NTL,
        "thermal":  COL_THERMAL,
        "overload": COL_OVERLOAD,
        "neutral":  COL_NEUTRAL,
        "freq_drift": COL_FREQ,
        "lol":      COL_LOL,
        "load":     COL_ENERGY,
    }

    if req.code in raw_collections:
        coll = raw_collections[req.code]
        raw  = list(
            db[coll].find(
                {"office_id": office_filter, "date": req.date},
                {"_id": 0},
            ).sort("confidence" if req.code != "load" else "value", -1)
        )
        for r in raw:
            # Null-safe confidence parsing: handle missing key AND explicit null values
            _raw_conf = r.get("confidence")
            conf = float(_raw_conf) if _raw_conf is not None else None
            # Null-safe value parsing: handle missing key AND explicit null values
            _raw_val = r.get("value")
            val = float(_raw_val) if _raw_val is not None else 0.0
            rows.append({
                "assetUid":    str(r.get("assetUid", "")),
                "assetName":   f"DT-{r.get('assetUid', '')}",
                "date":        req.date,
                "timestamp":   r.get("timestamp"),
                "value":       round(val, 4),
                meta["label"]: round(val, 4),
                "status":      "ALERT" if conf is not None and conf >= 50 else "Normal",
                "confidence":  f"{round(conf, 1)}%" if conf is not None else None,
            })
        total = (sum(1 for r in rows if r.get("status") == "ALERT")
                 if req.code in ("ntl", "thermal", "overload", "neutral", "freq_drift")
                 else round(sum(r.get("value", 0) for r in rows), 2))
    else:
        # Aggregated-only: peak | pf | voltage | imbalance | pq | overload_score |
        # volt_sag | load_factor — we don't keep per-DT raw, so fetch aggregated row.
        agg = db[COL_AGG].find_one(
            {
                "office_id": office_filter,
                "model_key": model_key,
                "date":      req.date,
            },
            {"_id": 0, "value": 1, "day_name": 1},
        )
        val = round(float(agg["value"]), 4) if agg else 0
        rows.append({
            "date":         req.date,
            "day_name":     agg.get("day_name", "") if agg else "",
            "scope":        "All transformers (aggregated)",
            meta["label"]:  val,
        })
        total = val

    client.close()
    p = paginate(rows, req.page, req.pageSize)

    _unit = meta["unit"]
    if _unit == "":
        _total_display = total
    elif _unit in ("₹", "$", "€", "£"):
        _total_display = f"{_unit}{total}"
    else:
        _total_display = f"{total} {_unit}"

    return {
        "data": {
            "date":   req.date,
            "office": req.office,
            "code":   req.code,
            "summary": {
                "total": _total_display,
                "label": meta["label"],
                "note":  ("per-transformer"
                          if req.code in raw_collections
                          else "aggregated only — no per-DT raw stored"),
            },
            "table": {
                "tableId":    "drilldown_transformers",
                "title":      f"{req.code.upper()} breakdown — {req.date}",
                "data":       p["data"],
                "pagination": p["pagination"],
            },
        },
        "message": "success",
        "status":  True,
    }


#try with thermalhelath

# @router.post("/roi-savings-drill")
# def get_insight_drilldown_1(req: DrilldownRequest):
#     """
#     Per-transformer drilldown for any of the 15 model codes on a specific date.
#     Mirrors smart-meter /insigh-test-drill contract.
#     """
#     section_ids   = resolve_to_sections(state.HIERARCHY_TREE, int(req.office))
#     office_filter = {"$in": [int(s) for s in section_ids]}
#     meta          = MODEL_META[req.code]
#     model_key     = MODEL_KEY_MAP[req.code]

#     client = MongoClient(MONGO_URI)
#     db     = client[DB_NAME]
#     rows   = []

#     # ── Per-DT drilldown for NTL, thermal, overload, neutral, freq, LoL ──
#     raw_collections = {
#         "ntl":      COL_NTL,
#         "thermal":  COL_THERMAL,
#         "overload": COL_OVERLOAD,
#         "neutral":  COL_NEUTRAL,
#         "freq_drift": COL_FREQ,
#         "lol":      COL_LOL,
#         "load":     COL_ENERGY,
#     }

#     if req.code in raw_collections:
#         coll = raw_collections[req.code]
#         raw  = list(
#             db[coll].find(
#                 {"office_id": office_filter, "date": req.date},
#                 {"_id": 0},
#             ).sort("confidence" if req.code != "load" else "value", -1)
#         )
#         for r in raw:
#             # Null-safe confidence parsing: handle missing key AND explicit null values
#             _raw_conf = r.get("confidence")
#             conf = float(_raw_conf) if _raw_conf is not None else None
#             # Null-safe value parsing: handle missing key AND explicit null values
#             _raw_val = r.get("value")
#             val = float(_raw_val) if _raw_val is not None else 0.0
#             rows.append({
#                 "assetUid":    str(r.get("assetUid", "")),
#                 "assetName":   f"DT-{r.get('assetUid', '')}",
#                 "date":        req.date,
#                 "timestamp":   r.get("timestamp"),
#                 "value":       round(val, 4),
#                 meta["label"]: round(val, 4),
#                 "status":      "ALERT" if conf is not None and conf >= 50 else "Normal",
#                 "confidence":  f"{round(conf, 1)}%" if conf is not None else None,
#             })
#         total = (sum(1 for r in rows if r.get("status") == "ALERT")
#                  if req.code in ("ntl", "thermal", "overload", "neutral", "freq_drift")
#                  else round(sum(r.get("value", 0) for r in rows), 2))
#     else:
#         # Aggregated-only: peak | pf | voltage | imbalance | pq | overload_score |
#         # volt_sag | load_factor — we don't keep per-DT raw, so fetch aggregated row.
#         agg = db[COL_AGG].find_one(
#             {
#                 "office_id": office_filter,
#                 "model_key": model_key,
#                 "date":      req.date,
#             },
#             {"_id": 0, "value": 1, "day_name": 1},
#         )
#         val = round(float(agg["value"]), 4) if agg else 0
#         rows.append({
#             "date":         req.date,
#             "day_name":     agg.get("day_name", "") if agg else "",
#             "scope":        "All transformers (aggregated)",
#             meta["label"]:  val,
#         })
#         total = val

#     client.close()
#     p = paginate(rows, req.page, req.pageSize)

#     _unit = meta["unit"]
#     if _unit == "":
#         _total_display = total
#     elif _unit in ("₹", "$", "€", "£"):
#         _total_display = f"{_unit}{total}"
#     else:
#         _total_display = f"{total} {_unit}"

#     return {
#         "data": {
#             "date":   req.date,
#             "office": req.office,
#             "code":   req.code,
#             "summary": {
#                 "total": _total_display,
#                 "label": meta["label"],
#                 "note":  ("per-transformer"
#                           if req.code in raw_collections
#                           else "aggregated only — no per-DT raw stored"),
#             },
#             "table": {
#                 "tableId":    "drilldown_transformers",
#                 "title":      f"{req.code.upper()} breakdown — {req.date}",
#                 "data":       p["data"],
#                 "pagination": p["pagination"],
#             },
#         },
#         "message": "success",
#         "status":  True,
#     }


