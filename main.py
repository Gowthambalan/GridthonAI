# ─────────────────────────────────────────────────────────────────────────────
# main.py — APDCL Transformer Analytics API (Distribution Transformer Dashboard)
# ─────────────────────────────────────────────────────────────────────────────
#
# 15 GLOBAL XGBoost MODELS LOADED ON STARTUP
#   Forecasting (6)  : load, peak demand, reactive/PF, voltage, phase imbalance, PQ index
#   Anomaly/Risk (2) : overload, NTL/theft
#   Cost Reduction(2): loss-of-life, load factor/right-sizing
#   Rectification(5) : thermal anomaly, volt sag/swell, overload risk score,
#                      frequency drift, neutral anomaly
#
#   Response shape: { data: {stats, charts, tables}, message, status }
# ─────────────────────────────────────────────────────────────────────────────

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator, model_validator, Field
from typing import Optional
from datetime import datetime

import pickle
import os
import pandas as pd

import state
from state import MODELS, load_hierarchy, build_tree

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
MAX_DATE_RANGE = 90
MODEL_DIR      = "models"

# All 15 transformer models (matching pkl filenames from train.py)
MODEL_PATHS = [
    # Forecasting
    "dt_load_forecast_model.pkl",
    "dt_peak_demand_model.pkl",
    "dt_reactive_pf_model.pkl",
    "dt_voltage_profile_model.pkl",
    "dt_phase_imbalance_model.pkl",
    "dt_pq_index_model.pkl",
    # Anomaly / Risk
    "dt_overload_risk_model.pkl",
    "dt_ntl_theft_model.pkl",
    # Cost Reduction
    "dt_loss_of_life_model.pkl",
    "dt_load_factor_model.pkl",
    # Rectification / Anomaly detection
    "dt_thermal_anomaly_model.pkl",
    "dt_volt_sag_swell_model.pkl",
    "dt_overload_risk_score_model.pkl",
    "dt_freq_drift_model.pkl",
    "dt_neutral_anomaly_model.pkl",
]


# ─────────────────────────────────────────────────────────────────────────────
# REQUEST MODEL (shared)
# ─────────────────────────────────────────────────────────────────────────────
class ForecastRequest(BaseModel):
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


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="APDCL Transformer Analytics API",
    version="1.0.0",
    description="Distribution Transformer (DT) ROI + Anomaly + Health Dashboard"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# MODEL LOADING
# ─────────────────────────────────────────────────────────────────────────────
def load_models():
    for fname in MODEL_PATHS:
        path = os.path.join(MODEL_DIR, fname)
        if os.path.exists(path):
            with open(path, "rb") as f:
                state.MODELS[fname] = pickle.load(f)
            print(f"✅ Loaded: {fname}")
        else:
            print(f"❌ Missing: {path}")


def load_anchor_index():
    """
    Build anchor index (latest known row per transformer) from any loaded model.
    We use load_forecast_model because it has the richest last_known buffer.
    """
    key = "dt_load_forecast_model.pkl"

    if key not in state.MODELS:
        print("❌ Load forecast model not loaded → anchor empty")
        state.ANCHOR_INDEX = pd.DataFrame()
        return

    lk = state.MODELS[key].get("last_known")
    if lk is None or len(lk) == 0:
        print("❌ last_known missing in model")
        state.ANCHOR_INDEX = pd.DataFrame()
        return

    df = lk.copy()

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Build anchor (latest record per transformer — keyed by assetUid)
    anchor = (
        df.sort_values("timestamp")
          .groupby("assetUid", as_index=False)
          .last()
    )

    state.ANCHOR_INDEX = anchor

    print("✅ Anchor loaded:", anchor.shape)
    print("Columns:", list(anchor.columns)[:20], "...")


def init_hierarchy():
    hierarchy = load_hierarchy()
    state.HIERARCHY_TREE = build_tree(hierarchy)
    print("✅ Hierarchy loaded:", len(state.HIERARCHY_TREE))


@app.on_event("startup")
def startup_event():
    init_hierarchy()
    load_models()
    load_anchor_index()


# ─────────────────────────────────────────────────────────────────────────────
# ROUTERS
# ─────────────────────────────────────────────────────────────────────────────
from tabs_router          import router as tabs_router
from transformer_insights import router as transformer_router
from transformer_roi      import router as roi_router
from transformer_health   import router as health_router
from transformer_anomalies import router as anomalies_router

app.include_router(tabs_router,         prefix="/transformer-dashboard/api/v1", tags=["Tabs"])
app.include_router(transformer_router,  prefix="/transformer-dashboard/api/v1", tags=["Transformer Insights"])
app.include_router(roi_router,          prefix="/transformer-dashboard/api/v1", tags=["ROI / Cost Savings"])
app.include_router(health_router,       prefix="/transformer-dashboard/api/v1", tags=["Thermal & Health"])
app.include_router(anomalies_router,    prefix="/transformer-dashboard/api/v1", tags=["Anomaly Rectification"])


# ─────────────────────────────────────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "status":  True,
        "message": "APDCL Transformer Analytics API running",
        "version": "1.0.0",
    }


@app.get("/health")
def health():
    anchor = state.ANCHOR_INDEX
    return {
        "status":            True,
        "models_loaded":     list(state.MODELS.keys()),
        "models_count":      len(state.MODELS),
        "anchor_index_rows": 0 if anchor is None else len(anchor),
        "anchor_columns":    [] if anchor is None else list(anchor.columns),
        "anchor_loaded":     anchor is not None and not anchor.empty,
        "hierarchy_loaded":  state.HIERARCHY_TREE is not None,
    }
