# ─────────────────────────────────────────────────────────────────────────────
# precompute_forecasts.py — Populates Mongo cache for Transformer Dashboard
# ─────────────────────────────────────────────────────────────────────────────
#
# PURPOSE:
#   The dashboard API (transformer_insights.py) reads pre-computed forecasts
#   from Mongo — it does NOT run models on every request. This script takes
#   each trained .pkl model, runs batch forecasts for all transformers, and
#   writes results to the collections the API reads from.
#
# WRITES TO:
#   transformer_forecast_aggregated      (one row per asset × model × date)
#   transformer_forecast_ntl_raw         (per-DT NTL classifier alerts)
#   transformer_forecast_thermal_raw     (per-DT thermal anomaly alerts)
#   transformer_forecast_overload_raw    (per-DT overload classifier alerts)
#   transformer_forecast_neutral_raw     (per-DT neutral anomaly alerts — safety)
#   transformer_forecast_freq_raw        (per-DT frequency drift alerts)
#   transformer_forecast_lol_raw         (per-DT loss-of-life values)
#   transformer_forecast_energy_raw      (per-DT energy forecast for drilldown)
#
# USAGE:
#   python precompute_forecasts.py --from_date 2026-04-22 --to_date 2026-05-22
#
# ─────────────────────────────────────────────────────────────────────────────

import os
import pickle
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
from pymongo import MongoClient, UpdateOne

# ── CONFIG ────────────────────────────────────────────────────────────────────
MONGO_URI   = "mongodb://admin:Password123@192.95.51.52:27017/"
DB_NAME     = "iotdb"
MODEL_DIR   = "models"
BATCH_WRITE = 2_000   # Mongo bulk write batch size

COL_AGG      = "transformer_forecast_aggregated"
COL_NTL      = "transformer_forecast_ntl_raw"
COL_THERMAL  = "transformer_forecast_thermal_raw"
COL_OVERLOAD = "transformer_forecast_overload_raw"
COL_NEUTRAL  = "transformer_forecast_neutral_raw"
COL_FREQ     = "transformer_forecast_freq_raw"
COL_LOL      = "transformer_forecast_lol_raw"
COL_ENERGY   = "transformer_forecast_energy_raw"

MODEL_PATHS = [
    "dt_load_forecast_model.pkl",
    "dt_peak_demand_model.pkl",
    "dt_reactive_pf_model.pkl",
    "dt_voltage_profile_model.pkl",
    "dt_phase_imbalance_model.pkl",
    "dt_pq_index_model.pkl",
    "dt_overload_risk_model.pkl",
    "dt_ntl_theft_model.pkl",
    "dt_loss_of_life_model.pkl",
    "dt_load_factor_model.pkl",
    "dt_thermal_anomaly_model.pkl",
    "dt_volt_sag_swell_model.pkl",
    "dt_overload_risk_score_model.pkl",
    "dt_freq_drift_model.pkl",
    "dt_neutral_anomaly_model.pkl",
]

# Each model → which per-DT raw collection (if any) and the aggregation used
RAW_COLLECTION_MAP = {
    "dt_ntl_theft_model.pkl":        COL_NTL,
    "dt_thermal_anomaly_model.pkl":  COL_THERMAL,
    "dt_overload_risk_model.pkl":    COL_OVERLOAD,
    "dt_neutral_anomaly_model.pkl":  COL_NEUTRAL,
    "dt_freq_drift_model.pkl":       COL_FREQ,
    "dt_loss_of_life_model.pkl":     COL_LOL,
    "dt_load_forecast_model.pkl":    COL_ENERGY,
}

# How to roll up per-DT values into the "aggregated" value per date
AGG_METHOD = {
    "dt_load_forecast_model.pkl":       "sum",
    "dt_peak_demand_model.pkl":         "max",
    "dt_reactive_pf_model.pkl":         "mean",
    "dt_voltage_profile_model.pkl":     "mean",
    "dt_phase_imbalance_model.pkl":     "max",
    "dt_pq_index_model.pkl":            "mean",
    "dt_overload_risk_model.pkl":       "sum",
    "dt_ntl_theft_model.pkl":           "sum",
    "dt_loss_of_life_model.pkl":        "max",
    "dt_load_factor_model.pkl":         "mean",
    "dt_thermal_anomaly_model.pkl":     "sum",
    "dt_volt_sag_swell_model.pkl":      "max",
    "dt_overload_risk_score_model.pkl": "mean",
    "dt_freq_drift_model.pkl":          "sum",
    "dt_neutral_anomaly_model.pkl":     "sum",
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_all_models():
    models = {}
    for fname in MODEL_PATHS:
        path = os.path.join(MODEL_DIR, fname)
        if not os.path.exists(path):
            log(f"  ⚠ Missing: {fname}")
            continue
        with open(path, "rb") as f:
            models[fname] = pickle.load(f)
        log(f"  ✔ Loaded: {fname}")
    return models


def build_weather(dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Synthetic weather — replace with open-meteo fetch in production."""
    N   = len(dates)
    rng = np.random.default_rng(int(dates[0].timestamp()) % 2**31)
    doy = dates.dayofyear
    temp = 27 + 3*np.sin(2*np.pi*doy/365) + 4*np.sin(2*np.pi*dates.hour/24) + rng.normal(0, 0.8, N)
    return pd.DataFrame({
        "timestamp":                dates,
        "temperature_2m":           np.round(temp, 1),
        "relative_humidity_2m":     np.round(rng.uniform(65, 90, N), 0),
        "cloud_cover":              np.round(rng.uniform(10, 80, N), 0),
        "wind_speed_10m":           np.round(rng.uniform(2, 12, N), 1),
        "surface_pressure":         np.round(rng.uniform(995, 1015, N), 1),
        "precipitation":            np.round(rng.uniform(0, 5, N), 1),
        "shortwave_radiation":      np.round(rng.uniform(0, 800, N), 0),
        "direct_normal_irradiance": np.round(rng.uniform(0, 700, N), 0),
    })


def encode_assets_batch(model_obj, anchor_df):
    """Reuse the hierarchy encoders from training to encode DTs."""
    encoders   = model_obj["encoders"]
    hier_cols  = model_obj["hier_cols"]
    hier_feats = model_obj["hier_feats"]

    result = anchor_df[["assetUid"]].copy().reset_index(drop=True)
    for col, feat in zip(hier_cols, hier_feats):
        le   = encoders[col]
        vals = (
            anchor_df[col].astype(str).fillna("unknown")
            if col in anchor_df.columns
            else pd.Series(["unknown"] * len(anchor_df))
        )
        result[feat] = vals.apply(
            lambda v: int(le.transform([v])[0]) if v in le.classes_ else 0
        ).values
    return result


def batch_predict(model_obj, anchor_df, forecast_dates, weather, hier_enc):
    """
    Simplified batch forecast — iterates through each date, builds a feature
    matrix for all transformers, and predicts in a single vectorised call.

    NOTE: This implementation builds lag features from the model's last_known
    buffer. For forecasts > 1 step ahead we assume zero drift (latest values
    persist); the training pipeline's full iterative roll-forward is beyond
    this file's scope and should be ported from smartmeter_insights.py's
    batch_forecast_model() if needed.
    """
    model         = model_obj["model"]
    features      = model_obj["features"]
    is_classifier = model_obj["type"] == "classification"
    lk            = model_obj["last_known"].copy()

    if "timestamp" in lk.columns:
        lk["timestamp"] = pd.to_datetime(lk["timestamp"])

    # Latest values per DT — used as lag-1 fallback
    latest = (lk.sort_values("timestamp")
                .groupby("assetUid", as_index=False)
                .last())

    n_dts = len(anchor_df)
    rows  = []

    for ts in forecast_dates:
        # Build feature row for every transformer for this timestamp
        feat_df = anchor_df[["assetUid"]].copy().reset_index(drop=True)

        # Hierarchy
        for feat in model_obj["hier_feats"]:
            feat_df[feat] = hier_enc[feat].values

        # Weather
        wr = weather.loc[weather["timestamp"] == ts]
        for col in ["temperature_2m", "relative_humidity_2m", "cloud_cover",
                    "wind_speed_10m", "surface_pressure", "precipitation",
                    "shortwave_radiation", "direct_normal_irradiance"]:
            feat_df[col] = float(wr[col].iloc[0]) if len(wr) > 0 and col in wr.columns else 0.0

        # Time
        feat_df["hour"]        = ts.hour
        feat_df["day_of_week"] = ts.dayofweek
        feat_df["month"]       = ts.month
        feat_df["weekend_flag"]= int(ts.dayofweek >= 5)

        # Pull the rest from latest (lag-1 persistence assumption)
        merge_cols = [c for c in features
                      if c not in feat_df.columns and c in latest.columns]
        if merge_cols:
            feat_df = feat_df.merge(
                latest[["assetUid"] + merge_cols],
                on="assetUid", how="left"
            )

        # Fill any still-missing features with zero
        for col in features:
            if col not in feat_df.columns:
                feat_df[col] = 0.0
        feat_df[features] = feat_df[features].fillna(0)

        X = feat_df[features]
        preds = model.predict(X)

        if is_classifier:
            probas = model.predict_proba(X)[:, 1]
            confs  = np.round(probas * 100, 1)
        else:
            preds = np.maximum(preds, 0.0)
            confs = np.full(n_dts, None)

        date_str = ts.date().isoformat()
        day_name = ts.strftime("%A")
        ts_str   = ts.isoformat()

        for i, aid in enumerate(anchor_df["assetUid"].values):
            val = float(preds[i])
            rows.append({
                "assetUid":   str(aid),
                "timestamp":  ts_str,
                "date":       date_str,
                "day_name":   day_name,
                "hour":       ts.hour,
                "value":      int(val) if is_classifier else round(val, 4),
                "confidence": float(confs[i]) if confs[i] is not None else None,
                "office_id":  int(anchor_df.iloc[i].get("officeID", anchor_df.iloc[i].get("officeId", 0))),
            })

    return pd.DataFrame(rows)


def aggregate(per_dt_df: pd.DataFrame, model_key: str) -> list:
    """Roll per-DT predictions up to (office_id × date) aggregated rows."""
    if per_dt_df.empty:
        return []

    method = AGG_METHOD.get(model_key, "sum")
    grouped = per_dt_df.groupby(["office_id", "date"], as_index=False)

    if method == "sum":
        agg = grouped["value"].sum()
    elif method == "mean":
        agg = grouped["value"].mean()
    else:  # max
        agg = grouped["value"].max()

    rows = []
    for _, r in agg.iterrows():
        rows.append({
            "office_id": int(r["office_id"]),
            "date":      r["date"],
            "value":     round(float(r["value"]), 4),
            "model_key": model_key,
            "day_name":  datetime.strptime(r["date"], "%Y-%m-%d").strftime("%A"),
        })
    return rows


def bulk_upsert(coll, rows, key_fields):
    if not rows:
        return
    ops = []
    for r in rows:
        key = {f: r[f] for f in key_fields}
        ops.append(UpdateOne(key, {"$set": r}, upsert=True))
        if len(ops) >= BATCH_WRITE:
            coll.bulk_write(ops, ordered=False)
            ops = []
    if ops:
        coll.bulk_write(ops, ordered=False)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main(from_date, to_date):
    log("=" * 72)
    log(f"TRANSFORMER FORECAST PRECOMPUTE  {from_date}  →  {to_date}")
    log("=" * 72)

    # ── Load models ─────────────────────────────────────────────────────────
    log("Loading trained models ...")
    models = load_all_models()
    if not models:
        raise RuntimeError("No models found in ./models/ — train them first.")

    # ── Build anchor from load-forecast model ───────────────────────────────
    anchor = models["dt_load_forecast_model.pkl"]["last_known"].copy()
    if "timestamp" in anchor.columns:
        anchor["timestamp"] = pd.to_datetime(anchor["timestamp"])
    anchor = (anchor.sort_values("timestamp")
                    .groupby("assetUid", as_index=False)
                    .last())
    log(f"Anchor built: {len(anchor):,} transformers")

    # ── Date range (hourly granularity — aligned with training) ─────────────
    dates_hourly = pd.date_range(
        start=datetime.fromisoformat(from_date),
        end  =datetime.fromisoformat(to_date) + timedelta(hours=23),
        freq ="h",
    )
    log(f"Forecast steps (hourly): {len(dates_hourly):,}")

    weather = build_weather(dates_hourly)
    log(f"Weather built: {weather.shape}")

    # ── Mongo connection ────────────────────────────────────────────────────
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    # ── Process each model ──────────────────────────────────────────────────
    for model_key, obj in models.items():
        log("─" * 72)
        log(f"MODEL: {model_key}")
        hier_enc = encode_assets_batch(obj, anchor)
        preds_df = batch_predict(obj, anchor, dates_hourly, weather, hier_enc)

        # Write per-DT raw (if model has a raw collection)
        raw_coll = RAW_COLLECTION_MAP.get(model_key)
        if raw_coll is not None:
            raw_rows = preds_df.to_dict("records")
            log(f"  → Writing {len(raw_rows):,} raw rows to {raw_coll}")
            bulk_upsert(db[raw_coll], raw_rows,
                        key_fields=["assetUid", "timestamp"])

        # Write aggregated rollup (office_id × date)
        agg_rows = aggregate(preds_df, model_key)
        log(f"  → Writing {len(agg_rows):,} aggregated rows to {COL_AGG}")
        bulk_upsert(db[COL_AGG], agg_rows,
                    key_fields=["office_id", "date", "model_key"])

    log("=" * 72)
    log("✔ Precompute complete")
    log("=" * 72)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--from_date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to_date",   required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.from_date, args.to_date)
