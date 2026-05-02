# ─────────────────────────────────────────────────────────────────────────────
# stats_helpers.py — Shared helpers for Response-2 compliant stat cards
# ─────────────────────────────────────────────────────────────────────────────
#
# Response-2 stat-card shape:
#   { 
#     "label": "Avg Power Factor",
#     "value": "0.9317",
#     "icon":  "ri-pulse-line",
#     "threshold_status": { "percentage": 0.93, "status": "stable" },   # optional
#     "tooltip": "Fleet-wide average power factor..."                    # optional
#   }
#
# Provides:
#   • fmt(val, unit)           → Response-2-styled display string
#   • make_stat(...)           → assembles a stat-card dict with optional
#                                threshold_status and tooltip
#   • build_threshold(...)     → computes { percentage, status } from value +
#                                rule (direction, thresholds)
# ─────────────────────────────────────────────────────────────────────────────

from typing import Optional, Dict, Any

# Currency symbols are prefixed; everything else is suffixed with a space.
_CURRENCY_UNITS = {"₹", "$", "€", "£"}


def fmt(val, unit: str = "") -> str:
    """
    Combine a numeric value with its unit into a single display string,
    matching Response-2's stringified-value convention.

    Examples:
        fmt(68, "transformers")    → "68 transformers"
        fmt(217.12, "kW")          → "217.12 kW"
        fmt(1654265.04, "kWh")     → "1,654,265.04 kWh"   (comma-formatted)
        fmt(579092, "₹")           → "₹579092"
        fmt(0.9317, "")            → "0.9317"
        fmt("0", "events")         → "0 events"
    """
    # Currency: prefix, no space, no thousands separator (matches Response-1 legacy)
    if unit in _CURRENCY_UNITS:
        return f"{unit}{val}"

    # For large numeric values we add commas (matches "1,654,265.04 kWh" style)
    if isinstance(val, (int, float)) and abs(val) >= 1000:
        val_str = f"{val:,}"
    else:
        val_str = str(val)

    if unit == "":
        return val_str
    return f"{val_str} {unit}"


def build_threshold(
    value: float,
    *,
    warn: Optional[float] = None,
    crit: Optional[float] = None,
    direction: str = "higher_is_worse",
    percentage: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Compute { percentage, status } for a stat card.

    direction:
      • "higher_is_worse" — value above warn/crit triggers warning/critical
                            (e.g. loss %, imbalance %, demand utilization %)
      • "lower_is_worse"  — value below warn/crit triggers warning/critical
                            (e.g. power factor, PQ index)

    `percentage` is the raw displayed number used by the frontend for a
    badge/bar visual; when omitted, defaults to `value`.

    Status values: "stable" | "warning" | "critical"
    """
    pct = percentage if percentage is not None else value

    # Default: no breach
    status = "stable"

    if direction == "higher_is_worse":
        if crit is not None and value >= crit:
            status = "critical"
        elif warn is not None and value >= warn:
            status = "warning"
    elif direction == "lower_is_worse":
        if crit is not None and value <= crit:
            status = "critical"
        elif warn is not None and value <= warn:
            status = "warning"

    return {
        "percentage": round(float(pct), 2) if isinstance(pct, float) else pct,
        "status":     status,
    }


def make_stat(
    label: str,
    value,
    icon: str,
    *,
    tooltip: Optional[str] = None,
    threshold_status: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Construct a stat-card dict with optional tooltip and threshold_status.
    Keeps JSON keys in a stable order (label → value → icon → threshold → tooltip).
    """
    card: Dict[str, Any] = {"label": label, "value": value, "icon": icon}

    if threshold_status is not None:
        card["threshold_status"] = threshold_status

    if tooltip is not None:
        card["tooltip"] = tooltip

    return card


# ─────────────────────────────────────────────────────────────────────────────
# FORECAST META — optional block appended at the response root to mirror
# Response-2's `forecast_meta` field. Individual routers may override.
# ─────────────────────────────────────────────────────────────────────────────
def build_forecast_meta(
    from_date: str,
    to_date: str,
    models: Optional[Dict[str, Dict[str, str]]] = None,
    training_period: str = "",
    frequency: str = "30-min intervals aggregated to daily",
) -> Dict[str, Any]:
    """
    Build the forecast_meta block (Response-2 parity).
    """
    return {
        "models":             models or {},
        "training_period":    training_period,
        "forecast_window":    f"{from_date} to {to_date}",
        "forecast_frequency": frequency,
    }
