# ─────────────────────────────────────────────────────────────────────────────
# tabs_router.py — Tabs metadata for Transformer Dashboard
# ─────────────────────────────────────────────────────────────────────────────
#
# 6 core tabs (aligned with smart-meter contract) + 3 transformer-specific tabs:
#   • thermal-health     — oil / winding / hotspot temps + thermal stress
#   • loss-of-life       — aging, LoL%, remaining life, replacement planning
#   • roi-savings        — ₹ savings from all cost-reduction models (APDCL tariff)
#
# ─────────────────────────────────────────────────────────────────────────────

from fastapi import APIRouter

router = APIRouter()


@router.get("/tabs")
def get_tabs():
    return {
        "success":     True,
        "message":     "SUCCESSFULL",
        "tabsVersion": "transformer-9",
        "data": [
            # ── CORE 6 (same contract as smart-meter) ─────────────────────────
            # {"name": "Data-Incoming",     "code": "data-incoming",     "group": "core"},
            # {"name": "Analytics",         "code": "analytics",         "group": "core"},
            # {"name": "Performance",       "code": "performance",       "group": "core"},
            # {"name": "Real-Time-Monitor", "code": "real-time-monitor", "group": "core"},
            # {"name": "Insights",          "code": "insights",          "group": "core"},
            {"name": "AI Insights",       "code": "ai-insight",       "type": "api"},

            # ── TRANSFORMER-SPECIFIC 3 ────────────────────────────────────────
            {"name": "Thermal Health",    "code": "thermal-health",    "group": "transformer"},
            {"name": "Loss of Life",      "code": "loss-of-life",      "group": "transformer"},
            {"name": "ROI & Cost Savings","code": "roi-savings",       "group": "transformer"},
        ],
    }
