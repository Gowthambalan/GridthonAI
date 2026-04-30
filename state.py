# ─────────────────────────────────────────────────────────────────────────────
# state.py — Global state for Transformer Analytics Dashboard
# Mirrors smart-meter structure, but keyed on assetUid (transformer identifier)
# ─────────────────────────────────────────────────────────────────────────────

MODELS         = {}      # { "dt_load_forecast_model.pkl": {model, features, ...}, ... }
ANCHOR_INDEX   = None    # pandas DataFrame — latest known reading per transformer
HIERARCHY_TREE = None    # { office_id: [child_ids], ... }


# ─────────────────────────────────────────────────────────────────────────────
# ID extraction — handles both Mongo JSON dumps and PyMongo int64
# ─────────────────────────────────────────────────────────────────────────────
def extract_id(value):
    """
    Handles both:
      - {"$numberLong": "87"}  (JSON dump)
      - Int64(87) / int        (PyMongo)
    """
    if isinstance(value, dict):
        return int(value.get("$numberLong"))
    return int(value)


# ─────────────────────────────────────────────────────────────────────────────
# Build office hierarchy tree (unchanged from smart-meter side)
# ─────────────────────────────────────────────────────────────────────────────
def build_tree(hierarchy):
    tree = {}

    def dfs(node):
        oid      = extract_id(node["_id"])
        children = node.get("children", [])
        tree[oid] = [extract_id(c["_id"]) for c in children]
        for c in children:
            dfs(c)

    for root in hierarchy:
        dfs(root)

    return tree


def resolve_to_sections(tree, office_id):
    """Walk tree from any node down to all leaf sections."""
    result = []

    def dfs(oid):
        children = tree.get(oid, [])
        if not children:          # leaf → SECTION
            result.append(oid)
            return
        for c in children:
            dfs(c)

    dfs(office_id)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Mongo hierarchy loader
# ─────────────────────────────────────────────────────────────────────────────
from pymongo import MongoClient
import pandas as pd

MONGO_URI = ""


def load_hierarchy():
    client = MongoClient(MONGO_URI)
    col    = client["aiview"]["office_hierarchy_snapshots"]

    # Get latest snapshot
    doc = col.find_one(sort=[("generatedAt", -1)])
    if not doc:
        raise Exception("No hierarchy snapshot found")

    return doc["snapshot"]["offices"]["hierarchy"]
