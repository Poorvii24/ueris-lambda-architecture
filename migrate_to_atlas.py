"""
migrate_to_atlas.py
====================
Copies only essential collections from local MongoDB to Atlas.
Skips heavy model binaries to stay within 512MB free tier.

Essential collections (lean data only):
  - batch_views       (stats, trends, risk_distribution — NO model_b64)
  - correlations      (Pearson matrices)
  - data_quality      (coverage stats)
  - trend_profiles    (seasonal/yearly patterns — NO worst/best periods)
  - system_insights   (executive summary)

Run AFTER all batch scripts have finished locally:
  py -3.11 migrate_to_atlas.py
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from common.db import build_client

# SECURITY: never hardcode Atlas credentials in source. Both URIs come from
# the environment. Set ATLAS_URI (and optionally LOCAL_URI) before running:
#   set ATLAS_URI=mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/?appName=Cluster0
#   py -3.11 migrate_to_atlas.py
LOCAL_URI = os.environ.get("LOCAL_URI", "mongodb://localhost:27017/")
ATLAS_URI = os.environ.get("ATLAS_URI")
DB_NAME   = os.environ.get("DB_NAME", "urban_env_db")

if not ATLAS_URI:
    raise SystemExit(
        "ATLAS_URI environment variable is not set. Refusing to run with a "
        "hardcoded credential. Set ATLAS_URI and re-run."
    )

# Fields to exclude from each collection to save space
EXCLUDE_FIELDS = {
    "batch_views": {
        "anomaly_model.model_b64": 0,
        "city_insight": 0,
        "forecasting_models": 0,
    },
    "trend_profiles": {
        "worst_periods": 0,
        "best_periods": 0,
    },
}

print("Connecting to local MongoDB...")
local_client = build_client(LOCAL_URI, serverSelectionTimeoutMS=5000)
local_db     = local_client[DB_NAME]

print("Connecting to Atlas...")
atlas_client = build_client(ATLAS_URI, serverSelectionTimeoutMS=15000)
atlas_db     = atlas_client[DB_NAME]

COLLECTIONS = ["batch_views", "correlations", "data_quality", "trend_profiles", "system_insights"]

for col_name in COLLECTIONS:
    try:
        # Get projection (exclude heavy fields)
        projection = EXCLUDE_FIELDS.get(col_name, {})
        if projection:
            docs = list(local_db[col_name].find({}, {**projection, "_id": 0}))
        else:
            docs = list(local_db[col_name].find({}, {"_id": 0}))

        if not docs:
            print(f"  {col_name}: no documents found locally — skipping")
            continue

        # Drop existing Atlas collection and reinsert
        atlas_db[col_name].drop()
        atlas_db[col_name].insert_many(docs)
        print(f"  {col_name}: {len(docs)} documents migrated to Atlas")

    except Exception as e:
        print(f"  {col_name}: FAILED — {e}")

print("\nVerifying Atlas collections:")
for col_name in COLLECTIONS:
    try:
        count = atlas_db[col_name].count_documents({})
        print(f"  {col_name}: {count} docs in Atlas")
    except Exception as e:
        print(f"  {col_name}: error — {e}")

local_client.close()
atlas_client.close()
print("\nMigration complete!")
