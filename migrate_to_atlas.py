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

import pymongo
import certifi
from datetime import datetime, timezone

LOCAL_URI = "mongodb://localhost:27017/"
ATLAS_URI = "mongodb+srv://poorvi1si23ad037_db_user:CvgWGwtnby2Uvtxj@cluster0.xvwmptt.mongodb.net/?appName=Cluster0"
DB_NAME   = "urban_env_db"

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
local_client = pymongo.MongoClient(LOCAL_URI, serverSelectionTimeoutMS=5000)
local_db     = local_client[DB_NAME]

print("Connecting to Atlas...")
atlas_client = pymongo.MongoClient(ATLAS_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=15000)
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
