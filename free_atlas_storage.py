import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common.db import build_client

# SECURITY: URI must come from the environment -- never hardcode credentials.
URI = os.environ.get("MONGO_URI") or os.environ.get("ATLAS_URI")
if not URI:
    raise SystemExit("Set MONGO_URI (or ATLAS_URI) before running this script.")

client = build_client(URI, serverSelectionTimeoutMS=15000)
db = client["urban_env_db"]

print("Before cleanup:")
for col in db.list_collection_names():
    print(f"  {col}: {db[col].count_documents({})} docs")

# Drop ai_models entirely (metrics already in batch_views)
db["ai_models"].drop()
print("\nDropped ai_models collection")

# Drop system_insights (can be regenerated)
db["system_insights"].drop()
print("Dropped system_insights collection")

# Remove heavy fields from batch_views
db["batch_views"].update_many({}, {"$unset": {
    "city_insight": "",
    "forecasting_models": "",
}})
print("Cleared heavy fields from batch_views")

# Remove heavy fields from trend_profiles
db["trend_profiles"].update_many({}, {"$unset": {
    "worst_periods": "",
    "best_periods": "",
}})
print("Cleaned trend_profiles")

print("\nAfter cleanup:")
for col in db.list_collection_names():
    print(f"  {col}: {db[col].count_documents({})} docs")

print("\nDone! Now try compute_atlas_collections.py again")
client.close()
