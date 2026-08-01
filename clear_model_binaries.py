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

print("Collections before cleanup:")
for col in db.list_collection_names():
    print(f"  {col}: {db[col].count_documents({})} docs")

# Remove heavy model binary data (keeps metrics, feature_importance, trained_at)
result = db["ai_models"].update_many({}, {"$unset": {"model_b64": ""}})
print(f"\nCleared model binaries from {result.modified_count} ai_models documents")

# Also remove model_b64 from batch_views anomaly_model
result2 = db["batch_views"].update_many({}, {"$unset": {"anomaly_model.model_b64": ""}})
print(f"Cleared anomaly model binaries from {result2.modified_count} batch_views documents")

print("\nCollections after cleanup:")
for col in db.list_collection_names():
    print(f"  {col}: {db[col].count_documents({})} docs")

print("\nDone! Storage freed. Now run compute_atlas_collections.py")
client.close()
