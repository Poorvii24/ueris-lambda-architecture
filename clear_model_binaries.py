import pymongo
import certifi

URI = "mongodb+srv://poorvi1si23ad037_db_user:CvgWGwtnby2Uvtxj@cluster0.xvwmptt.mongodb.net/?appName=Cluster0"

client = pymongo.MongoClient(URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=15000)
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
