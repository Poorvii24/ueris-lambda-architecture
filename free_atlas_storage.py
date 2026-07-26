import pymongo
import certifi

URI = "mongodb+srv://poorvi1si23ad037_db_user:CvgWGwtnby2Uvtxj@cluster0.xvwmptt.mongodb.net/?appName=Cluster0"
client = pymongo.MongoClient(URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=15000)
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
