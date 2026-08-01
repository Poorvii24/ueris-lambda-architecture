import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common.db import build_client

# SECURITY: URI must come from the environment -- never hardcode credentials.
URI = os.environ.get("MONGO_URI") or os.environ.get("ATLAS_URI")
if not URI:
    raise SystemExit("Set MONGO_URI (or ATLAS_URI) before running this script.")

client = build_client(URI, serverSelectionTimeoutMS=15000)

# Drop entire database to completely free all storage
client.drop_database("urban_env_db")
print("Dropped entire urban_env_db database")
print("All storage freed.")
print("Collections now:", client["urban_env_db"].list_collection_names())
client.close()
