"""
migrate_models_to_files.py
============================
One-time migration for EXISTING data: if your live MongoDB (Atlas or
local) still has model_b64 blobs sitting in it from before this storage
refactor, this script extracts each one, saves it to disk via
StorageManager, and rewrites the document to hold only metadata.

Safe to run multiple times (idempotent) -- documents that no longer have
model_b64 are simply skipped.

Run:
    py -3.11 migrate_models_to_files.py

Environment: needs MONGO_URI set, same as every other script in this
project (see common/db.py).
"""

import base64
import os
import pickle
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from common.db import get_db
from common.storage import StorageManager, get_logger

log = get_logger("migration")


def migrate_ai_models_collection(storage: StorageManager) -> int:
    """Migrate documents in the `ai_models` collection (ModelRegistry's
    collection) that still carry a raw model_b64 blob."""
    db = storage.db()
    col = db["ai_models"]
    migrated = 0

    for doc in col.find({"model_b64": {"$exists": True}}):
        city       = doc.get("city", "unknown")
        model_type = doc.get("model_type", "model")
        model_name = doc.get("model_name", model_type)
        horizon    = doc.get("horizon", "monthly")

        try:
            model = pickle.loads(base64.b64decode(doc["model_b64"]))
            file_meta = storage.save_model(
                model, city=city, model_type=model_type,
                model_name=model_name, horizon=horizon, fmt="joblib",
            )
            col.update_one(
                {"_id": doc["_id"]},
                {"$set": file_meta, "$unset": {"model_b64": ""}},
            )
            migrated += 1
            log.info(f"Migrated ai_models doc: {city}/{model_type}/{model_name}")
        except Exception as e:
            log.error(f"Failed to migrate ai_models doc {doc.get('_id')}: {e}")

    return migrated


def migrate_batch_views_anomaly_model(storage: StorageManager) -> int:
    """Migrate any leftover model_b64 inside batch_views.<city>.anomaly_model
    (the pre-refactor location). Only used as a fallback for cities whose
    ai_models collection doesn't already have a current anomaly_ensemble --
    new code no longer writes here at all."""
    db = storage.db()
    col = db["batch_views"]
    registry_col = db["ai_models"]
    migrated = 0

    for doc in col.find({"anomaly_model.model_b64": {"$exists": True}}):
        city = doc.get("city", "unknown")

        already_has_current = registry_col.find_one({
            "city": city, "model_type": "anomaly_ensemble", "horizon": "realtime",
        })
        if already_has_current:
            # ai_models already has the authoritative copy -- just strip
            # the stale blob from batch_views, nothing to migrate.
            col.update_one({"_id": doc["_id"]}, {"$unset": {"anomaly_model.model_b64": ""}})
            log.info(f"Dropped stale batch_views blob for {city} (ai_models copy already current)")
            continue

        try:
            b64 = doc["anomaly_model"]["model_b64"]
            model = pickle.loads(base64.b64decode(b64))
            file_meta = storage.save_model(
                model, city=city, model_type="anomaly_ensemble",
                model_name="AnomalyEnsemble", horizon="realtime", fmt="joblib",
            )
            registry_col.update_one(
                {"city": city, "model_type": "anomaly_ensemble",
                 "model_name": "AnomalyEnsemble", "horizon": "realtime"},
                {"$set": {
                    "city": city, "model_type": "anomaly_ensemble",
                    "model_name": "AnomalyEnsemble", "horizon": "realtime",
                    **file_meta,
                    "metrics": {}, "feature_cols": ["aqi", "temperature", "humidity", "usi"],
                    "trained_at": datetime.now(timezone.utc).isoformat(),
                    "is_best": True,
                    "migrated_from": "batch_views.anomaly_model",
                }},
                upsert=True,
            )
            col.update_one({"_id": doc["_id"]}, {"$unset": {"anomaly_model.model_b64": ""}})
            migrated += 1
            log.info(f"Migrated batch_views anomaly_model for {city}")
        except Exception as e:
            log.error(f"Failed to migrate batch_views anomaly_model for {city}: {e}")

    return migrated


if __name__ == "__main__":
    storage = StorageManager()
    if not storage.ping():
        raise SystemExit("Cannot reach MongoDB -- check MONGO_URI before running migration.")

    print("=" * 60)
    print("  UERIS model storage migration: Mongo blobs -> files")
    print("=" * 60)

    before = storage.storage_stats()
    n1 = migrate_ai_models_collection(storage)
    n2 = migrate_batch_views_anomaly_model(storage)
    after = storage.storage_stats()

    print(f"\nMigrated {n1} ai_models document(s), {n2} batch_views document(s).")
    print(f"Local models/ folder now holds "
          f"{after['models_bytes'] / 1024:.1f} KB "
          f"(was {before['models_bytes'] / 1024:.1f} KB before this run).")
    print("\nRun compute_atlas_collections.py or check Atlas storage in a few")
    print("minutes to confirm the cluster's reported storage size has dropped.")
    print("=" * 60)
