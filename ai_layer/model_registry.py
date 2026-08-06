"""
ai_layer/model_registry.py
===========================
UERIS -- Model Registry.

Handles persistence, versioning, and loading of all trained AI models.

IMPORTANT: as of this version, model *objects* are no longer stored in
MongoDB. They are saved to disk (models/<city>/...) via
common.storage.StorageManager, and MongoDB's `ai_models` collection holds
only metadata: model name/type/version, file path, format, size, accuracy
metrics, feature columns, and timestamps. This is the single biggest
contributor to Atlas free-tier storage exhaustion under the old design --
a handful of pickled ensembles across 26+ cities, re-saved every batch
run, easily ran into hundreds of MB. Metadata documents are a few hundred
bytes each.

Public API (save/load/load_best/get_all_metrics/get_registry_summary/
delete_city_models) is unchanged from the previous version -- every
existing caller (batch_layer, speed_layer, serving_layer, streaming)
keeps working without modification.
"""

import sys
import os
from datetime import datetime, timezone

import pymongo

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.storage import StorageManager, StorageError, get_logger

log = get_logger("model_registry")


class ModelRegistry:
    COLLECTION = "ai_models"

    def __init__(self, db: pymongo.database.Database, storage: StorageManager = None):
        self._db  = db
        self._col = db[self.COLLECTION]
        self._storage = storage or StorageManager()
        self._col.create_index(
            [("city",1),("model_type",1),("model_name",1),("horizon",1)],
            unique=True, background=True,
        )
        self._col.create_index(
            [("city",1),("model_type",1),("is_best",1)], background=True
        )

    def save(self, city, model_type, model_name, model, metrics,
             feature_cols, horizon="monthly", is_best=False, fmt="joblib") -> bool:
        try:
            file_meta = self._storage.save_model(
                model, city=city, model_type=model_type, model_name=model_name,
                horizon=horizon, fmt=fmt,
            )
            doc = {
                "city": city, "model_type": model_type,
                "model_name": model_name, "horizon": horizon,
                **file_meta,  # model_path, model_format, model_size_bytes
                "metrics": metrics, "feature_cols": feature_cols,
                "trained_at": datetime.now(timezone.utc).isoformat(),
                "n_samples": metrics.get("n_samples", 0),
                "is_best": is_best,
            }
            self._col.update_one(
                {"city": city, "model_type": model_type,
                 "model_name": model_name, "horizon": horizon},
                {"$set": doc}, upsert=True,
            )
            if is_best:
                self._col.update_many(
                    {"city": city, "model_type": model_type,
                     "model_name": {"$ne": model_name}, "horizon": horizon},
                    {"$set": {"is_best": False}},
                )
            return True
        except (StorageError, Exception) as e:
            log.error(f"save failed for {city}/{model_type}/{model_name}: {e}")
            return False

    def _load_doc(self, doc):
        """Load the model file referenced by a metadata doc; returns (model, meta)."""
        if not doc:
            return None, None
        model_path = doc.get("model_path")
        if not model_path:
            # Legacy document from before this refactor -- no file to load.
            log.warning(f"Document for {doc.get('city')}/{doc.get('model_type')} has no "
                        f"model_path (pre-migration record). Run migrate_models_to_files.py.")
            return None, None
        try:
            model = self._storage.load_model(model_path, fmt=doc.get("model_format"))
        except StorageError as e:
            log.error(f"load failed for {model_path}: {e}")
            return None, None
        meta = {k: v for k, v in doc.items() if k != "_id"}
        return model, meta

    def load(self, city, model_type, model_name, horizon="monthly"):
        doc = self._col.find_one(
            {"city": city, "model_type": model_type,
             "model_name": model_name, "horizon": horizon}, {"_id": 0},
        )
        return self._load_doc(doc)

    def load_best(self, city, model_type, horizon="monthly"):
        doc = self._col.find_one(
            {"city": city, "model_type": model_type,
             "is_best": True, "horizon": horizon}, {"_id": 0},
        )
        if not doc:
            doc = self._col.find_one(
                {"city": city, "model_type": model_type, "horizon": horizon},
                {"_id": 0}, sort=[("metrics.rmse", pymongo.ASCENDING)],
            )
        return self._load_doc(doc)

    def get_all_metrics(self, city, model_type, horizon="monthly"):
        try:
            return list(self._col.find(
                {"city": city, "model_type": model_type, "horizon": horizon},
                {"_id": 0, "model_path": 0},
            ))
        except Exception:
            return []

    def get_registry_summary(self):
        try:
            return {
                "total_models":       self._col.count_documents({}),
                "cities_with_models": len(self._col.distinct("city")),
                "model_types":        self._col.distinct("model_type"),
                "best_models":        self._col.count_documents({"is_best": True}),
            }
        except Exception as e:
            return {"error": str(e)}

    def delete_city_models(self, city, model_type=None, delete_files=True):
        query = {"city": city}
        if model_type:
            query["model_type"] = model_type
        if delete_files:
            for doc in self._col.find(query, {"model_path": 1}):
                if doc.get("model_path"):
                    self._storage.delete_model_file(doc["model_path"])
        return self._col.delete_many(query).deleted_count
