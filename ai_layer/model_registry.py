"""
ai_layer/model_registry.py
===========================
UERIS — Model Registry

Handles persistence, versioning, and loading of all trained AI models.
Models stored in MongoDB ai_models collection.
"""

import base64
import pickle
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

import pymongo


class ModelRegistry:
    COLLECTION = "ai_models"

    def __init__(self, db: pymongo.database.Database):
        self._db  = db
        self._col = db[self.COLLECTION]
        self._col.create_index(
            [("city",1),("model_type",1),("model_name",1),("horizon",1)],
            unique=True, background=True,
        )
        self._col.create_index(
            [("city",1),("model_type",1),("is_best",1)], background=True
        )

    def save(self, city, model_type, model_name, model, metrics,
             feature_cols, horizon="monthly", is_best=False) -> bool:
        try:
            doc = {
                "city": city, "model_type": model_type,
                "model_name": model_name, "horizon": horizon,
                "model_b64": base64.b64encode(pickle.dumps(model)).decode(),
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
        except Exception as e:
            print(f"[ModelRegistry] save failed: {e}")
            return False

    def load(self, city, model_type, model_name, horizon="monthly"):
        try:
            doc = self._col.find_one(
                {"city": city, "model_type": model_type,
                 "model_name": model_name, "horizon": horizon}, {"_id": 0},
            )
            if not doc:
                return None, None
            model = pickle.loads(base64.b64decode(doc["model_b64"]))
            meta  = {k: v for k, v in doc.items() if k != "model_b64"}
            return model, meta
        except Exception as e:
            print(f"[ModelRegistry] load failed: {e}")
            return None, None

    def load_best(self, city, model_type, horizon="monthly"):
        try:
            doc = self._col.find_one(
                {"city": city, "model_type": model_type,
                 "is_best": True, "horizon": horizon}, {"_id": 0},
            )
            if not doc:
                doc = self._col.find_one(
                    {"city": city, "model_type": model_type, "horizon": horizon},
                    {"_id": 0}, sort=[("metrics.rmse", pymongo.ASCENDING)],
                )
            if not doc:
                return None, None
            model = pickle.loads(base64.b64decode(doc["model_b64"]))
            meta  = {k: v for k, v in doc.items() if k != "model_b64"}
            return model, meta
        except Exception as e:
            print(f"[ModelRegistry] load_best failed: {e}")
            return None, None

    def get_all_metrics(self, city, model_type, horizon="monthly"):
        try:
            return list(self._col.find(
                {"city": city, "model_type": model_type, "horizon": horizon},
                {"_id": 0, "model_b64": 0},
            ))
        except:
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

    def delete_city_models(self, city, model_type=None):
        query = {"city": city}
        if model_type:
            query["model_type"] = model_type
        return self._col.delete_many(query).deleted_count
