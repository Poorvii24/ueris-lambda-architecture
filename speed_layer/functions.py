"""
speed_layer/functions.py
=========================
UERIS — Pure Business Logic Functions (PySpark-free)

These functions are extracted from speed_processing.py so they can be
imported and tested without requiring PySpark to be installed.

Imported by:
  - speed_layer/speed_processing.py  (simulation mode)
  - speed_layer/spark_kafka_consumer.py (kafka mode)
  - tests/test_ueris.py (unit tests)
"""

import os
import time
import pickle
import base64

import numpy as np
import pymongo

RETRY_BACKOFF_BASE = float(os.environ.get("RETRY_BACKOFF_BASE_S", "0.5"))


def compute_usi(aqi: float, temperature: float, humidity: float) -> float:
    """Urban Stress Index — canonical formula, identical across all layers."""
    aqi_norm  = min(aqi / 300.0, 1.0)
    temp_norm = min(max((temperature - 15.0) / 25.0, 0.0), 1.0)
    hum_norm  = abs(humidity - 50.0) / 50.0
    return round((0.5 * aqi_norm + 0.3 * temp_norm + 0.2 * hum_norm) * 100.0, 2)


def classify_risk(usi: float) -> str:
    if usi < 20: return "Low"
    if usi < 40: return "Moderate"
    if usi < 60: return "High"
    if usi < 80: return "Very High"
    return "Severe"


def is_ml_anomaly(
    city: str,
    aqi: float,
    temperature: float,
    humidity: float,
    usi: float,
    models: dict,
) -> tuple:
    """
    Detect anomaly using city-specific Isolation Forest model.
    Falls back to AQI > 200 threshold if model unavailable.

    Returns:
        (is_anomaly: bool, method: str)
    """
    clf = models.get(city)
    if clf is None:
        return aqi > 200, "threshold"
    try:
        X          = np.array([[aqi, temperature, humidity, usi]])
        prediction = clf.predict(X)
        return bool(prediction[0] == -1), "IsolationForest"
    except Exception:
        return aqi > 200, "threshold_fallback"


def load_anomaly_models(db, logger=None) -> dict:
    """
    Load per-city Isolation Forest models from MongoDB batch_views.
    Returns dict of city → sklearn IsolationForest instance.
    """
    models = {}
    try:
        docs = db["batch_views"].find(
            {}, {"city": 1, "anomaly_model": 1, "_id": 0}
        )
        for doc in docs:
            city = doc.get("city")
            info = doc.get("anomaly_model", {})
            b64  = info.get("model_b64")
            if city and b64:
                try:
                    models[city] = pickle.loads(base64.b64decode(b64))
                except Exception as e:
                    if logger:
                        logger.warning("model.load.failed", city=city, error=str(e))
    except Exception as e:
        if logger:
            logger.error("models.load.error", error=str(e))
    return models


def upsert_with_retry(
    col,
    city: str,
    doc: dict,
    max_retries: int = 3,
    logger=None,
) -> bool:
    """
    Upsert a document into MongoDB with exponential backoff retry.

    Args:
        col:         PyMongo collection
        city:        City name (used as filter key)
        doc:         Document to upsert
        max_retries: Maximum retry attempts
        logger:      Optional structured logger

    Returns:
        True if successful, False if all retries exhausted
    """
    for attempt in range(1, max_retries + 1):
        try:
            col.update_one({"city": city}, {"$set": doc}, upsert=True)
            return True
        except pymongo.errors.AutoReconnect as e:
            backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
            if logger:
                logger.warning(
                    "mongo.retry", city=city, attempt=attempt,
                    max_retries=max_retries, error=str(e), backoff_s=backoff,
                )
            if attempt < max_retries:
                time.sleep(backoff)
        except pymongo.errors.PyMongoError as e:
            if logger:
                logger.error("mongo.upsert.failed", city=city, attempt=attempt, error=str(e))
            return False
    if logger:
        logger.error("mongo.max_retries_exceeded", city=city, max_retries=max_retries)
    return False


def save_checkpoint(checkpoint_file: str, processed: set, logger=None):
    """Persist checkpoint to disk. Overwrites with full set."""
    try:
        with open(checkpoint_file, "w") as f:
            f.write("\n".join(sorted(processed)))
    except Exception as e:
        if logger:
            logger.error("checkpoint.save.failed", error=str(e))
