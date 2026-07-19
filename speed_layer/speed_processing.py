"""
speed_layer/speed_processing.py
================================
UERIS — Speed Layer (Simulation Mode)

Lambda Architecture — Fast Path

What this does:
  - Watches data/streaming_input/ every POLL_INTERVAL_SECONDS for new JSON files
  - Processes new readings with PySpark (recomputes USI for consistency)
  - Loads per-city Isolation Forest models from MongoDB batch_views
  - Detects anomalies using ML (falls back to AQI > 200 threshold)
  - Upserts results into MongoDB realtime_views (one doc per city)
  - Sends alert webhooks for ML-detected anomalies
  - Routes malformed files to Dead Letter Queue
  - Logs structured JSON events for every batch

This file handles SIMULATION MODE only (file polling).
For KAFKA MODE, use speed_layer/spark_kafka_consumer.py.

Run:
  python speed_layer/speed_processing.py

Environment variables:
  MONGO_URI               MongoDB connection string
  DB_NAME                 Database name (default: urban_env_db)
  STREAM_OUTPUT_DIR       Directory to watch for JSON files
  POLL_INTERVAL_SECONDS   Polling interval (default: 5)
  ALERT_WEBHOOK_URL       Slack/Teams webhook for anomaly alerts
  MODEL_RELOAD_INTERVAL_MIN  Minutes between model reloads (default: 60)
  LOG_LEVEL               DEBUG / INFO / WARNING (default: INFO)
  MAX_RETRIES             MongoDB upsert retry count (default: 3)
"""

import os
import sys
import json
import time
import glob
import pickle
import base64
from datetime import datetime, timezone

import numpy as np
import pymongo

# ── Make streaming module importable ───────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from streaming.monitoring import logger, metrics
from streaming.dlq_handler import DLQHandler

# ── PySpark Python executable ──────────────────────────────────────────────────
_py = os.environ.get("PYSPARK_PYTHON", sys.executable)
os.environ["PYSPARK_PYTHON"]        = _py
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ.get("PYSPARK_DRIVER_PYTHON", _py)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType
)

# ── Config (all from environment variables) ────────────────────────────────────
MONGO_URI             = os.environ.get("MONGO_URI",  "mongodb://localhost:27017/")
DB_NAME               = os.environ.get("DB_NAME",    "urban_env_db")
SPEED_COLLECTION      = "realtime_views"
STREAMING_DIR         = os.environ.get(
    "STREAM_OUTPUT_DIR",
    os.path.join(os.path.dirname(__file__), "../data/streaming_input")
)
CHECKPOINT_DIR        = os.path.join(os.path.dirname(__file__), "../data/checkpoint")
POLL_INTERVAL         = int(os.environ.get("POLL_INTERVAL_SECONDS",     "5"))
ALERT_WEBHOOK_URL     = os.environ.get("ALERT_WEBHOOK_URL",              "")
MODEL_RELOAD_MIN      = int(os.environ.get("MODEL_RELOAD_INTERVAL_MIN", "60"))
MAX_RETRIES           = int(os.environ.get("MAX_RETRIES",                "3"))
RETRY_BACKOFF_BASE    = float(os.environ.get("RETRY_BACKOFF_BASE_S",    "0.5"))

os.makedirs(STREAMING_DIR,  exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# ── Message schema for PySpark ─────────────────────────────────────────────────
SCHEMA = StructType([
    StructField("schema_version", StringType(),  True),
    StructField("source",         StringType(),  True),
    StructField("timestamp",      StringType(),  True),
    StructField("city",           StringType(),  True),
    StructField("lat",            DoubleType(),  True),
    StructField("lon",            DoubleType(),  True),
    StructField("aqi",            DoubleType(),  True),
    StructField("temperature",    DoubleType(),  True),
    StructField("humidity",       DoubleType(),  True),
    StructField("usi",            DoubleType(),  True),
    StructField("risk_level",     StringType(),  True),
    StructField("is_anomaly",     BooleanType(), True),
    StructField("data_source",    StringType(),  True),
])

# ── Spark session ──────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("UERIS_SpeedLayer_Simulation") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logger.info(
    "speed_layer.started",
    mode="simulation",
    streaming_dir=STREAMING_DIR,
    poll_interval_s=POLL_INTERVAL,
    model_reload_min=MODEL_RELOAD_MIN,
)

print(f"\n{'='*60}")
print(f"  UERIS Speed Layer — Simulation Mode")
print(f"  Watch  : {STREAMING_DIR}")
print(f"  Poll   : every {POLL_INTERVAL}s")
print(f"  MongoDB: {MONGO_URI[:40]}...")
print(f"{'='*60}\n")


# ── Pure functions ─────────────────────────────────────────────────────────────

def load_anomaly_models(db) -> dict:
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
            city   = doc.get("city")
            info   = doc.get("anomaly_model", {})
            b64    = info.get("model_b64")
            if city and b64:
                try:
                    models[city] = pickle.loads(base64.b64decode(b64))
                except Exception as e:
                    logger.warning(
                        "speed_layer.model.load.failed",
                        city=city, error=str(e)
                    )
        logger.info("speed_layer.models.loaded", count=len(models))
        print(f"  ML models loaded: {len(models)} cities")
    except Exception as e:
        logger.error("speed_layer.models.load.error", error=str(e))
    return models


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
        prediction = clf.predict(X)   # -1 = anomaly, 1 = normal
        return bool(prediction[0] == -1), "IsolationForest"
    except Exception as e:
        logger.warning(
            "speed_layer.model.predict.failed",
            city=city, error=str(e)
        )
        return aqi > 200, "threshold_fallback"


def send_alert(city: str, aqi: float, usi: float, risk: str, method: str):
    """Send anomaly alert to webhook (non-blocking, best-effort)."""
    if not ALERT_WEBHOOK_URL:
        return
    try:
        import requests
        payload = {
            "text": (
                f"*UERIS ML ANOMALY ALERT*\n"
                f"City: {city} | AQI: {aqi} | USI: {usi} | Risk: {risk}\n"
                f"Method: {method} | {datetime.now(timezone.utc).isoformat()}"
            )
        }
        requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=5)
    except Exception as e:
        logger.warning("speed_layer.alert.webhook.failed", error=str(e))


def upsert_with_retry(col, city: str, doc: dict, max_retries: int = MAX_RETRIES):
    """
    Upsert a document into MongoDB with exponential backoff retry.

    Args:
        col:         PyMongo collection
        city:        City name (used as filter key)
        doc:         Document to upsert
        max_retries: Maximum number of retry attempts

    Returns:
        True if successful, False if all retries exhausted
    """
    for attempt in range(1, max_retries + 1):
        try:
            col.update_one({"city": city}, {"$set": doc}, upsert=True)
            return True
        except pymongo.errors.AutoReconnect as e:
            backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
            logger.warning(
                "speed_layer.mongo.retry",
                city=city,
                attempt=attempt,
                max_retries=max_retries,
                error=str(e),
                backoff_s=backoff,
            )
            if attempt < max_retries:
                time.sleep(backoff)
        except pymongo.errors.PyMongoError as e:
            logger.error(
                "speed_layer.mongo.upsert.failed",
                city=city,
                attempt=attempt,
                error=str(e),
            )
            return False
    logger.error(
        "speed_layer.mongo.max_retries_exceeded",
        city=city,
        max_retries=max_retries,
    )
    return False


# ── Main processing loop ───────────────────────────────────────────────────────

def process_loop():
    """
    Main polling loop:
    1. Find new JSON files in streaming_input/
    2. Parse + validate with PySpark
    3. Recompute USI
    4. Run Isolation Forest anomaly detection
    5. Upsert into MongoDB realtime_views
    6. Checkpoint processed files
    7. Route bad files to DLQ
    """
    # MongoDB connection
    mongo_client   = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    db             = mongo_client[DB_NAME]
    col            = db[SPEED_COLLECTION]

    # DLQ handler
    dlq            = DLQHandler(kafka_producer=None)

    # Load ML models
    anomaly_models    = load_anomaly_models(db)
    last_model_reload = time.monotonic()

    # Checkpoint: track processed files
    checkpoint_file = os.path.join(CHECKPOINT_DIR, "processed_files.txt")
    processed       = set()
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            processed = set(line.strip() for line in f if line.strip())
        logger.info(
            "speed_layer.checkpoint.loaded",
            processed_count=len(processed)
        )

    batch_num     = 0
    total_records = 0
    total_anomaly = 0

    print("  Speed layer running... (Ctrl+C to stop)\n")

    while True:
        # ── Periodic model reload ──────────────────────────────────────────────
        elapsed_min = (time.monotonic() - last_model_reload) / 60
        if elapsed_min >= MODEL_RELOAD_MIN:
            logger.info("speed_layer.models.reloading", elapsed_min=round(elapsed_min))
            anomaly_models    = load_anomaly_models(db)
            last_model_reload = time.monotonic()

        # ── Find new files ─────────────────────────────────────────────────────
        all_files  = set(glob.glob(os.path.join(STREAMING_DIR, "*.json")))
        new_files  = sorted(all_files - processed)

        if not new_files:
            time.sleep(POLL_INTERVAL)
            continue

        # ── Parse JSON files ───────────────────────────────────────────────────
        records     = []
        bad_files   = []
        for fpath in new_files:
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    raw     = f.read()
                    record  = json.loads(raw)
                    # Minimal validation before Spark
                    if not record.get("city") or record.get("aqi") is None:
                        raise ValueError(f"Missing required field in {fpath}")
                    records.append(record)
            except json.JSONDecodeError as e:
                bad_files.append(fpath)
                dlq.send(
                    raw_message=open(fpath).read() if os.path.exists(fpath) else fpath,
                    error=f"JSONDecodeError: {e}",
                    topic="file-stream",
                )
                logger.error(
                    "speed_layer.file.parse.failed",
                    file=fpath, error=str(e)
                )
                metrics.record_error("JSONDecodeError")
            except Exception as e:
                bad_files.append(fpath)
                dlq.send(
                    raw_message=fpath,
                    error=f"FileReadError: {e}",
                    topic="file-stream",
                )
                logger.error(
                    "speed_layer.file.read.failed",
                    file=fpath, error=str(e)
                )
                metrics.record_error("FileReadError")

        if not records:
            # All files bad — checkpoint them and move on
            processed.update(new_files)
            _save_checkpoint(checkpoint_file, processed)
            time.sleep(POLL_INTERVAL)
            continue

        # ── PySpark: recompute USI ─────────────────────────────────────────────
        t_batch = time.monotonic()
        try:
            df = spark.createDataFrame(records, schema=SCHEMA)
            df = df.withColumn(
                "usi_realtime",
                F.round(
                    (F.least(F.col("aqi") / 300.0, F.lit(1.0)) * 0.5 +
                     F.least(F.greatest(
                         (F.col("temperature") - 15.0) / 25.0, F.lit(0.0)
                     ), F.lit(1.0)) * 0.3 +
                     F.abs(F.col("humidity") - 50.0) / 50.0 * 0.2) * 100.0,
                    2)
            ).withColumn(
                "risk_realtime",
                F.when(F.col("usi_realtime") < 20, "Low")
                 .when(F.col("usi_realtime") < 40, "Moderate")
                 .when(F.col("usi_realtime") < 60, "High")
                 .when(F.col("usi_realtime") < 80, "Very High")
                 .otherwise("Severe")
            )
            rows = df.collect()
        except Exception as e:
            logger.error("speed_layer.spark.failed", batch=batch_num, error=str(e))
            metrics.record_error("SparkError")
            # Don't checkpoint — retry next cycle
            time.sleep(POLL_INTERVAL)
            continue

        # ── Process each row ───────────────────────────────────────────────────
        batch_success = 0
        batch_anomaly = 0

        for row in rows:
            city = row["city"]
            if not city:
                metrics.record_error("EmptyCityField")
                continue

            aqi   = float(row["aqi"]         or 0)
            temp  = float(row["temperature"] or 26)
            hum   = float(row["humidity"]    or 50)
            usi   = float(row["usi_realtime"] or 0)
            risk  = row["risk_realtime"]     or "Unknown"

            # Isolation Forest anomaly detection
            is_anomaly, method = is_ml_anomaly(city, aqi, temp, hum, usi, anomaly_models)

            if is_anomaly:
                batch_anomaly  += 1
                total_anomaly  += 1
                metrics.record_anomaly(city)
                send_alert(city, aqi, usi, risk, method)
                logger.warning(
                    "speed_layer.anomaly.detected",
                    city=city, aqi=aqi, usi=usi,
                    risk=risk, method=method, batch=batch_num,
                )

            # Build MongoDB document
            doc = {
                "city":            city,
                "timestamp":       row["timestamp"],
                "aqi":             aqi,
                "temperature":     temp,
                "humidity":        hum,
                "usi":             usi,
                "risk_level":      risk,
                "is_anomaly":      is_anomaly,
                "anomaly_method":  method,
                "updated_at":      datetime.now(timezone.utc).isoformat(),
                "layer":           "speed",
                "batch_id":        batch_num,
                "data_source":     row.get("data_source", "simulation"),
            }

            # Upsert with retry
            ok = upsert_with_retry(col, city, doc)
            if ok:
                batch_success += 1
                total_records += 1
                metrics.record_message_received(city)
            else:
                metrics.record_error("MongoUpsertFailed", city=city)

            # Console output
            flag = "  ⚠ ANOMALY" if is_anomaly else ""
            print(
                f"  Batch {batch_num:04d} | {city:<24} | "
                f"AQI={aqi:6.1f} | USI={usi:6.2f} | "
                f"Risk={risk:<10} | [{method[:6]}]{flag}"
            )

        # ── Checkpoint processed files ─────────────────────────────────────────
        processed.update(new_files)
        _save_checkpoint(checkpoint_file, processed)

        # ── Batch summary log ──────────────────────────────────────────────────
        elapsed_ms = round((time.monotonic() - t_batch) * 1000)
        logger.info(
            "speed_layer.batch.complete",
            batch_id=batch_num,
            files=len(new_files),
            records=len(rows),
            success=batch_success,
            anomalies=batch_anomaly,
            elapsed_ms=elapsed_ms,
            total_records=total_records,
            total_anomalies=total_anomaly,
        )

        batch_num += 1
        time.sleep(POLL_INTERVAL)

    mongo_client.close()


def _save_checkpoint(checkpoint_file: str, processed: set):
    """Persist checkpoint to disk. Truncates and rewrites the full set."""
    try:
        with open(checkpoint_file, "w") as f:
            f.write("\n".join(sorted(processed)))
    except Exception as e:
        logger.error("speed_layer.checkpoint.save.failed", error=str(e))


# ── Entry point ────────────────────────────────────────────────────────────────
try:
    process_loop()
except KeyboardInterrupt:
    logger.info("speed_layer.stopped")
    print("\n  Speed layer stopped.")
    spark.stop()
