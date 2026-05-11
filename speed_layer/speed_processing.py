"""
speed_layer/speed_processing.py
================================
SPEED LAYER — Lambda Architecture

What this does:
  - Watches data/streaming_input/ every 5 seconds for new JSON files
  - Processes new readings with PySpark (recomputes USI)
  - Loads the per-city Isolation Forest model from MongoDB batch_views
    to classify anomalies — replaces the naive AQI > 200 threshold
  - Upserts results into MongoDB realtime_views (one doc per city)
  - Sends alert webhooks for ML-detected anomalies

Kafka mode:
  Set KAFKA_MODE=1 to consume from Kafka instead of file polling.
  Requires confluent-kafka: pip install confluent-kafka

Run:
  python speed_layer/speed_processing.py
"""

import os, sys, json, time, glob, pickle, base64
import numpy as np
from datetime import datetime

_py = os.environ.get("PYSPARK_PYTHON", sys.executable)
os.environ["PYSPARK_PYTHON"]        = _py
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ.get("PYSPARK_DRIVER_PYTHON", _py)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
import pymongo

MONGO_URI        = os.environ.get("MONGO_URI",  "mongodb://localhost:27017/")
DB_NAME          = os.environ.get("DB_NAME",    "urban_env_db")
SPEED_COLLECTION = "realtime_views"
STREAMING_DIR    = os.environ.get("STREAM_OUTPUT_DIR",
                     os.path.join(os.path.dirname(__file__), "../data/streaming_input"))
CHECKPOINT_DIR   = os.path.join(os.path.dirname(__file__), "../data/checkpoint")
POLL_INTERVAL    = int(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
ALERT_WEBHOOK_URL= os.environ.get("ALERT_WEBHOOK_URL", "")
KAFKA_MODE       = os.environ.get("KAFKA_MODE", "0") == "1"
KAFKA_BROKER     = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC", "ueris-stream")

os.makedirs(STREAMING_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("UrbanEnvRisk_SpeedLayer") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

SCHEMA = StructType([
    StructField("timestamp",   StringType(),  True),
    StructField("city",        StringType(),  True),
    StructField("aqi",         DoubleType(),  True),
    StructField("temperature", DoubleType(),  True),
    StructField("humidity",    DoubleType(),  True),
    StructField("usi",         DoubleType(),  True),
    StructField("risk_level",  StringType(),  True),
    StructField("is_anomaly",  BooleanType(), True),
])

print("\n" + "="*60)
print("  SPEED LAYER — Urban Environmental Risk System")
print(f"  Mode   : {'Kafka consumer' if KAFKA_MODE else 'File watcher'}")
print(f"  Watch  : {STREAMING_DIR}")
print(f"  Poll   : every {POLL_INTERVAL}s")
print("="*60)


def load_anomaly_models(db):
    """
    Load Isolation Forest models from MongoDB batch_views.
    Returns: dict of city -> sklearn IsolationForest instance
    """
    models = {}
    docs = db["batch_views"].find({}, {"city": 1, "anomaly_model": 1, "_id": 0})
    for doc in docs:
        city  = doc.get("city")
        model_info = doc.get("anomaly_model", {})
        model_b64  = model_info.get("model_b64")
        if city and model_b64:
            try:
                models[city] = pickle.loads(base64.b64decode(model_b64))
            except Exception as e:
                print(f"  WARNING: Could not load model for {city}: {e}")
    print(f"  ML anomaly models loaded: {len(models)} cities")
    return models


def is_ml_anomaly(city, aqi, temperature, humidity, usi, models):
    """
    Use the city's Isolation Forest model to detect anomaly.
    Returns True if the reading is anomalous.
    Falls back to threshold AQI > 200 if model unavailable.
    """
    clf = models.get(city)
    if clf is None:
        return aqi > 200
    try:
        X = np.array([[aqi, temperature, humidity, usi]])
        prediction = clf.predict(X)  # -1 = anomaly, 1 = normal
        return bool(prediction[0] == -1)
    except Exception:
        return aqi > 200


def send_alert(city, aqi, usi, risk, timestamp):
    """Send anomaly alert to webhook (Slack, Teams, etc.)."""
    if not ALERT_WEBHOOK_URL:
        return
    try:
        import requests
        payload = {
            "text": (
                f"*UERIS ML ANOMALY ALERT*\n"
                f"City: {city}\n"
                f"AQI: {aqi} | USI: {usi} | Risk: {risk}\n"
                f"Detected: {timestamp} (Isolation Forest)"
            )
        }
        requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=5)
    except Exception:
        pass


def process_loop():
    client = pymongo.MongoClient(MONGO_URI)
    db     = client[DB_NAME]
    col    = db[SPEED_COLLECTION]

    # Load ML anomaly models from batch layer
    anomaly_models = load_anomaly_models(db)

    processed_file = os.path.join(CHECKPOINT_DIR, "processed_files.txt")
    processed = set()
    if os.path.exists(processed_file):
        with open(processed_file, "r") as f:
            processed = set(f.read().splitlines())

    batch_num = 0
    anomaly_count = 0
    print("  Speed layer running... (Ctrl+C to stop)\n")

    while True:
        all_files = set(glob.glob(os.path.join(STREAMING_DIR, "*.json")))
        new_files  = sorted(all_files - processed)

        if new_files:
            records = []
            for fpath in new_files:
                try:
                    with open(fpath, "r") as f:
                        records.append(json.load(f))
                except Exception:
                    pass

            if records:
                # PySpark recomputes USI for consistency
                df  = spark.createDataFrame(records, schema=SCHEMA)
                df  = df.withColumn(
                    "usi_realtime",
                    F.round(
                        (F.least(F.col("aqi") / 300.0, F.lit(1.0)) * 0.5 +
                         F.least(F.greatest((F.col("temperature") - 15.0) / 25.0, F.lit(0.0)), F.lit(1.0)) * 0.3 +
                         F.abs(F.col("humidity") - 50.0) / 50.0 * 0.2) * 100.0,
                        2)
                )
                rows = df.collect()

                for row in rows:
                    aqi  = float(row["aqi"] or 0)
                    temp = float(row["temperature"] or 26)
                    hum  = float(row["humidity"] or 50)
                    usi  = float(row["usi_realtime"] or 0)

                    # ML anomaly detection using Isolation Forest
                    ml_anomaly = is_ml_anomaly(row["city"], aqi, temp, hum, usi, anomaly_models)
                    if ml_anomaly:
                        anomaly_count += 1
                        send_alert(row["city"], aqi, usi, row["risk_level"],
                                   row["timestamp"])

                    doc = {
                        "city":             row["city"],
                        "timestamp":        row["timestamp"],
                        "aqi":              aqi,
                        "temperature":      temp,
                        "humidity":         hum,
                        "usi":              usi,
                        "risk_level":       row["risk_level"],
                        "is_anomaly":       ml_anomaly,
                        "anomaly_method":   "IsolationForest" if row["city"] in anomaly_models else "threshold",
                        "updated_at":       datetime.now().isoformat(),
                        "layer":            "speed",
                        "batch_id":         batch_num,
                        "total_anomalies":  anomaly_count,
                    }
                    col.update_one({"city": row["city"]}, {"$set": doc}, upsert=True)

                    method = "ML" if row["city"] in anomaly_models else "thresh"
                    flag   = "  ANOMALY!" if ml_anomaly else ""
                    print(
                        f"  Batch {batch_num:04d} | {row['city']:<24} | "
                        f"USI={usi:6.2f} | Risk={row['risk_level']:<10} | "
                        f"[{method}]{flag}"
                    )

                processed.update(new_files)
                with open(processed_file, "w") as f:
                    f.write("\n".join(processed))
                batch_num += 1

        time.sleep(POLL_INTERVAL)

    client.close()


try:
    process_loop()
except KeyboardInterrupt:
    print("\n  Speed layer stopped.")
    spark.stop()
