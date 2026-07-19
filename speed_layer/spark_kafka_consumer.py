"""
speed_layer/spark_kafka_consumer.py
=====================================
UERIS — Spark Structured Streaming + Kafka Consumer

This module implements the enterprise-grade speed layer using
PySpark Structured Streaming consuming from Kafka.

Architecture:
    Kafka (ueris.env.readings)
          ↓
    Spark Structured Streaming (readStream from Kafka)
          ↓
    USI computation (PySpark SQL)
          ↓
    Isolation Forest anomaly detection (Python UDF)
          ↓
    MongoDB realtime_views (foreachBatch upsert)

This is the Kafka-mode counterpart to speed_processing.py.
Both write to the same MongoDB collection and are interchangeable.

Run:
    KAFKA_MODE=kafka python speed_layer/spark_kafka_consumer.py

Environment variables:
    MONGO_URI                   MongoDB connection string
    KAFKA_BROKER                Kafka bootstrap server
    KAFKA_TOPIC                 Source topic (default: ueris.env.readings)
    KAFKA_CONSUMER_GROUP        Consumer group ID
    SPARK_TRIGGER_INTERVAL_S    Micro-batch interval in seconds (default: 10)
    CHECKPOINT_DIR              Spark checkpoint location
"""

import os
import sys
import json
import pickle
import base64
import time
from datetime import datetime, timezone

# ── Python path: make streaming module importable ──────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── PySpark Python executable ──────────────────────────────────────────────────
_py = os.environ.get("PYSPARK_PYTHON", sys.executable)
os.environ["PYSPARK_PYTHON"]        = _py
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ.get("PYSPARK_DRIVER_PYTHON", _py)

import numpy as np
import pymongo
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, LongType
)

from streaming.kafka_config import (
    get_spark_kafka_options,
    KAFKA_BROKER,
    KAFKA_TOPIC,
    KAFKA_DLQ_TOPIC,
)
from streaming.monitoring import logger, metrics
from streaming.dlq_handler import DLQHandler

# ── Config ─────────────────────────────────────────────────────────────────────
MONGO_URI             = os.environ.get("MONGO_URI",  "mongodb://localhost:27017/")
DB_NAME               = os.environ.get("DB_NAME",    "urban_env_db")
SPEED_COLLECTION      = "realtime_views"
CHECKPOINT_DIR        = os.environ.get(
    "CHECKPOINT_DIR",
    os.path.join(os.path.dirname(__file__), "../data/checkpoint_kafka")
)
TRIGGER_INTERVAL_S    = int(os.environ.get("SPARK_TRIGGER_INTERVAL_S", "10"))
ALERT_WEBHOOK_URL     = os.environ.get("ALERT_WEBHOOK_URL", "")

os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# ── Schema for incoming Kafka JSON value ───────────────────────────────────────
MESSAGE_SCHEMA = StructType([
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
    StructField("fetch_duration_ms", LongType(), True),
])


def load_anomaly_models(mongo_uri: str, db_name: str) -> dict:
    """
    Load Isolation Forest models from MongoDB batch_views.
    Returns dict of city -> sklearn IsolationForest instance.
    """
    models = {}
    try:
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
        db     = client[db_name]
        docs   = db["batch_views"].find({}, {"city": 1, "anomaly_model": 1, "_id": 0})
        for doc in docs:
            city = doc.get("city")
            info = doc.get("anomaly_model", {})
            b64  = info.get("model_b64")
            if city and b64:
                try:
                    models[city] = pickle.loads(base64.b64decode(b64))
                except Exception as e:
                    logger.warning("spark.model.load.failed", city=city, error=str(e))
        client.close()
        logger.info("spark.models.loaded", count=len(models))
    except Exception as e:
        logger.error("spark.models.load.error", error=str(e))
    return models


def process_batch(batch_df, batch_id: int, anomaly_models: dict, dlq: DLQHandler):
    """
    foreachBatch function called by Spark for each micro-batch.

    Args:
        batch_df:       Spark DataFrame for this micro-batch
        batch_id:       Monotonically increasing batch identifier
        anomaly_models: Dict of city -> IsolationForest
        dlq:            DLQ handler for invalid records
    """
    if batch_df.isEmpty():
        logger.debug("spark.batch.empty", batch_id=batch_id)
        return

    t_start    = time.monotonic()
    rows       = batch_df.collect()
    processed  = 0
    anomalies  = 0

    mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    col          = mongo_client[DB_NAME][SPEED_COLLECTION]

    try:
        for row in rows:
            city = row["city"]
            if not city:
                dlq.send(
                    raw_message=str(dict(row.asDict())),
                    error="Empty city field",
                    topic=KAFKA_TOPIC,
                    partition=row.get("kafka_partition", 0),
                    offset=row.get("kafka_offset", -1),
                )
                continue

            aqi         = float(row["aqi"] or 0)
            temperature = float(row["temperature"] or 26)
            humidity    = float(row["humidity"] or 50)

            # USI recomputed by Spark (consistent with batch layer)
            usi  = float(row["usi_spark"] or 0)
            risk = row["risk_spark"] or "Unknown"

            # Anomaly detection
            clf = anomaly_models.get(city)
            if clf is not None:
                try:
                    X          = np.array([[aqi, temperature, humidity, usi]])
                    prediction = clf.predict(X)
                    is_anomaly = bool(prediction[0] == -1)
                    method     = "IsolationForest"
                except Exception:
                    is_anomaly = aqi > 200
                    method     = "threshold_fallback"
            else:
                is_anomaly = aqi > 200
                method     = "threshold"

            if is_anomaly:
                anomalies += 1
                metrics.record_anomaly(city)
                _send_alert_webhook(city, aqi, usi, risk, method)
                logger.warning(
                    "spark.anomaly.detected",
                    city=city, aqi=aqi, usi=usi, risk=risk,
                    method=method, batch_id=batch_id,
                )

            # Upsert into MongoDB
            doc = {
                "city":            city,
                "timestamp":       row["timestamp"],
                "aqi":             aqi,
                "temperature":     temperature,
                "humidity":        humidity,
                "usi":             usi,
                "risk_level":      risk,
                "is_anomaly":      is_anomaly,
                "anomaly_method":  method,
                "updated_at":      datetime.now(timezone.utc).isoformat(),
                "layer":           "speed_spark",
                "kafka_partition": row.get("kafka_partition"),
                "kafka_offset":    row.get("kafka_offset"),
                "spark_batch_id":  batch_id,
                "data_source":     row.get("data_source", "kafka"),
            }

            col.update_one({"city": city}, {"$set": doc}, upsert=True)
            metrics.record_message_received(city)
            processed += 1

    finally:
        mongo_client.close()

    elapsed_ms = round((time.monotonic() - t_start) * 1000)
    logger.info(
        "spark.batch.complete",
        batch_id=batch_id,
        total_rows=len(rows),
        processed=processed,
        anomalies=anomalies,
        elapsed_ms=elapsed_ms,
    )


def _send_alert_webhook(city: str, aqi: float, usi: float, risk: str, method: str):
    """Send anomaly alert to webhook (non-blocking)."""
    if not ALERT_WEBHOOK_URL:
        return
    try:
        import requests
        payload = {
            "text": (
                f"*UERIS SPARK ANOMALY*\n"
                f"City: {city} | AQI: {aqi} | USI: {usi} | Risk: {risk}\n"
                f"Detection: {method} | {datetime.now(timezone.utc).isoformat()}"
            )
        }
        requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=5)
    except Exception:
        pass


def build_spark_session() -> SparkSession:
    """Build Spark session configured for Kafka Structured Streaming."""
    # Spark Kafka connector version must match Spark version
    spark_version  = "3.5.1"
    scala_version  = "2.12"
    kafka_pkg      = (
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version},"
        f"org.apache.kafka:kafka-clients:3.4.0"
    )

    spark = SparkSession.builder \
        .appName("UERIS_SpeedLayer_Kafka") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars.packages", kafka_pkg) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def run():
    """Main entry point for Spark Kafka consumer."""
    logger.info(
        "spark.consumer.starting",
        broker=KAFKA_BROKER,
        topic=KAFKA_TOPIC,
        trigger_interval_s=TRIGGER_INTERVAL_S,
    )

    # Load anomaly models once at startup
    anomaly_models = load_anomaly_models(MONGO_URI, DB_NAME)
    dlq            = DLQHandler(kafka_producer=None)

    spark          = build_spark_session()

    # ── Read from Kafka ────────────────────────────────────────────────────────
    kafka_opts = get_spark_kafka_options()
    raw_stream = spark.readStream \
        .format("kafka") \
        .options(**kafka_opts) \
        .load()

    # ── Parse JSON value ───────────────────────────────────────────────────────
    parsed = raw_stream.select(
        F.from_json(
            F.col("value").cast("string"),
            MESSAGE_SCHEMA
        ).alias("data"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    ).select("data.*", "kafka_partition", "kafka_offset") \
     .filter(F.col("city").isNotNull()) \
     .filter(F.col("aqi").isNotNull()) \
     .filter(F.col("temperature").isNotNull()) \
     .filter(F.col("humidity").isNotNull())

    # ── Recompute USI in Spark SQL (consistent with batch layer) ───────────────
    enriched = parsed \
        .withColumn(
            "usi_spark",
            F.round(
                (F.least(F.col("aqi") / 300.0, F.lit(1.0)) * 0.5 +
                 F.least(F.greatest((F.col("temperature") - 15.0) / 25.0, F.lit(0.0)), F.lit(1.0)) * 0.3 +
                 F.abs(F.col("humidity") - 50.0) / 50.0 * 0.2) * 100.0,
                2)
        ) \
        .withColumn(
            "risk_spark",
            F.when(F.col("usi_spark") < 20, "Low")
             .when(F.col("usi_spark") < 40, "Moderate")
             .when(F.col("usi_spark") < 60, "High")
             .when(F.col("usi_spark") < 80, "Very High")
             .otherwise("Severe")
        )

    # ── Write via foreachBatch ─────────────────────────────────────────────────
    query = enriched.writeStream \
        .foreachBatch(
            lambda df, bid: process_batch(df, bid, anomaly_models, dlq)
        ) \
        .trigger(processingTime=f"{TRIGGER_INTERVAL_S} seconds") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .outputMode("update") \
        .start()

    logger.info("spark.streaming.query.started", trigger_s=TRIGGER_INTERVAL_S)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("spark.consumer.interrupted")
        query.stop()
        spark.stop()
        logger.info("spark.consumer.stopped")


if __name__ == "__main__":
    run()
