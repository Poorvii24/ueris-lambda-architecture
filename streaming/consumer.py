"""
streaming/consumer.py
======================
UERIS — Enterprise Kafka Consumer

Consumes from ueris.env.readings topic and:
  1. Validates message schema
  2. Computes USI using the same formula as the batch layer
  3. Applies Isolation Forest anomaly detection (model loaded from MongoDB)
  4. Upserts result into MongoDB realtime_views
  5. Sends alert webhooks for ML-detected anomalies
  6. Routes invalid messages to Dead Letter Queue
  7. Commits offsets manually (at-least-once delivery)

This is the Kafka-mode equivalent of speed_layer/speed_processing.py.
Both modes write to the same MongoDB collection and are interchangeable.

Usage:
    from streaming.consumer import UERISConsumer

    consumer = UERISConsumer()
    consumer.start()          # blocks forever (run in a thread or process)
    consumer.stop()           # graceful shutdown

Environment variables: see streaming/kafka_config.py
"""

import json
import os
import pickle
import base64
import time
import threading
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pymongo

from streaming.kafka_config import (
    get_consumer_config,
    KAFKA_TOPIC,
    KAFKA_DLQ_TOPIC,
    KAFKA_BROKER,
)
from streaming.schema import validate, ValidationError, SCHEMA_VERSION
from streaming.dlq_handler import DLQHandler
from streaming.monitoring import logger, metrics

# ── Config ─────────────────────────────────────────────────────────────────────
MONGO_URI         = os.environ.get("MONGO_URI",  "mongodb://localhost:27017/")
DB_NAME           = os.environ.get("DB_NAME",    "urban_env_db")
SPEED_COLLECTION  = "realtime_views"
POLL_TIMEOUT_S    = float(os.environ.get("KAFKA_POLL_TIMEOUT_S", "1.0"))
COMMIT_EVERY_N    = int(os.environ.get("KAFKA_COMMIT_EVERY_N",   "10"))
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "")
MODEL_RELOAD_MIN  = int(os.environ.get("MODEL_RELOAD_INTERVAL_MIN", "60"))


# ── Pure functions (shared with speed_processing.py) ──────────────────────────

def compute_usi(aqi: float, temperature: float, humidity: float) -> float:
    """Urban Stress Index — identical formula to batch layer."""
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


def send_alert_webhook(city: str, aqi: float, usi: float, risk: str, method: str):
    """Send anomaly alert to Slack/Teams webhook."""
    if not ALERT_WEBHOOK_URL:
        return
    try:
        import requests
        payload = {
            "text": (
                f"*UERIS ML ANOMALY ALERT*\n"
                f"City: {city} | AQI: {aqi} | USI: {usi} | Risk: {risk}\n"
                f"Detection: {method} | Time: {datetime.now(timezone.utc).isoformat()}"
            )
        }
        requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=5)
    except Exception as e:
        logger.warning("alert.webhook.failed", error=str(e))


class UERISConsumer:
    """
    Production Kafka consumer for the UERIS speed layer.

    Features:
    - Manual offset commit (at-least-once delivery guarantee)
    - Isolation Forest anomaly detection loaded from MongoDB
    - Periodic model reload (MODEL_RELOAD_INTERVAL_MIN)
    - DLQ routing for invalid / unprocessable messages
    - Structured logging + metrics for every consumed message
    - Graceful shutdown via stop()
    """

    def __init__(self, group_id: str = None):
        try:
            from confluent_kafka import Consumer, KafkaError, KafkaException
            self._KafkaError     = KafkaError
            self._KafkaException = KafkaException
        except ImportError:
            raise ImportError(
                "confluent-kafka is not installed. "
                "Install with: pip install confluent-kafka"
            )

        self._cfg      = get_consumer_config(group_id)
        self._consumer = Consumer(self._cfg)
        self._stopped  = threading.Event()

        # MongoDB
        self._mongo_client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        self._db           = self._mongo_client[DB_NAME]
        self._col          = self._db[SPEED_COLLECTION]

        # DLQ (file-only for consumer; Kafka DLQ published via separate producer)
        self._dlq = DLQHandler(kafka_producer=None, dlq_topic=KAFKA_DLQ_TOPIC)

        # Anomaly models (loaded from MongoDB batch_views)
        self._anomaly_models    = {}
        self._last_model_load   = 0.0
        self._load_anomaly_models()

        # Commit counter
        self._uncommitted = 0

        logger.info(
            "consumer.initialized",
            broker=KAFKA_BROKER,
            topic=KAFKA_TOPIC,
            group_id=self._cfg["group.id"],
        )

    # ── Anomaly models ─────────────────────────────────────────────────────────

    def _load_anomaly_models(self):
        """
        Load Isolation Forest models from MongoDB batch_views.
        Called on startup and periodically during operation.
        """
        models = {}
        try:
            docs = self._db["batch_views"].find(
                {}, {"city": 1, "anomaly_model": 1, "_id": 0}
            )
            for doc in docs:
                city  = doc.get("city")
                info  = doc.get("anomaly_model", {})
                b64   = info.get("model_b64")
                if city and b64:
                    try:
                        models[city] = pickle.loads(base64.b64decode(b64))
                    except Exception as e:
                        logger.warning("consumer.model.load.failed", city=city, error=str(e))
            self._anomaly_models  = models
            self._last_model_load = time.monotonic()
            logger.info("consumer.models.loaded", count=len(models))
        except Exception as e:
            logger.error("consumer.models.load.error", error=str(e))

    def _maybe_reload_models(self):
        """Reload models if MODEL_RELOAD_MIN minutes have elapsed."""
        elapsed_min = (time.monotonic() - self._last_model_load) / 60
        if elapsed_min >= MODEL_RELOAD_MIN:
            logger.info("consumer.models.reloading", elapsed_min=round(elapsed_min))
            self._load_anomaly_models()

    def _detect_anomaly(
        self, city: str, aqi: float, temperature: float, humidity: float, usi: float
    ) -> tuple[bool, str]:
        """
        Detect anomaly using city-specific Isolation Forest model.
        Falls back to AQI > 200 threshold if model unavailable.

        Returns:
            (is_anomaly: bool, method: str)
        """
        clf = self._anomaly_models.get(city)
        if clf is None:
            return aqi > 200, "threshold"
        try:
            X          = np.array([[aqi, temperature, humidity, usi]])
            prediction = clf.predict(X)   # -1 = anomaly, 1 = normal
            return bool(prediction[0] == -1), "IsolationForest"
        except Exception as e:
            logger.warning("consumer.anomaly.detect.failed", city=city, error=str(e))
            return aqi > 200, "threshold_fallback"

    # ── Message processing ─────────────────────────────────────────────────────

    def _process_message(self, raw_value: str, partition: int, offset: int) -> bool:
        """
        Process one Kafka message: validate → compute → upsert → commit.

        Returns:
            True if processed successfully, False if sent to DLQ
        """
        t_start = time.monotonic()

        # ── Parse JSON ─────────────────────────────────────────────────────────
        try:
            record = json.loads(raw_value)
        except json.JSONDecodeError as e:
            self._dlq.send(
                raw_message=raw_value,
                error=f"JSONDecodeError: {e}",
                topic=KAFKA_TOPIC,
                partition=partition,
                offset=offset,
            )
            metrics.record_error("JSONDecodeError")
            return False

        # ── Schema validation ──────────────────────────────────────────────────
        try:
            record = validate(record)
        except ValidationError as e:
            self._dlq.send(
                raw_message=raw_value,
                error=f"ValidationError: {e}",
                topic=KAFKA_TOPIC,
                partition=partition,
                offset=offset,
            )
            metrics.record_error("ValidationError", city=record.get("city"))
            return False

        # ── Compute USI (speed layer recomputes for consistency) ───────────────
        city        = record["city"]
        aqi         = float(record["aqi"])
        temperature = float(record["temperature"])
        humidity    = float(record["humidity"])

        usi  = compute_usi(aqi, temperature, humidity)
        risk = classify_risk(usi)

        # ── Anomaly detection ──────────────────────────────────────────────────
        self._maybe_reload_models()
        is_anomaly, method = self._detect_anomaly(city, aqi, temperature, humidity, usi)

        if is_anomaly:
            metrics.record_anomaly(city)
            send_alert_webhook(city, aqi, usi, risk, method)
            logger.warning(
                "consumer.anomaly.detected",
                city=city,
                aqi=aqi,
                usi=usi,
                risk=risk,
                method=method,
                partition=partition,
                offset=offset,
            )

        # ── Upsert into MongoDB ────────────────────────────────────────────────
        latency_ms = round((time.monotonic() - t_start) * 1000, 1)
        doc = {
            "city":              city,
            "timestamp":         record["timestamp"],
            "aqi":               aqi,
            "temperature":       temperature,
            "humidity":          humidity,
            "usi":               usi,
            "risk_level":        risk,
            "is_anomaly":        is_anomaly,
            "anomaly_method":    method,
            "updated_at":        datetime.now(timezone.utc).isoformat(),
            "layer":             "speed",
            "kafka_partition":   partition,
            "kafka_offset":      offset,
            "processing_ms":     latency_ms,
            "data_source":       record.get("data_source", "kafka"),
        }

        try:
            self._col.update_one({"city": city}, {"$set": doc}, upsert=True)
        except pymongo.errors.PyMongoError as e:
            logger.error("consumer.mongodb.upsert.failed", city=city, error=str(e))
            metrics.record_error("MongoDBError", city=city)
            # Don't DLQ for transient DB errors — message will be reprocessed on restart
            return False

        metrics.record_message_received(city, latency_ms=latency_ms)
        logger.info(
            "consumer.message.processed",
            city=city,
            aqi=aqi,
            usi=usi,
            risk=risk,
            is_anomaly=is_anomaly,
            method=method,
            latency_ms=latency_ms,
            partition=partition,
            offset=offset,
        )
        return True

    # ── Main loop ──────────────────────────────────────────────────────────────

    def start(self):
        """
        Start consuming from Kafka topic. Blocks until stop() is called.
        """
        self._consumer.subscribe([KAFKA_TOPIC])
        logger.info("consumer.started", topic=KAFKA_TOPIC)

        last_metrics_log = time.monotonic()
        METRICS_INTERVAL = 300   # log summary every 5 minutes

        try:
            while not self._stopped.is_set():
                msg = self._consumer.poll(timeout=POLL_TIMEOUT_S)

                if msg is None:
                    continue

                # ── Kafka errors ───────────────────────────────────────────────
                if msg.error():
                    if msg.error().code() == self._KafkaError._PARTITION_EOF:
                        logger.debug(
                            "consumer.partition.eof",
                            partition=msg.partition(),
                            offset=msg.offset(),
                        )
                        continue
                    else:
                        logger.error("consumer.kafka.error", error=str(msg.error()))
                        metrics.record_error("KafkaError")
                        continue

                # ── Process message ────────────────────────────────────────────
                raw_value = msg.value().decode("utf-8") if msg.value() else ""
                self._process_message(
                    raw_value=raw_value,
                    partition=msg.partition(),
                    offset=msg.offset(),
                )

                # ── Manual offset commit (at-least-once) ───────────────────────
                self._uncommitted += 1
                if self._uncommitted >= COMMIT_EVERY_N:
                    self._consumer.commit(asynchronous=False)
                    self._uncommitted = 0
                    logger.debug("consumer.offset.committed")

                # ── Periodic metrics log ───────────────────────────────────────
                if time.monotonic() - last_metrics_log >= METRICS_INTERVAL:
                    metrics.log_summary(logger)
                    last_metrics_log = time.monotonic()

        except KeyboardInterrupt:
            logger.info("consumer.interrupted")
        finally:
            self._shutdown()

    def stop(self):
        """Signal the consumer to stop gracefully."""
        self._stopped.set()
        logger.info("consumer.stop.requested")

    def _shutdown(self):
        """Commit remaining offsets and close connections."""
        try:
            if self._uncommitted > 0:
                self._consumer.commit(asynchronous=False)
            self._consumer.close()
            self._mongo_client.close()
            logger.info("consumer.shutdown.complete")
        except Exception as e:
            logger.error("consumer.shutdown.error", error=str(e))
