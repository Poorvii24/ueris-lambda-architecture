"""
streaming/producer.py
======================
UERIS — Enterprise Kafka Producer

Wraps confluent-kafka Producer with:
  - Automatic retry with exponential backoff
  - Delivery confirmation callbacks
  - Dead Letter Queue for permanently failed messages
  - Structured logging of every publish event
  - Graceful shutdown (flush on exit)

Usage:
    from streaming.producer import UERISProducer

    producer = UERISProducer()
    producer.send(city="Delhi", aqi=185.0, temperature=29.5, humidity=55.0)
    producer.flush()
    producer.close()

Environment variables: see streaming/kafka_config.py
"""

import json
import time
import os
from datetime import datetime, timezone
from typing import Optional

from streaming.kafka_config import (
    get_producer_config,
    KAFKA_TOPIC,
    KAFKA_DLQ_TOPIC,
    KAFKA_BROKER,
)
from streaming.schema import build_reading, ValidationError, build_dlq_message
from streaming.dlq_handler import DLQHandler
from streaming.monitoring import logger, metrics

MAX_RETRIES        = int(os.environ.get("KAFKA_PRODUCER_RETRIES", "5"))
RETRY_BACKOFF_BASE = float(os.environ.get("RETRY_BACKOFF_BASE_S", "0.5"))  # seconds


class UERISProducer:
    """
    Production-grade Kafka producer for the UERIS streaming pipeline.

    Thread-safe. Designed to be instantiated once per process and reused
    across all city fetch cycles.
    """

    def __init__(self, topic: str = None):
        """
        Initialise Kafka producer and DLQ handler.

        Args:
            topic: Kafka topic to publish to (default: KAFKA_TOPIC env var)

        Raises:
            ImportError: if confluent-kafka is not installed
            RuntimeError: if broker connection fails
        """
        try:
            from confluent_kafka import Producer
        except ImportError:
            raise ImportError(
                "confluent-kafka is not installed. "
                "Install with: pip install confluent-kafka"
            )

        self._topic    = topic or KAFKA_TOPIC
        self._cfg      = get_producer_config()
        self._producer = Producer(self._cfg)
        self._dlq      = DLQHandler(kafka_producer=self._producer, dlq_topic=KAFKA_DLQ_TOPIC)
        self._closed   = False

        # Verify broker connectivity immediately — fail fast if unreachable
        print(f"[PRODUCER] connecting to broker={KAFKA_BROKER} topic={self._topic}", flush=True)
        remaining = self._producer.flush(timeout=10)
        if remaining > 0:
            print(f"[PRODUCER WARNING] flush returned {remaining} undelivered (normal at init)", flush=True)

        logger.info(
            "producer.initialized",
            broker=KAFKA_BROKER,
            topic=self._topic,
            acks=self._cfg.get("acks"),
            retries=self._cfg.get("retries"),
            idempotence=self._cfg.get("enable.idempotence"),
        )
        print(f"[PRODUCER] initialized OK broker={KAFKA_BROKER} topic={self._topic}", flush=True)

    # ── Delivery callback ──────────────────────────────────────────────────────

    def _on_delivery(self, err, msg):
        """
        Called by confluent-kafka for every message after delivery attempt.
        Runs in the producer's internal thread.
        """
        if err:
            # Print directly to stdout so it appears in docker logs
            print(
                f"[PRODUCER ERROR] delivery failed | "
                f"topic={msg.topic()} partition={msg.partition()} "
                f"error={err}",
                flush=True
            )
            logger.error(
                "producer.delivery.failed",
                error=str(err),
                error_code=err.code() if hasattr(err, 'code') else None,
                topic=msg.topic(),
                partition=msg.partition(),
                key=msg.key().decode() if msg.key() else None,
            )
            metrics.record_error("DeliveryFailure")
        else:
            print(
                f"[PRODUCER OK] city={msg.key().decode() if msg.key() else '?'} "
                f"topic={msg.topic()} partition={msg.partition()} "
                f"offset={msg.offset()}",
                flush=True
            )
            logger.debug(
                "producer.delivery.confirmed",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                key=msg.key().decode() if msg.key() else None,
            )

    # ── Public API ─────────────────────────────────────────────────────────────

    def send(
        self,
        city: str,
        aqi: float,
        temperature: float,
        humidity: float,
        lat: float = None,
        lon: float = None,
        source: str = "simulator",
        data_source: str = "Open-Meteo+WAQI",
        fetch_duration_ms: int = None,
    ) -> bool:
        """
        Validate and publish one environmental reading to Kafka.

        Args:
            city:              City name (must match UERIS city list)
            aqi:               Air Quality Index value
            temperature:       Temperature in Celsius
            humidity:          Relative humidity %
            lat/lon:           Coordinates (optional, for geo features)
            source:            'simulator' or 'live'
            data_source:       API source description
            fetch_duration_ms: How long the API fetch took

        Returns:
            True if successfully enqueued, False if sent to DLQ

        Note:
            This is non-blocking. Call flush() to ensure delivery.
        """
        if self._closed:
            raise RuntimeError("Producer is closed")

        # ── Step 1: Schema validation ──────────────────────────────────────────
        try:
            record = build_reading(
                city=city,
                aqi=aqi,
                temperature=temperature,
                humidity=humidity,
                lat=lat,
                lon=lon,
                source=source,
                data_source=data_source,
                fetch_duration_ms=fetch_duration_ms,
            )
        except ValidationError as e:
            raw = json.dumps({
                "city": city, "aqi": aqi,
                "temperature": temperature, "humidity": humidity
            })
            self._dlq.send(
                raw_message=raw,
                error=f"ValidationError: {e}",
                topic=self._topic,
            )
            metrics.record_error("ValidationError", city=city)
            return False

        # ── Step 2: Publish with retry ─────────────────────────────────────────
        payload     = json.dumps(record, ensure_ascii=False)
        message_key = city.encode("utf-8")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=message_key,
                    value=payload.encode("utf-8"),
                    on_delivery=self._on_delivery,
                )
                # poll(1) — block up to 1 second to trigger delivery callbacks
                # This makes delivery confirmations appear immediately in logs
                self._producer.poll(1)
                metrics.record_message_sent(city)
                print(
                    f"[PRODUCER] enqueued city={city} "
                    f"topic={self._topic} attempt={attempt}",
                    flush=True
                )
                logger.debug(
                    "producer.message.enqueued",
                    city=city,
                    aqi=record.get("aqi"),
                    temperature=record.get("temperature"),
                    attempt=attempt,
                )
                return True

            except BufferError:
                # Producer queue full — flush and retry
                print(f"[PRODUCER] buffer full for {city}, flushing...", flush=True)
                logger.warning("producer.buffer.full", city=city, attempt=attempt)
                self._producer.flush(timeout=10)

            except Exception as e:
                backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                print(
                    f"[PRODUCER ERROR] send failed city={city} "
                    f"attempt={attempt}/{MAX_RETRIES} error={e}",
                    flush=True
                )
                logger.warning(
                    "producer.send.failed",
                    city=city,
                    attempt=attempt,
                    max_retries=MAX_RETRIES,
                    error=str(e),
                    backoff_s=backoff,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(backoff)

        # All retries exhausted → DLQ
        logger.error(
            "producer.max.retries.exceeded",
            city=city,
            max_retries=MAX_RETRIES,
        )
        self._dlq.send(
            raw_message=payload,
            error=f"MaxRetriesExceeded after {MAX_RETRIES} attempts",
            topic=self._topic,
            retry_count=MAX_RETRIES,
        )
        metrics.record_error("MaxRetriesExceeded", city=city)
        return False

    def flush(self, timeout: float = 30.0):
        """
        Flush all buffered messages. Call before exiting to ensure delivery.

        Args:
            timeout: Maximum seconds to wait for all deliveries
        """
        if self._closed:
            return
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning("producer.flush.timeout", undelivered=remaining)
        else:
            logger.info("producer.flush.complete")

    def close(self):
        """Flush and close the producer gracefully."""
        if not self._closed:
            self.flush()
            self._closed = True
            logger.info("producer.closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
