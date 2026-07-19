"""
streaming/dlq_handler.py
=========================
UERIS — Dead Letter Queue Handler

Handles messages that cannot be processed after MAX_RETRIES attempts.
Failed messages are:
  1. Published to the Kafka DLQ topic (ueris.env.dlq) if Kafka is available
  2. Written to local dlq/failed_<timestamp>.jsonl as backup
  3. Logged as structured error events

Usage:
    from streaming.dlq_handler import DLQHandler

    dlq = DLQHandler()
    dlq.send(raw_message, error="ValidationError: missing aqi", topic="ueris.env.readings")
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from streaming.monitoring import logger, metrics
from streaming.schema import build_dlq_message

_DEFAULT_DLQ_DIR = os.path.join(os.path.dirname(__file__), "../data/dlq")
MAX_RETRIES      = int(os.environ.get("DLQ_MAX_RETRIES", "3"))


class DLQHandler:
    """
    Dead Letter Queue handler for the UERIS streaming pipeline.

    Dual-sink architecture:
    - Primary: Kafka DLQ topic (if Kafka producer available)
    - Fallback: local JSONL file (always)
    """

    def __init__(self, kafka_producer=None, dlq_topic: str = "ueris.env.dlq"):
        self._producer  = kafka_producer
        self._dlq_topic = dlq_topic
        # Read DLQ_DIR lazily so tests can override via patch.dict(os.environ)
        dlq_dir = os.environ.get("DLQ_DIR", _DEFAULT_DLQ_DIR)
        Path(dlq_dir).mkdir(parents=True, exist_ok=True)
        self._dlq_file  = os.path.join(
            dlq_dir,
            f"dlq_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.jsonl"
        )
        logger.info("dlq.handler.initialized",
                    dlq_topic=dlq_topic,
                    dlq_file=self._dlq_file,
                    max_retries=MAX_RETRIES)

    def send(
        self,
        raw_message: str,
        error: str,
        topic: str = "ueris.env.readings",
        partition: int = 0,
        offset: int = -1,
        retry_count: int = 0,
    ):
        """
        Send a failed message to the DLQ.

        Args:
            raw_message:  The original raw message string that failed
            error:        Human-readable error description
            topic:        Source Kafka topic
            partition:    Source partition number
            offset:       Source offset
            retry_count:  How many times this message has been retried
        """
        dlq_msg = build_dlq_message(
            original_message=raw_message,
            error=error,
            topic=topic,
            partition=partition,
            offset=offset,
            retry_count=retry_count,
        )

        # Always write to local file (guaranteed durability)
        self._write_local(dlq_msg)

        # Publish to Kafka DLQ topic if producer is available
        if self._producer:
            self._publish_kafka(dlq_msg)

        # Record in metrics
        metrics.record_dlq(reason=error[:50])

        logger.error(
            "dlq.message.sent",
            error=error,
            topic=topic,
            partition=partition,
            offset=offset,
            retry_count=retry_count,
            raw_preview=raw_message[:120] if raw_message else "",
        )

    def _write_local(self, dlq_msg: dict):
        """Append DLQ message to local JSONL file."""
        try:
            with open(self._dlq_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(dlq_msg) + "\n")
        except Exception as e:
            logger.error("dlq.local.write.failed", error=str(e))

    def _publish_kafka(self, dlq_msg: dict):
        """Publish DLQ message to Kafka DLQ topic."""
        try:
            self._producer.produce(
                self._dlq_topic,
                key="dlq",
                value=json.dumps(dlq_msg),
            )
            self._producer.poll(0)
        except Exception as e:
            logger.error("dlq.kafka.publish.failed", error=str(e))

    def should_retry(self, retry_count: int) -> bool:
        """Return True if retry_count is below the max retry threshold."""
        return retry_count < MAX_RETRIES

    def get_dlq_stats(self) -> dict:
        """Return DLQ file stats."""
        try:
            size  = Path(self._dlq_file).stat().st_size
            count = 0
            with open(self._dlq_file, "r") as f:
                count = sum(1 for _ in f)
            return {"file": self._dlq_file, "size_bytes": size, "message_count": count}
        except Exception:
            return {"file": self._dlq_file, "size_bytes": 0, "message_count": 0}
