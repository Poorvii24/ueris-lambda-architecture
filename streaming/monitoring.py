"""
streaming/monitoring.py
========================
UERIS — Structured Logging & Pipeline Monitoring

Provides:
  - Structured JSON logging (compatible with CloudWatch, Datadog, Splunk)
  - Pipeline metrics (messages/sec, latency, error rates, lag)
  - Alert thresholds for anomaly rates
  - Component health status reporting

Usage:
    from streaming.monitoring import logger, PipelineMetrics

    logger.info("producer.send", city="Delhi", aqi=185.0)
    metrics.record_message_sent("Delhi")
    metrics.record_error("ValidationError")
    metrics.log_summary()
"""

import json
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from threading import Lock


# ── Log level from environment ─────────────────────────────────────────────────
LOG_LEVEL   = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_FORMAT  = os.environ.get("LOG_FORMAT", "json")   # "json" or "text"
COMPONENT   = os.environ.get("UERIS_COMPONENT", "streaming")


class StructuredLogger:
    """
    Emits structured JSON log lines with consistent field names.
    Compatible with log aggregation platforms (CloudWatch, Splunk, Datadog).
    """

    def __init__(self, component: str, level: str = "INFO"):
        self._component = component
        self._level_no  = getattr(logging, level, logging.INFO)
        logging.basicConfig(
            level=self._level_no,
            format="%(message)s",
        )
        self._logger = logging.getLogger(component)

    def _emit(self, level: str, event: str, **fields):
        if getattr(logging, level) < self._level_no:
            return
        record = {
            "ts":        datetime.now(timezone.utc).isoformat(),
            "level":     level,
            "component": self._component,
            "event":     event,
        }
        record.update(fields)
        if LOG_FORMAT == "json":
            self._logger.log(getattr(logging, level), json.dumps(record))
        else:
            parts = " ".join(f"{k}={v!r}" for k, v in fields.items())
            self._logger.log(
                getattr(logging, level),
                f"[{level}] [{self._component}] {event} {parts}"
            )

    def debug(self, event: str, **fields):
        self._emit("DEBUG", event, **fields)

    def info(self, event: str, **fields):
        self._emit("INFO", event, **fields)

    def warning(self, event: str, **fields):
        self._emit("WARNING", event, **fields)

    def error(self, event: str, **fields):
        self._emit("ERROR", event, **fields)

    def critical(self, event: str, **fields):
        self._emit("CRITICAL", event, **fields)


class PipelineMetrics:
    """
    Thread-safe pipeline metrics collector.
    Tracks: messages sent/received/failed, latency, DLQ count, anomaly rate.
    """

    def __init__(self, window_seconds: int = 300):
        """
        Args:
            window_seconds: Rolling window for rate calculations (default 5 min)
        """
        self._lock            = Lock()
        self._window          = window_seconds
        self._start_time      = time.monotonic()

        # Counters
        self._messages_sent   = 0
        self._messages_recv   = 0
        self._messages_failed = 0
        self._dlq_count       = 0
        self._anomaly_count   = 0
        self._cities_seen     = set()

        # Rolling window for rate calculation
        self._sent_times      = deque()   # timestamps of sent messages
        self._recv_times      = deque()   # timestamps of received messages
        self._latencies_ms    = deque()   # end-to-end latency in ms

        # Per-city counters
        self._city_counts     = defaultdict(int)
        self._city_errors     = defaultdict(int)

        # Error type counters
        self._error_types     = defaultdict(int)

    def record_message_sent(self, city: str):
        with self._lock:
            self._messages_sent += 1
            self._cities_seen.add(city)
            self._city_counts[city] += 1
            now = time.monotonic()
            self._sent_times.append(now)
            self._evict_old(self._sent_times, now)

    def record_message_received(self, city: str, latency_ms: float = None):
        with self._lock:
            self._messages_recv += 1
            now = time.monotonic()
            self._recv_times.append(now)
            self._evict_old(self._recv_times, now)
            if latency_ms is not None:
                self._latencies_ms.append(latency_ms)
                if len(self._latencies_ms) > 1000:
                    self._latencies_ms.popleft()

    def record_error(self, error_type: str, city: str = None):
        with self._lock:
            self._messages_failed += 1
            self._error_types[error_type] += 1
            if city:
                self._city_errors[city] += 1

    def record_dlq(self, reason: str):
        with self._lock:
            self._dlq_count += 1
            self._error_types[f"dlq.{reason}"] += 1

    def record_anomaly(self, city: str):
        with self._lock:
            self._anomaly_count += 1

    def _evict_old(self, queue: deque, now: float):
        """Remove entries older than the rolling window."""
        cutoff = now - self._window
        while queue and queue[0] < cutoff:
            queue.popleft()

    def get_snapshot(self) -> dict:
        """Return current metrics snapshot as a dict."""
        with self._lock:
            uptime_s   = time.monotonic() - self._start_time
            now        = time.monotonic()
            self._evict_old(self._sent_times, now)
            self._evict_old(self._recv_times, now)

            send_rate  = len(self._sent_times) / self._window
            recv_rate  = len(self._recv_times) / self._window

            lats       = list(self._latencies_ms)
            avg_lat    = round(sum(lats) / len(lats), 1) if lats else None
            p99_lat    = round(sorted(lats)[int(len(lats) * 0.99)], 1) if len(lats) >= 10 else None

            error_rate = (
                self._messages_failed / max(self._messages_sent, 1) * 100
            )

            anomaly_rate = (
                self._anomaly_count / max(self._messages_recv, 1) * 100
            )

            return {
                "uptime_seconds":      round(uptime_s),
                "messages_sent":       self._messages_sent,
                "messages_received":   self._messages_recv,
                "messages_failed":     self._messages_failed,
                "dlq_count":           self._dlq_count,
                "anomaly_count":       self._anomaly_count,
                "cities_active":       len(self._cities_seen),
                "send_rate_per_sec":   round(send_rate, 2),
                "recv_rate_per_sec":   round(recv_rate, 2),
                "avg_latency_ms":      avg_lat,
                "p99_latency_ms":      p99_lat,
                "error_rate_pct":      round(error_rate, 2),
                "anomaly_rate_pct":    round(anomaly_rate, 2),
                "error_types":         dict(self._error_types),
                "top_cities":          dict(
                    sorted(self._city_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                ),
            }

    def log_summary(self, logger: "StructuredLogger"):
        """Log a metrics summary line."""
        snap = self.get_snapshot()
        logger.info("pipeline.metrics.summary", **snap)


# ── Module-level singletons ────────────────────────────────────────────────────
logger  = StructuredLogger(COMPONENT, LOG_LEVEL)
metrics = PipelineMetrics(window_seconds=300)
