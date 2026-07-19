"""
data/stream_simulator.py
=========================
UERIS — Dual-Mode Stream Simulator

Supports two streaming modes selected via STREAMING_MODE env var:

  MODE: simulation (default)
    Writes JSON files to data/streaming_input/.
    Consumed by speed_layer/speed_processing.py (PySpark file polling).
    Works on any machine including Windows without Kafka.

  MODE: kafka
    Publishes to Kafka topic via streaming/producer.py.
    Consumed by speed_layer/spark_kafka_consumer.py (Spark Structured Streaming).
    Requires Kafka broker running (see docker-compose.yml).

Both modes fetch REAL data from:
  - Open-Meteo API  : temperature + humidity (free, no key required)
  - WAQI API        : AQI (free token at https://aqicn.org/api/)
  - Open-Meteo AQ   : AQI fallback if no WAQI token

This file intentionally preserves 100% backward compatibility with
the existing speed_processing.py file-mode consumer.

Run:
  # Simulation mode (default)
  python data/stream_simulator.py

  # Kafka mode
  STREAMING_MODE=kafka python data/stream_simulator.py

Environment variables:
  STREAMING_MODE          'simulation' or 'kafka' (default: simulation)
  WAQI_TOKEN              WAQI API token (default: demo = Open-Meteo AQ fallback)
  STREAM_OUTPUT_DIR       File-mode output folder (default: data/streaming_input)
  FETCH_INTERVAL_SECONDS  Seconds between full city cycles (default: 60)
  ALERT_WEBHOOK_URL       Slack/Teams webhook for anomaly alerts (optional)
  LOG_LEVEL               DEBUG / INFO / WARNING (default: INFO)

  # Kafka-mode only (see streaming/kafka_config.py for full list):
  KAFKA_BROKER            Broker address (default: localhost:9092)
  KAFKA_TOPIC             Topic name (default: ueris.env.readings)

Backward compatibility:
  KAFKA_MODE=1 is still accepted and maps to STREAMING_MODE=kafka.
  KAFKA_BROKER and KAFKA_TOPIC env vars are forwarded unchanged.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests as _requests

# ── Make streaming module importable ───────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from streaming.monitoring import logger, metrics
from streaming.schema import build_reading, ValidationError

# ── Config ─────────────────────────────────────────────────────────────────────
# Support legacy KAFKA_MODE=1 as alias for STREAMING_MODE=kafka
_legacy_kafka = os.environ.get("KAFKA_MODE", "0") == "1"
STREAMING_MODE  = os.environ.get("STREAMING_MODE", "kafka" if _legacy_kafka else "simulation")

WAQI_TOKEN      = os.environ.get("WAQI_TOKEN",            "demo")
OUTPUT_DIR      = os.environ.get("STREAM_OUTPUT_DIR",
                    os.path.join(os.path.dirname(__file__), "streaming_input"))
FETCH_INTERVAL  = int(os.environ.get("FETCH_INTERVAL_SECONDS", "60"))
ALERT_WEBHOOK   = os.environ.get("ALERT_WEBHOOK_URL",     "")

# ── Cities with coordinates ────────────────────────────────────────────────────
CITIES = {
    "Ahmedabad":          {"lat": 23.0225, "lon": 72.5714},
    "Aizawl":             {"lat": 23.7271, "lon": 92.7176},
    "Amaravati":          {"lat": 16.5730, "lon": 80.3582},
    "Amritsar":           {"lat": 31.6340, "lon": 74.8723},
    "Bengaluru":          {"lat": 12.9716, "lon": 77.5946},
    "Bhopal":             {"lat": 23.2599, "lon": 77.4126},
    "Brajrajnagar":       {"lat": 21.8167, "lon": 83.9167},
    "Chandigarh":         {"lat": 30.7333, "lon": 76.7794},
    "Chennai":            {"lat": 13.0827, "lon": 80.2707},
    "Coimbatore":         {"lat": 11.0168, "lon": 76.9558},
    "Delhi":              {"lat": 28.6139, "lon": 77.2090},
    "Ernakulam":          {"lat":  9.9816, "lon": 76.2999},
    "Gurugram":           {"lat": 28.4595, "lon": 77.0266},
    "Guwahati":           {"lat": 26.1445, "lon": 91.7362},
    "Hyderabad":          {"lat": 17.3850, "lon": 78.4867},
    "Jaipur":             {"lat": 26.9124, "lon": 75.7873},
    "Jorapokhar":         {"lat": 23.6800, "lon": 86.4200},
    "Kochi":              {"lat":  9.9312, "lon": 76.2673},
    "Kolkata":            {"lat": 22.5726, "lon": 88.3639},
    "Lucknow":            {"lat": 26.8467, "lon": 80.9462},
    "Mumbai":             {"lat": 19.0760, "lon": 72.8777},
    "Patna":              {"lat": 25.5941, "lon": 85.1376},
    "Shillong":           {"lat": 25.5788, "lon": 91.8933},
    "Talcher":            {"lat": 20.9500, "lon": 85.2333},
    "Thiruvananthapuram": {"lat":  8.5241, "lon": 76.9366},
    "Visakhapatnam":      {"lat": 17.6868, "lon": 83.2185},
}

# ── API fetch helpers (unchanged from original) ────────────────────────────────

def fetch_weather(lat: float, lon: float) -> tuple:
    """Fetch temperature + humidity from Open-Meteo (no API key needed)."""
    try:
        t0  = time.monotonic()
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&current=temperature_2m,relative_humidity_2m"
            f"&timezone=Asia/Kolkata"
        )
        r   = _requests.get(url, timeout=10)
        c   = r.json()["current"]
        ms  = round((time.monotonic() - t0) * 1000)
        return float(c["temperature_2m"]), float(c["relative_humidity_2m"]), ms
    except Exception as e:
        logger.warning("simulator.weather.fetch.failed", error=str(e))
        return None, None, None


def fetch_aqi(city: str, lat: float, lon: float) -> tuple:
    """
    Fetch AQI from WAQI (primary) or Open-Meteo AQ (fallback).
    Returns (aqi, source_name).
    """
    # Primary: WAQI token
    if WAQI_TOKEN and WAQI_TOKEN != "demo":
        try:
            url = f"https://api.waqi.info/feed/geo:{lat};{lon}/?token={WAQI_TOKEN}"
            r   = _requests.get(url, timeout=10)
            d   = r.json()
            if d.get("status") == "ok":
                return float(d["data"]["aqi"]), "WAQI"
        except Exception as e:
            logger.warning("simulator.waqi.failed", city=city, error=str(e))

    # Fallback: Open-Meteo Air Quality
    try:
        url = (
            f"https://air-quality-api.open-meteo.com/v1/air-quality"
            f"?latitude={lat}&longitude={lon}"
            f"&current=us_aqi&timezone=Asia/Kolkata"
        )
        r   = _requests.get(url, timeout=10)
        aqi = r.json().get("current", {}).get("us_aqi")
        if aqi is not None:
            return float(aqi), "Open-Meteo-AQ"
    except Exception as e:
        logger.warning("simulator.openmeteo_aqi.failed", city=city, error=str(e))

    return None, None


def send_alert_webhook(city: str, aqi: float, usi: float, risk: str):
    """Post anomaly alert to Slack/Teams webhook."""
    if not ALERT_WEBHOOK:
        return
    try:
        payload = {
            "text": (
                f"*UERIS ANOMALY ALERT*\n"
                f"City: {city} | AQI: {aqi} | USI: {usi} | Risk: {risk}\n"
                f"Time: {datetime.now(timezone.utc).isoformat()}"
            )
        }
        _requests.post(ALERT_WEBHOOK, json=payload, timeout=5)
    except Exception:
        pass


# ── Mode implementations ───────────────────────────────────────────────────────

class SimulationMode:
    """
    File-based simulation mode.
    Writes one JSON file per reading to STREAM_OUTPUT_DIR.
    Consumed by speed_layer/speed_processing.py.
    Fully backward compatible with original implementation.
    """

    def __init__(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        # Clean stale files from previous run
        cleaned = 0
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(".json"):
                os.remove(os.path.join(OUTPUT_DIR, f))
                cleaned += 1
        if cleaned:
            logger.info("simulator.simulation.cleaned_stale", count=cleaned)
        self._counter = 0

    def publish(self, record: dict) -> bool:
        """Write record as JSON file."""
        try:
            fname = os.path.join(OUTPUT_DIR, f"stream_{self._counter:06d}.json")
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False)
            self._counter += 1
            return True
        except Exception as e:
            logger.error("simulator.file.write.failed", error=str(e))
            return False

    def flush(self):
        pass   # no-op for file mode

    def close(self):
        pass


class KafkaMode:
    """
    Kafka producer mode.
    Publishes messages via streaming/producer.py with full retry + DLQ.
    Consumed by speed_layer/spark_kafka_consumer.py.
    """

    def __init__(self):
        from streaming.producer import UERISProducer
        print(
            f"[SIMULATOR] Initialising Kafka mode | "
            f"broker={os.environ.get('KAFKA_BROKER','localhost:9092')} "
            f"topic={os.environ.get('KAFKA_TOPIC','ueris.env.readings')}",
            flush=True
        )
        self._producer = UERISProducer()
        print("[SIMULATOR] Kafka producer ready", flush=True)

    def publish(self, record: dict) -> bool:
        """Send record to Kafka via UERISProducer."""
        result = self._producer.send(
            city=record["city"],
            aqi=record["aqi"],
            temperature=record["temperature"],
            humidity=record["humidity"],
            lat=record.get("lat"),
            lon=record.get("lon"),
            source=record.get("source", "simulator"),
            data_source=record.get("data_source", "Open-Meteo+WAQI"),
            fetch_duration_ms=record.get("fetch_duration_ms"),
        )
        if not result:
            print(
                f"[SIMULATOR ERROR] publish returned False for city={record['city']}",
                flush=True
            )
        return result

    def flush(self):
        print("[SIMULATOR] flushing producer...", flush=True)
        self._producer.flush()
        print("[SIMULATOR] flush complete", flush=True)

    def close(self):
        self._producer.close()


# ── Main loop ──────────────────────────────────────────────────────────────────

def run():
    """Main simulation loop — runs forever until KeyboardInterrupt."""

    # Initialise the selected mode
    if STREAMING_MODE == "kafka":
        try:
            backend = KafkaMode()
            mode_label = "Kafka"
        except Exception as e:
            logger.error(
                "simulator.kafka.init.failed",
                error=str(e),
                fallback="simulation"
            )
            backend    = SimulationMode()
            mode_label = "Simulation (Kafka init failed)"
    else:
        backend    = SimulationMode()
        mode_label = "Simulation (file-based)"

    logger.info(
        "simulator.started",
        mode=mode_label,
        cities=len(CITIES),
        interval_s=FETCH_INTERVAL,
        waqi=("token" if WAQI_TOKEN != "demo" else "Open-Meteo fallback"),
        webhook=bool(ALERT_WEBHOOK),
    )

    print(f"\n{'='*60}")
    print(f"  UERIS Stream Simulator")
    print(f"  Mode    : {mode_label}")
    print(f"  Cities  : {len(CITIES)}")
    print(f"  Interval: {FETCH_INTERVAL}s")
    print(f"  AQI src : {'WAQI token' if WAQI_TOKEN != 'demo' else 'Open-Meteo AQ'}")
    print(f"  Alerts  : {'Enabled' if ALERT_WEBHOOK else 'Disabled'}")
    print(f"{'='*60}\n")

    cycle = 0
    try:
        while True:
            cycle     += 1
            t_cycle    = time.monotonic()
            now_ts     = datetime.now(timezone.utc)
            success    = 0
            anomalies  = 0

            print(f"[{now_ts.strftime('%H:%M:%S')}] Cycle {cycle} — fetching {len(CITIES)} cities...")

            for city, coords in CITIES.items():
                lat, lon = coords["lat"], coords["lon"]

                # Fetch weather
                temp, humidity, weather_ms = fetch_weather(lat, lon)
                if temp is None:
                    metrics.record_error("WeatherFetchFailed", city=city)
                    print(f"  {city:<24} | weather fetch failed — skipping")
                    continue

                # Fetch AQI
                aqi, aqi_source = fetch_aqi(city, lat, lon)
                if aqi is None:
                    metrics.record_error("AQIFetchFailed", city=city)
                    print(f"  {city:<24} | AQI fetch failed — skipping")
                    continue

                # Build record using schema builder (validates automatically)
                try:
                    record = build_reading(
                        city=city,
                        aqi=aqi,
                        temperature=temp,
                        humidity=humidity,
                        lat=lat,
                        lon=lon,
                        source="simulator",
                        data_source=f"Open-Meteo+{aqi_source}",
                        fetch_duration_ms=weather_ms,
                    )
                except ValidationError as e:
                    logger.error("simulator.validation.failed", city=city, error=str(e))
                    metrics.record_error("ValidationError", city=city)
                    continue

                # Publish
                ok = backend.publish(record)
                if ok:
                    success += 1
                    metrics.record_message_sent(city)

                    # Anomaly alert (threshold-based at simulator level)
                    if aqi > 200:
                        anomalies += 1
                        send_alert_webhook(city, aqi, 0.0, "High")

                    flag = "  ⚠ HIGH POLLUTION" if aqi > 200 else ""
                    print(
                        f"  {city:<24} | AQI={aqi:6.1f} | "
                        f"Temp={temp:5.1f}°C | Hum={humidity:4.0f}% | "
                        f"{aqi_source:<15}{flag}"
                    )
                else:
                    print(f"  {city:<24} | publish failed (see logs)")

                time.sleep(0.5)   # avoid API rate limits

            # Flush after each full cycle
            backend.flush()

            elapsed = round(time.monotonic() - t_cycle)
            logger.info(
                "simulator.cycle.complete",
                cycle=cycle,
                success=success,
                total=len(CITIES),
                anomalies=anomalies,
                elapsed_s=elapsed,
            )
            print(f"\n  Cycle {cycle} complete: {success}/{len(CITIES)} cities, "
                  f"{anomalies} anomalies, {elapsed}s elapsed. "
                  f"Next in {FETCH_INTERVAL}s...\n")

            time.sleep(FETCH_INTERVAL)

    except KeyboardInterrupt:
        logger.info("simulator.interrupted", cycle=cycle)
        print("\n  Simulator stopped by user.")
    finally:
        backend.flush()
        backend.close()
        metrics.log_summary(logger)


if __name__ == "__main__":
    run()
