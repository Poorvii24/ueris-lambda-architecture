"""
data/stream_simulator.py
=========================
SPEED LAYER DATA SOURCE — Real-time data for all 26 Indian cities

Data sources:
  - Open-Meteo API  : temperature + humidity (free, no key)
  - WAQI API        : real AQI (free token at https://aqicn.org/api/)
  - Open-Meteo AQ   : fallback AQI if no WAQI token

Architecture note:
  Writes one JSON file per city per cycle into data/streaming_input/.
  This simulates a Kafka producer in a Windows-compatible way.
  Set KAFKA_MODE=1 to switch to actual Kafka publishing (requires
  confluent-kafka installed and a local broker).

Alert webhooks:
  Set ALERT_WEBHOOK_URL=https://hooks.slack.com/... to post anomaly
  alerts to Slack (or any webhook endpoint).

How to run:
  python data/stream_simulator.py

Environment variables:
  WAQI_TOKEN          WAQI API token (default: demo = Open-Meteo fallback)
  STREAM_OUTPUT_DIR   Output folder (default: data/streaming_input)
  FETCH_INTERVAL_SECONDS  Seconds between full city fetch cycles (default: 60)
  ALERT_WEBHOOK_URL   Optional Slack/Teams webhook for anomaly alerts
  KAFKA_MODE          Set to 1 to publish to Kafka instead of files
  KAFKA_BROKER        Kafka broker address (default: localhost:9092)
  KAFKA_TOPIC         Kafka topic name (default: ueris-stream)
"""

import json, os, time, requests, smtplib
from datetime import datetime
from email.mime.text import MIMEText

WAQI_TOKEN          = os.environ.get("WAQI_TOKEN", "demo")
OUTPUT_DIR          = os.environ.get("STREAM_OUTPUT_DIR",
                        os.path.join(os.path.dirname(__file__), "streaming_input"))
FETCH_INTERVAL      = int(os.environ.get("FETCH_INTERVAL_SECONDS", "60"))
ALERT_WEBHOOK_URL   = os.environ.get("ALERT_WEBHOOK_URL", "")
KAFKA_MODE          = os.environ.get("KAFKA_MODE", "0") == "1"
KAFKA_BROKER        = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC         = os.environ.get("KAFKA_TOPIC", "ueris-stream")

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

os.makedirs(OUTPUT_DIR, exist_ok=True)
for f in os.listdir(OUTPUT_DIR):
    if f.endswith(".json"):
        os.remove(os.path.join(OUTPUT_DIR, f))

# Kafka producer (only if KAFKA_MODE=1)
kafka_producer = None
if KAFKA_MODE:
    try:
        from confluent_kafka import Producer
        kafka_producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        print(f"  Kafka mode enabled — broker: {KAFKA_BROKER}, topic: {KAFKA_TOPIC}")
    except ImportError:
        print("  WARNING: confluent-kafka not installed. Falling back to file mode.")
        KAFKA_MODE = False


def compute_usi(aqi, temperature, humidity):
    aqi_norm  = min(aqi / 300.0, 1.0)
    temp_norm = min(max((temperature - 15.0) / 25.0, 0.0), 1.0)
    hum_norm  = abs(humidity - 50.0) / 50.0
    return round((0.5 * aqi_norm + 0.3 * temp_norm + 0.2 * hum_norm) * 100.0, 2)


def classify_risk(usi):
    if usi < 20:  return "Low"
    if usi < 40:  return "Moderate"
    if usi < 60:  return "High"
    if usi < 80:  return "Very High"
    return "Severe"


def fetch_weather(lat, lon):
    try:
        url = (f"https://api.open-meteo.com/v1/forecast"
               f"?latitude={lat}&longitude={lon}"
               f"&current=temperature_2m,relative_humidity_2m"
               f"&timezone=Asia/Kolkata")
        r = requests.get(url, timeout=10)
        c = r.json()["current"]
        return c["temperature_2m"], c["relative_humidity_2m"]
    except Exception:
        return None, None


def fetch_aqi(city, lat, lon):
    if WAQI_TOKEN and WAQI_TOKEN != "demo":
        try:
            url = f"https://api.waqi.info/feed/geo:{lat};{lon}/?token={WAQI_TOKEN}"
            r = requests.get(url, timeout=10)
            d = r.json()
            if d.get("status") == "ok":
                return float(d["data"]["aqi"])
        except Exception:
            pass
    try:
        url = (f"https://air-quality-api.open-meteo.com/v1/air-quality"
               f"?latitude={lat}&longitude={lon}"
               f"&current=us_aqi&timezone=Asia/Kolkata")
        r = requests.get(url, timeout=10)
        aqi = r.json().get("current", {}).get("us_aqi")
        if aqi is not None:
            return float(aqi)
    except Exception:
        pass
    return None


def send_alert_webhook(record):
    """Send anomaly alert to Slack/Teams/generic webhook."""
    if not ALERT_WEBHOOK_URL:
        return
    try:
        payload = {
            "text": (
                f"*UERIS ANOMALY ALERT*\n"
                f"City: {record['city']}\n"
                f"AQI: {record['aqi']} | USI: {record['usi']} | Risk: {record['risk_level']}\n"
                f"Time: {record['timestamp']}"
            )
        }
        requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=5)
    except Exception:
        pass


def publish_record(record, file_counter):
    """Publish to Kafka or write to file."""
    if KAFKA_MODE and kafka_producer:
        kafka_producer.produce(
            KAFKA_TOPIC,
            key=record["city"],
            value=json.dumps(record)
        )
        kafka_producer.poll(0)
    else:
        fname = os.path.join(OUTPUT_DIR, f"stream_{file_counter:06d}.json")
        with open(fname, "w") as f:
            json.dump(record, f)


print("\n" + "="*60)
print(f"  STREAM SIMULATOR — {len(CITIES)} Indian Cities")
print(f"  Mode    : {'Kafka' if KAFKA_MODE else 'File-based (Kafka simulation)'}")
print(f"  AQI src : {'WAQI token' if WAQI_TOKEN != 'demo' else 'Open-Meteo AQ (fallback)'}")
print(f"  Interval: {FETCH_INTERVAL}s")
print(f"  Alerts  : {'Webhook set' if ALERT_WEBHOOK_URL else 'Disabled'}")
print("="*60 + "\n")

file_counter = 0
while True:
    ts = datetime.now()
    print(f"\n[{ts.strftime('%H:%M:%S')}] Fetching {len(CITIES)} cities...")
    success = 0

    for city, coords in CITIES.items():
        lat, lon = coords["lat"], coords["lon"]
        temp, humidity = fetch_weather(lat, lon)
        aqi = fetch_aqi(city, lat, lon)

        if temp is None or aqi is None:
            print(f"  {city:<24} | Could not fetch — skipping")
            continue

        usi  = compute_usi(aqi, temp, humidity)
        risk = classify_risk(usi)
        is_anomaly = aqi > 200

        record = {
            "timestamp":   ts.strftime("%Y-%m-%d %H:%M:%S"),
            "city":        city,
            "aqi":         round(aqi, 1),
            "temperature": round(temp, 1),
            "humidity":    round(float(humidity), 1),
            "usi":         usi,
            "risk_level":  risk,
            "is_anomaly":  is_anomaly,
            "data_source": "Open-Meteo + WAQI"
        }

        publish_record(record, file_counter)

        if is_anomaly:
            send_alert_webhook(record)

        flag = "  ANOMALY!" if is_anomaly else ""
        print(f"  {city:<24} | AQI={aqi:6.1f} | Temp={temp:5.1f}C | "
              f"Hum={humidity:4.0f}% | USI={usi:6.2f} | {risk:<10}{flag}")
        file_counter += 1
        success += 1
        time.sleep(0.5)

    if KAFKA_MODE and kafka_producer:
        kafka_producer.flush()

    print(f"\n  Fetched {success}/{len(CITIES)} cities. Next in {FETCH_INTERVAL}s...")
    time.sleep(FETCH_INTERVAL)
