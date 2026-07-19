# UERIS — Urban Environmental Risk Intelligence System

> Enterprise-grade Environmental Intelligence Platform built on Lambda Architecture,
> Apache Kafka, Apache Spark, MongoDB Atlas, and Flask.

🌐 **Live Demo**: [ueris-dashboard.onrender.com](https://ueris-dashboard.onrender.com)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    UERIS LAMBDA ARCHITECTURE                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   DATA SOURCES                                                          │
│   ┌──────────────────┐    ┌──────────────────────────────────────┐    │
│   │ Kaggle Dataset   │    │ Live APIs                            │    │
│   │ city_day.csv     │    │ Open-Meteo (temp/humidity)           │    │
│   │ 24,850 records   │    │ WAQI (AQI)                           │    │
│   │ 26 Indian cities │    │ Open-Meteo AQ (AQI fallback)         │    │
│   └────────┬─────────┘    └──────────────┬───────────────────────┘    │
│            │                             │                             │
│   ─────────────────────── BATCH PATH ──────────────────────────────   │
│            │                                                           │
│   ┌────────▼─────────────────────────────────────────────────────┐    │
│   │  batch_layer/batch_processing.py                             │    │
│   │  PySpark (local) · Runs once (or nightly)                    │    │
│   │  • USI computation per city per day                          │    │
│   │  • Isolation Forest per city (trained on 2015-2020)          │    │
│   │  • Pearson correlation matrices                               │    │
│   │  • 12-month USI forecasts (seasonal + linear)                │    │
│   │  • City health rankings                                       │    │
│   │  • Monthly/yearly trend aggregations                          │    │
│   └────────────────────────┬─────────────────────────────────────┘    │
│                            │                                           │
│   ─────────────────────── SPEED PATH ──────────────────────────────   │
│                            │                                           │
│   ┌──────────────────┐     │                                          │
│   │ stream_simulator │     │                                          │
│   │ data/stream_     │     │                                          │
│   │ simulator.py     │     │                                          │
│   │                  │     │                                          │
│   │ SIMULATION MODE  │     │                                          │
│   │ writes .json     │     │                                          │
│   │ files            │     │                                          │
│   │                  │     │                                          │
│   │ KAFKA MODE       │     │                                          │
│   │ publishes to     │     │                                          │
│   │ Kafka topic      │     │                                          │
│   └──────┬──────┬────┘     │                                          │
│          │      │          │                                           │
│    SIM   │      │ KAFKA    │                                           │
│    MODE  │      │  MODE    │                                           │
│          │      │          │                                           │
│   ┌──────▼──┐ ┌─▼──────────────────────┐                             │
│   │ speed_  │ │ Apache Kafka           │                             │
│   │ process-│ │ Topic: ueris.env.      │                             │
│   │ ing.py  │ │ readings               │                             │
│   │ PySpark │ │ DLQ:   ueris.env.dlq  │                             │
│   │ file    │ └─────────┬──────────────┘                             │
│   │ polling │           │                                             │
│   └────┬────┘ ┌─────────▼──────────────┐                             │
│        │      │ spark_kafka_consumer.py│                             │
│        │      │ Spark Structured       │                             │
│        │      │ Streaming              │                             │
│        │      │ + Isolation Forest     │                             │
│        │      └─────────┬──────────────┘                             │
│        │                │                                             │
│   ─────────────────── SERVING PATH ────────────────────────────────  │
│        │                │                                             │
│   ┌────▼────────────────▼──────────────────────┐                     │
│   │          MongoDB Atlas                      │                     │
│   │  batch_views    (26 docs — historical)      │                     │
│   │  realtime_views (26 docs — live)            │                     │
│   │  correlations   (26 docs — Pearson matrix)  │                     │
│   │  data_quality   (26 docs — coverage stats)  │                     │
│   └───────────────────────┬─────────────────────┘                     │
│                           │                                            │
│   ┌───────────────────────▼─────────────────────┐                     │
│   │  serving_layer/app.py (Flask + Gunicorn)    │                     │
│   │  11 REST API endpoints                       │                     │
│   │  Background LiveWorker thread (Render)       │                     │
│   │  Lambda merge: realtime ?? batch fallback    │                     │
│   └───────────────────────┬─────────────────────┘                     │
│                           │                                            │
│   ┌───────────────────────▼─────────────────────┐                     │
│   │  dashboard/index.html                        │                     │
│   │  9 tabs: Overview · Ranking · Forecast ·    │                     │
│   │  Compare · Trends · Correlation · Alerts ·  │                     │
│   │  Data Quality · Architecture                 │                     │
│   └─────────────────────────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Enterprise Streaming Architecture (Phase 1)

```
┌─────────────────────────────────────────────────────────────┐
│              STREAMING ENGINE — DUAL MODE                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  SIMULATION MODE (default, STREAMING_MODE=simulation)       │
│                                                             │
│  Open-Meteo + WAQI                                          │
│       ↓                                                     │
│  stream_simulator.py → streaming/schema.py (validate)       │
│       ↓                                                     │
│  data/streaming_input/*.json (one file per reading)         │
│       ↓ (polled every 5s)                                   │
│  speed_processing.py → PySpark createDataFrame              │
│       ↓                                                     │
│  USI recompute + Isolation Forest                           │
│       ↓                                                     │
│  MongoDB realtime_views (upsert, 3x retry)                  │
│       ↓ (failed records)                                    │
│  streaming/dlq_handler.py → data/dlq/*.jsonl                │
│                                                             │
│  KAFKA MODE (STREAMING_MODE=kafka)                          │
│                                                             │
│  Open-Meteo + WAQI                                          │
│       ↓                                                     │
│  stream_simulator.py → streaming/producer.py                │
│       ↓ (5x retry + exponential backoff)                    │
│  Apache Kafka → ueris.env.readings (3 partitions)           │
│       ↓ (failed after retries)                              │
│  ueris.env.dlq + data/dlq/*.jsonl                           │
│       ↓                                                     │
│  spark_kafka_consumer.py → Spark readStream from Kafka      │
│       ↓                                                     │
│  USI recompute (Spark SQL) + Isolation Forest               │
│       ↓                                                     │
│  MongoDB realtime_views (upsert)                            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
urban_env_risk/
│
├── batch_layer/
│   └── batch_processing.py          # PySpark batch layer (run locally)
│
├── speed_layer/
│   ├── speed_processing.py          # PySpark file consumer (simulation mode)
│   └── spark_kafka_consumer.py      # Spark Structured Streaming (kafka mode)
│
├── streaming/                       # NEW: Enterprise streaming engine
│   ├── __init__.py
│   ├── kafka_config.py              # All Kafka settings from env vars
│   ├── schema.py                    # Message schema + validator
│   ├── producer.py                  # Kafka producer (retry + DLQ)
│   ├── consumer.py                  # Kafka consumer (anomaly + MongoDB)
│   ├── dlq_handler.py               # Dead Letter Queue (Kafka + file)
│   └── monitoring.py                # Structured logging + metrics
│
├── serving_layer/
│   └── app.py                       # Flask REST API + LiveWorker thread
│
├── data/
│   ├── stream_simulator.py          # Dual-mode simulator (simulation/kafka)
│   ├── generate_historical.py       # One-time synthetic data generator
│   ├── historical/city_day.csv      # Real Kaggle dataset (26 cities)
│   ├── streaming_input/             # Simulation mode: JSON files land here
│   ├── checkpoint/                  # Simulation mode: processed file list
│   ├── checkpoint_kafka/            # Kafka mode: Spark checkpoint
│   └── dlq/                         # Dead Letter Queue files (.jsonl)
│
├── dashboard/
│   └── index.html                   # 9-tab SPA (Chart.js, no build step)
│
├── tests/
│   └── test_ueris.py                # pytest suite (unit + integration)
│
├── compute_atlas_collections.py     # One-time: push correlations/quality to Atlas
├── docker-compose.yml               # Full stack: Kafka + MongoDB + App
├── Dockerfile                       # Multi-stage: slim or with Spark
├── requirements.txt                 # Production dependencies
├── requirements-local.txt           # Local dev: PySpark + Kafka
├── .env.example                     # Environment variable template
└── README.md
```

---

## Quick Start

### Option A — Simulation Mode (no Kafka, easiest)

```bash
# 1. Install dependencies
pip install -r requirements.txt -r requirements-local.txt

# 2. Copy env file
cp .env.example .env
# Edit .env: set MONGO_URI and WAQI_TOKEN

# 3. Run batch layer once
python batch_layer/batch_processing.py

# 4. Start simulator (Terminal 1)
python data/stream_simulator.py

# 5. Start speed layer (Terminal 2)
python speed_layer/speed_processing.py

# 6. Start serving layer (Terminal 3)
python serving_layer/app.py

# 7. Open browser
open http://localhost:5000
```

### Option B — Kafka Mode (full enterprise stack)

```bash
# 1. Copy env file and set STREAMING_MODE
cp .env.example .env
# Set: STREAMING_MODE=kafka, MONGO_URI, WAQI_TOKEN

# 2. Start full stack with Docker Compose
docker-compose up --build

# 3. Monitor Kafka at http://localhost:8080
# 4. View dashboard at http://localhost:5000
# 5. Check API health at http://localhost:5000/api/health
```

### Option C — Simulation mode via Docker Compose

```bash
docker-compose --profile simulation up --build
```

---

## Streaming Mode Selection

| Environment Variable | Value | Effect |
|---------------------|-------|--------|
| `STREAMING_MODE` | `simulation` (default) | File-based, no Kafka needed |
| `STREAMING_MODE` | `kafka` | Enterprise Kafka pipeline |
| `KAFKA_MODE` | `1` | Legacy alias for `STREAMING_MODE=kafka` |

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/cities` | GET | All 26 cities, merged batch+realtime |
| `/api/city/<name>` | GET | Full detail: stats, trends, realtime, forecast, correlation |
| `/api/ranking` | GET | Cities ranked by historical avg USI |
| `/api/forecast/<name>` | GET | 12-month USI forecast with confidence bands |
| `/api/correlation/<name>` | GET | Pearson 4×4 matrix [AQI, Temp, Hum, USI] |
| `/api/quality` | GET | Pipeline data quality: coverage, freshness, model status |
| `/api/health` | GET | All pipeline components status |
| `/api/realtime` | GET | Raw speed layer readings |
| `/api/export/csv` | GET | Download all city data as CSV |

---

## Urban Stress Index (USI) Formula

```
USI = (0.5 × AQI_norm) + (0.3 × Temp_norm) + (0.2 × Hum_deviation)

Where:
  AQI_norm      = min(AQI / 300, 1.0)
  Temp_norm     = clamp((Temperature - 15) / 25, 0, 1)
  Hum_deviation = |Humidity - 50| / 50

Range: 0 (ideal) → 100 (severe stress)
```

| USI | Risk Level |
|-----|-----------|
| 0–20 | Low |
| 20–40 | Moderate |
| 40–60 | High |
| 60–80 | Very High |
| 80–100 | Severe |

---

## Testing

```bash
# Unit tests only (no app/Kafka required)
pytest tests/test_ueris.py -v -k "not integration"

# All tests including API integration (requires app on port 5000)
pytest tests/test_ueris.py -v

# Specific test class
pytest tests/test_ueris.py::TestSchema -v
pytest tests/test_ueris.py::TestKafkaConfig -v
pytest tests/test_ueris.py::TestDLQHandler -v

# With coverage
pytest tests/test_ueris.py -v --cov=streaming --cov-report=term-missing
```

---

## Deployment (Render)

1. Push to GitHub
2. Connect repo to Render (Web Service)
3. Set environment variables in Render dashboard:
   - `MONGO_URI` — MongoDB Atlas connection string
   - `WAQI_TOKEN` — WAQI API token
   - `PORT` — 5000
4. Render auto-deploys on push
5. LiveWorker background thread fetches live data every 60s

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Batch Processing | Apache PySpark 3.5 (local mode) |
| Stream Producer | Python + confluent-kafka |
| Stream Broker | Apache Kafka 7.6 (Confluent image) |
| Stream Consumer | Spark Structured Streaming |
| Simulation Mode | PySpark file polling |
| Anomaly Detection | scikit-learn Isolation Forest |
| Database | MongoDB Atlas |
| REST API | Flask 3.0 + Gunicorn |
| Dashboard | HTML + Chart.js (no build step) |
| Deployment | Render + Docker |
| Monitoring | Structured JSON logging |
