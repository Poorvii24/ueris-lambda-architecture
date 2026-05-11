# UERIS — Urban Environmental Risk Intelligence System

> Production-grade **Lambda Architecture** for real-time air quality monitoring across 26 Indian cities, with ML anomaly detection, forecasting, and correlation analysis.

---

Live demo: https://ueris-dashboard.onrender.com/
## Architecture

```
╔══════════════════════════════════════════════════════════════════╗
║                      LAMBDA ARCHITECTURE                        ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  BATCH LAYER      [Kaggle CSV] → PySpark → batch_views          ║
║  (Accuracy)        • USI computation for all records            ║
║                    • Isolation Forest model per city            ║
║                    • Pearson correlation matrix                  ║
║                    • 12-month USI forecast                       ║
║                    • City health ranking                         ║
║                              ↘                                   ║
║  SERVING LAYER    Flask REST API → Lambda Merge → Dashboard      ║
║  (Query)           realtime-priority | batch-fallback            ║
║                              ↗                                   ║
║  SPEED LAYER      [Open-Meteo+WAQI] → Stream Sim → PySpark      ║
║  (Freshness)       • Loads batch-trained IF model per city      ║
║                    • ML anomaly scoring on live readings         ║
║                    • Alert webhooks on anomaly detection         ║
╚══════════════════════════════════════════════════════════════════╝
```

---

## Tech Stack

| Component        | Technology                                      |
|-----------------|-------------------------------------------------|
| Batch processing | Apache PySpark (local mode)                    |
| ML / Analytics   | scikit-learn Isolation Forest, Pearson corr.   |
| Speed layer      | PySpark micro-batch + file simulation           |
| Storage          | MongoDB (batch_views + realtime_views)          |
| Serving layer    | Flask + Flask-CORS                              |
| Dashboard        | Vanilla JS + Chart.js 4.4                       |
| Historical data  | Kaggle — Air Quality Data in India (2015–2020) |
| Live weather     | Open-Meteo API (free, no key)                  |
| Live AQI         | WAQI API (free token) + Open-Meteo AQ fallback |
| Containerisation | Docker + Docker Compose                         |
| Tests            | pytest (unit + integration)                     |

---

## USI Formula

```
USI = ( 0.5 × AQI_norm  +  0.3 × Temp_norm  +  0.2 × Hum_norm ) × 100

  AQI_norm  = min(AQI / 300, 1.0)           WHO hazardous threshold
  Temp_norm = clamp((T − 15) / 25, 0, 1)    15°C baseline; 40°C = max stress
  Hum_norm  = |H − 50| / 50                 50% RH = comfort midpoint

Weights: 50% AQI (WHO 2021) · 30% Temp (Lancet 2021) · 20% Humidity
Risk bands: Low(<20) · Moderate(20-39) · High(40-59) · Very High(60-79) · Severe(≥80)
```

---

## ML Features

| Feature | Method | Where |
|---|---|---|
| Anomaly detection | Isolation Forest (per city, contamination=0.05) | Batch trains → Speed applies |
| Forecasting | Linear trend + seasonal blending (12 months) | Batch layer |
| Correlation | Pearson r matrix (AQI × Temp × Humidity × USI) | Batch layer |
| Health ranking | Normalised inverse-USI score (0–100) | Batch layer |

---

## Project Structure

```
urban-env-risk/
├── data/
│   ├── historical/city_day.csv      ← Kaggle dataset (download separately)
│   ├── streaming_input/             ← Speed layer input (auto-created)
│   └── checkpoint/                  ← PySpark checkpoint (auto-created)
├── batch_layer/
│   └── batch_processing.py          ← STEP 1: Run once
├── data/
│   └── stream_simulator.py          ← STEP 2: Run continuously
├── speed_layer/
│   └── speed_processing.py          ← STEP 3: Run continuously
├── serving_layer/
│   └── app.py                       ← STEP 4: Run continuously
├── dashboard/
│   └── index.html                   ← 9-tab dashboard
├── tests/
│   └── test_ueris.py                ← pytest unit + integration tests
├── docker-compose.yml               ← One-command deployment
├── Dockerfile
└── requirements.txt
```

---

## Setup (Manual)

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Download Kaggle dataset

https://www.kaggle.com/datasets/rohanrao/air-quality-data-in-india

Place at: `data/historical/city_day.csv`

### 3. (Optional) WAQI token

```bash
set WAQI_TOKEN=your_token     # Windows
export WAQI_TOKEN=your_token  # Linux/Mac
```

Free token at https://aqicn.org/api/

---

## Running (4 terminals)

```bash
# Terminal 1 — once, wait for completion (~2-3 min)
python batch_layer/batch_processing.py

# Terminal 2 — keep running
python data/stream_simulator.py

# Terminal 3 — keep running
python speed_layer/speed_processing.py

# Terminal 4 — keep running, then open browser
python serving_layer/app.py
# → http://localhost:5000
```

---

## Docker (one command)

```bash
# Place city_day.csv in data/historical/ first
docker-compose up
# → http://localhost:5000
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Layer status + document counts |
| GET | `/api/cities` | All 26 cities — Lambda merged view |
| GET | `/api/city/<n>` | Single city: batch + realtime + forecast + corr |
| GET | `/api/realtime` | Raw speed-layer readings |
| GET | `/api/ranking` | Health leaderboard sorted by score |
| GET | `/api/forecast/<n>` | 12-month USI forecast with bands |
| GET | `/api/correlation/<n>` | Pearson correlation matrix |
| GET | `/api/quality` | Data quality report |
| GET | `/api/export/csv` | Download all stats as CSV |
| GET | `/api/architecture` | System metadata |

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017/` | MongoDB connection |
| `DB_NAME` | `urban_env_db` | Database name |
| `WAQI_TOKEN` | `demo` | WAQI API token |
| `HISTORICAL_CSV` | `data/historical/city_day.csv` | Kaggle dataset path |
| `STREAM_OUTPUT_DIR` | `data/streaming_input/` | Speed layer input dir |
| `FETCH_INTERVAL_SECONDS` | `60` | Stream simulator interval |
| `POLL_INTERVAL_SECONDS` | `5` | Speed layer poll interval |
| `FRESHNESS_WINDOW_MIN` | `30` | Realtime staleness cutoff |
| `ALERT_WEBHOOK_URL` | `` | Slack/Teams webhook URL |
| `KAFKA_MODE` | `0` | Set `1` to use Kafka instead of files |
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `ueris-stream` | Kafka topic name |
| `API_KEY` | `` | Optional API key for /api/* auth |
| `PORT` | `5000` | Flask port |
| `PYSPARK_PYTHON` | Current Python | PySpark worker Python |

---

## Tests

```bash
# Unit tests only (no server needed)
pytest tests/test_ueris.py -v -k "not integration"

# All tests (requires app.py running)
pytest tests/test_ueris.py -v
```

---

## Dashboard Tabs

| Tab | What it shows |
|---|---|
| 📊 Overview | Live city cards + detail panel with Lambda merge info |
| 🏆 Ranking | Health leaderboard with score bars |
| 🔮 Forecast | 12-month USI forecast charts per city |
| ⚖️ Compare | Side-by-side bar/radar/stacked charts |
| 📈 Trends | Historical AQI vs USI per city (2015–2020) |
| 🔗 Correlation | Pearson heatmaps — AQI × Temp × Humidity × USI |
| 🚨 Alerts | ML anomaly log with detection method |
| ✅ Data Quality | Coverage, freshness, model status per city |
| 🏗️ Architecture | System diagram, USI formula, live layer status |

---

## Cities Covered (26)

Ahmedabad · Aizawl · Amaravati · Amritsar · Bengaluru · Bhopal · Brajrajnagar ·
Chandigarh · Chennai · Coimbatore · Delhi · Ernakulam · Gurugram · Guwahati ·
Hyderabad · Jaipur · Jorapokhar · Kochi · Kolkata · Lucknow · Mumbai · Patna ·
Shillong · Talcher · Thiruvananthapuram · Visakhapatnam

---

## Notes

- PySpark runs in `local[*]` mode. For a cluster: `.master("spark://host:7077")`
- Kafka mode: install `confluent-kafka` and set `KAFKA_MODE=1`
- Speed layer uses file-based micro-batching by default (Windows-compatible Kafka simulation)
- Isolation Forest trained per city (not globally) so anomaly baselines are city-specific
