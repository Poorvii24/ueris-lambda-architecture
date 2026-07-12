"""
serving_layer/app.py
====================
UERIS — Urban Environmental Risk Intelligence System
Serving Layer: Flask REST API

Exposes all endpoints consumed by dashboard/index.html.
All routes are prefixed with /api/.

Environment variables:
    MONGO_URI              MongoDB connection string (default: localhost)
    DB_NAME                Database name (default: urban_env_db)
    PORT                   HTTP port (default: 5000)
    FRESHNESS_WINDOW_MIN   Minutes before realtime data is considered stale (default: 30)

Run locally:
    python serving_layer/app.py

Run in production (gunicorn):
    gunicorn -w 2 -b 0.0.0.0:5000 serving_layer.app:app
"""

import io
import csv
import math
import os
from datetime import datetime, timezone, timedelta

import pymongo
from flask import Flask, jsonify, send_from_directory, Response
from flask_cors import CORS

# ── App Setup ──────────────────────────────────────────────────────────────────
app = Flask(
    __name__,
    static_folder=os.path.join(os.path.dirname(__file__), "../dashboard"),
    static_url_path="",
)
CORS(app)

# ── Config ─────────────────────────────────────────────────────────────────────
MONGO_URI            = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME              = os.environ.get("DB_NAME", "urban_env_db")
FRESHNESS_WINDOW_MIN = int(os.environ.get("FRESHNESS_WINDOW_MIN", 30))

# City name normalisation — Kaggle uses "Bengaluru", live APIs use "Bangalore"
CITY_ALIASES = {
    "Bangalore": "Bengaluru",
    "Bengaluru": "Bengaluru",
}


# ── DB Helper ──────────────────────────────────────────────────────────────────
def get_db():
    """Return (db, client). Caller must close client."""
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    return client[DB_NAME], client


# ── Pure Functions (business logic, testable) ──────────────────────────────────
def is_fresh(updated_at_str: str | None) -> bool:
    """Return True if updated_at is within FRESHNESS_WINDOW_MIN minutes."""
    if not updated_at_str:
        return False
    try:
        updated = datetime.fromisoformat(updated_at_str)
        # Normalise to UTC-aware
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age_min = (datetime.now(timezone.utc) - updated).total_seconds() / 60
        return age_min <= FRESHNESS_WINDOW_MIN
    except Exception:
        return False


def health_score(avg_usi: float | None) -> float | None:
    """Convert avg USI (0-100, higher = worse) to health score (higher = better)."""
    if avg_usi is None:
        return None
    return round(max(0.0, 100.0 - avg_usi), 1)


def compute_usi(aqi: float, temperature: float, humidity: float) -> float:
    """Urban Stress Index — weighted combination of AQI, Temp, Humidity."""
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


def build_rank_map(batch_docs: list) -> dict:
    """Return {city: rank} sorted by avg_usi descending (most polluted = rank 1)."""
    sortable = [(b["city"], b.get("stats", {}).get("avg_usi") or 0) for b in batch_docs]
    sortable.sort(key=lambda x: x[1], reverse=True)
    return {city: i + 1 for i, (city, _) in enumerate(sortable)}


def enrich_city(batch: dict, rt: dict, rank: int) -> dict:
    """
    Merge batch (slow path) and realtime (fast path) into a single city view.
    This is the Lambda Architecture merge logic.
    """
    fresh   = is_fresh(rt.get("updated_at")) if rt else False
    avg_u   = batch.get("stats", {}).get("avg_usi")

    # Lambda merge: realtime if fresh, else fall back to historical avg
    current_usi  = (rt.get("usi")  if fresh else None) or avg_u
    current_aqi  = (rt.get("aqi")  if fresh else None) or batch.get("stats", {}).get("avg_aqi")
    current_temp = (rt.get("temperature") if fresh else None) or batch.get("stats", {}).get("avg_temp")
    current_hum  = rt.get("humidity") if fresh else None
    current_risk = (rt.get("risk_level") if fresh else None) or classify_risk(current_usi or 0)

    return {
        "city":             batch["city"],
        # ── Live (speed layer) ──
        "current_usi":      current_usi,
        "current_aqi":      current_aqi,
        "current_temp":     current_temp,
        "current_humidity": current_hum,
        "current_risk":     current_risk,
        "is_anomaly":       rt.get("is_anomaly", False) if fresh else False,
        "anomaly_method":   rt.get("anomaly_method", "threshold") if rt else "threshold",
        "last_updated":     rt.get("updated_at") if rt else None,
        "freshness": {
            "is_fresh":   fresh,
            "updated_at": rt.get("updated_at") if rt else None,
        },
        # ── Historical (batch layer) ──
        "avg_usi":           avg_u,
        "avg_aqi":           batch.get("stats", {}).get("avg_aqi"),
        "avg_temp":          batch.get("stats", {}).get("avg_temp"),
        "avg_humidity":      batch.get("stats", {}).get("avg_humidity"),
        "max_usi":           batch.get("stats", {}).get("max_usi"),
        "health_score":      health_score(avg_u),
        "health_rank":       rank,
        "risk_distribution": batch.get("risk_distribution", {}),
    }


# ── Static Routes ──────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


# ── /api/cities ────────────────────────────────────────────────────────────────
@app.route("/api/cities")
def get_cities():
    """
    Return all cities with merged batch + realtime data.
    Used by: Overview, Compare, Alerts tabs.
    """
    db, client = get_db()
    try:
        batch_docs    = list(db["batch_views"].find({}, {"_id": 0}))
        realtime_docs = list(db["realtime_views"].find({}, {"_id": 0}))
        rt_by_city    = {d["city"]: d for d in realtime_docs}
        rank_map      = build_rank_map(batch_docs)

        cities = [
            enrich_city(b, rt_by_city.get(b["city"], {}), rank_map.get(b["city"], 0))
            for b in batch_docs
        ]
        return jsonify({"status": "ok", "cities": cities})
    finally:
        client.close()


# ── /api/city/<name> ───────────────────────────────────────────────────────────
@app.route("/api/city/<city_name>")
def get_city_detail(city_name):
    """
    Return full detail for one city: batch stats, trends, realtime, lambda merge.
    Used by: Overview (detail panel), Trends tab.
    """
    db, client = get_db()
    try:
        lookup = CITY_ALIASES.get(city_name, city_name)
        batch  = db["batch_views"].find_one({"city": lookup}, {"_id": 0})
        rt     = db["realtime_views"].find_one({"city": lookup}, {"_id": 0})

        if not batch:
            return jsonify({"status": "error", "message": f"City '{city_name}' not found"}), 404

        fresh  = is_fresh(rt.get("updated_at")) if rt else False
        avg_u  = batch.get("stats", {}).get("avg_usi")

        # Rank
        all_batch = list(db["batch_views"].find({}, {"city": 1, "stats": 1, "_id": 0}))
        rank_map  = build_rank_map(all_batch)
        rank      = rank_map.get(lookup, 0)

        # Augment stats with derived fields
        stats = dict(batch.get("stats", {}))
        stats["health_score"] = health_score(avg_u)
        stats["health_rank"]  = rank

        # Lambda merge decision
        current_usi = (rt.get("usi") if fresh else None) or avg_u
        lambda_merge = {
            "current_usi": current_usi,
            "source":      "realtime" if fresh else "batch_fallback",
            "is_fresh":    fresh,
        }

        return jsonify({
            "status":            "ok",
            "city":              batch["city"],
            "batch_stats":       stats,
            "risk_distribution": batch.get("risk_distribution", {}),
            "monthly_trend":     batch.get("monthly_trend", []),
            "yearly_trend":      batch.get("yearly_trend", []),
            "lambda_merge":      lambda_merge,
            "realtime": {
                "usi":            rt.get("usi")                   if rt else None,
                "aqi":            rt.get("aqi")                   if rt else None,
                "temperature":    rt.get("temperature")            if rt else None,
                "humidity":       rt.get("humidity")               if rt else None,
                "risk_level":     rt.get("risk_level", "No data") if rt else "No data",
                "is_anomaly":     rt.get("is_anomaly", False)      if rt else False,
                "anomaly_method": rt.get("anomaly_method", "threshold") if rt else "threshold",
                "updated_at":     rt.get("updated_at")             if rt else None,
                "is_fresh":       fresh,
            },
        })
    finally:
        client.close()


# ── /api/ranking ───────────────────────────────────────────────────────────────
@app.route("/api/ranking")
def get_ranking():
    """
    Return cities ranked by historical avg USI (most polluted first).
    Used by: Ranking tab.
    """
    db, client = get_db()
    try:
        batch_docs    = list(db["batch_views"].find({}, {"_id": 0}))
        realtime_docs = list(db["realtime_views"].find({}, {"_id": 0}))
        rt_by_city    = {d["city"]: d for d in realtime_docs}

        ranking = []
        for batch in batch_docs:
            city  = batch["city"]
            rt    = rt_by_city.get(city, {})
            fresh = is_fresh(rt.get("updated_at"))
            avg_u = batch.get("stats", {}).get("avg_usi")
            hs    = health_score(avg_u)
            ranking.append({
                "city":          city,
                "avg_usi":       avg_u,
                "avg_aqi":       batch.get("stats", {}).get("avg_aqi"),
                "max_usi":       batch.get("stats", {}).get("max_usi"),
                "avg_pm25":      batch.get("stats", {}).get("avg_pm25"),
                "health_score":  hs,
                "current_usi":   rt.get("usi")          if fresh else avg_u,
                "current_aqi":   rt.get("aqi")          if fresh else batch.get("stats", {}).get("avg_aqi"),
                "current_risk":  rt.get("risk_level")   if fresh else classify_risk(avg_u or 0),
                "total_records": batch.get("stats", {}).get("total_records", 0),
                "is_fresh":      fresh,
            })

        # Sort by avg_usi descending
        ranking.sort(key=lambda x: x["avg_usi"] or 0, reverse=True)
        for i, r in enumerate(ranking):
            r["live_rank"] = i + 1

        return jsonify({"status": "ok", "ranking": ranking})
    finally:
        client.close()


# ── /api/forecast/<city> ───────────────────────────────────────────────────────
@app.route("/api/forecast/<city_name>")
def get_forecast(city_name):
    """
    Generate 12-month USI forecast using seasonal blending + linear trend.
    Method: 70% seasonal (from monthly_trend) + 30% linear trend.
    Used by: Forecast tab.
    """
    db, client = get_db()
    try:
        lookup = CITY_ALIASES.get(city_name, city_name)
        batch  = db["batch_views"].find_one({"city": lookup}, {"_id": 0})
        if not batch:
            return jsonify({"status": "error", "message": f"City '{city_name}' not found"}), 404

        monthly  = sorted(batch.get("monthly_trend", []), key=lambda x: x["month"])
        avg_usi  = batch.get("stats", {}).get("avg_usi", 40) or 40
        std_usi  = batch.get("stats", {}).get("stddev_usi") or 5

        if not monthly:
            return jsonify({"status": "ok", "city": lookup, "forecast": []})

        monthly_avg = {m["month"]: m["avg_usi"] for m in monthly}
        grand_avg   = sum(monthly_avg.values()) / len(monthly_avg)

        MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        start  = datetime.now().month
        forecast = []
        for i in range(12):
            month_idx = (start + i - 1) % 12 + 1
            seasonal  = monthly_avg.get(month_idx, grand_avg)
            trend     = avg_usi + (i * 0.08)   # gentle upward drift
            predicted = round(seasonal * 0.7 + trend * 0.3, 2)
            ci        = round(std_usi * 0.75, 2)
            forecast.append({
                "month":         MONTHS[month_idx - 1],
                "predicted_usi": predicted,
                "upper_bound":   round(predicted + ci, 2),
                "lower_bound":   round(max(0.0, predicted - ci), 2),
            })

        return jsonify({"status": "ok", "city": lookup, "forecast": forecast})
    finally:
        client.close()


# ── /api/correlation/<city> ────────────────────────────────────────────────────
@app.route("/api/correlation/<city_name>")
def get_city_correlation(city_name):
    """
    Return Pearson correlation matrix [AQI, Temp, Hum, USI] for a city.
    Used by: Correlation tab (heatmap).
    """
    db, client = get_db()
    try:
        lookup = CITY_ALIASES.get(city_name, city_name)
        doc    = db["correlations"].find_one({"city": lookup}, {"_id": 0})
        if not doc:
            return jsonify({"status": "error", "message": f"No correlation data for '{city_name}'"}), 404

        corr = doc.get("correlations", {})

        # 4×4 symmetric matrix — labels match dashboard JS: ['AQI','Temp','Hum','USI']
        matrix = [
            [1.0,                           corr.get("aqi_vs_temp", 0),  corr.get("aqi_vs_hum", 0),  corr.get("aqi_vs_usi", 0)],
            [corr.get("aqi_vs_temp", 0),    1.0,                          corr.get("temp_vs_hum", 0), corr.get("temp_vs_usi", 0)],
            [corr.get("aqi_vs_hum", 0),     corr.get("temp_vs_hum", 0),  1.0,                         corr.get("hum_vs_usi", 0)],
            [corr.get("aqi_vs_usi", 0),     corr.get("temp_vs_usi", 0),  corr.get("hum_vs_usi", 0),  1.0],
        ]

        return jsonify({
            "status": "ok",
            "city":   lookup,
            "correlation": {
                "matrix":      matrix,
                "labels":      ["AQI", "temperature", "humidity", "usi"],
                "sample_size": doc.get("sample_size", 0),
                "raw":         corr,
            },
        })
    finally:
        client.close()


# ── /api/quality ───────────────────────────────────────────────────────────────
@app.route("/api/quality")
def get_quality():
    """
    Return pipeline data quality report: coverage, freshness, model status.
    Used by: Data Quality tab.
    """
    db, client = get_db()
    try:
        quality_docs  = list(db["data_quality"].find({}, {"_id": 0}))
        realtime_docs = list(db["realtime_views"].find({}, {"_id": 0}))
        rt_by_city    = {d["city"]: d for d in realtime_docs}

        total_records = sum(d.get("total_records", 0) for d in quality_docs)
        live_cities   = sum(1 for d in quality_docs if rt_by_city.get(d["city"]))
        fresh_cities  = sum(1 for d in quality_docs if is_fresh(rt_by_city.get(d["city"], {}).get("updated_at")))

        cities_out = []
        for d in quality_docs:
            city  = d["city"]
            rt    = rt_by_city.get(city, {})
            fresh = is_fresh(rt.get("updated_at"))
            cities_out.append({
                "city":             city,
                "batch_records":    d.get("total_records", 0),
                "has_realtime":     bool(rt),
                "is_fresh":         fresh,
                "has_ml_model":     False,   # Isolation Forest not yet deployed
                "model_trained_on": d.get("total_records", 0),
                "last_updated":     rt.get("updated_at"),
                "coverage":         d.get("coverage", {}),
                "quality_score":    d.get("quality_score", 0),
            })

        return jsonify({
            "status": "ok",
            "summary": {
                "total_cities":  len(quality_docs),
                "live_cities":   live_cities,
                "fresh_cities":  fresh_cities,
                "model_cities":  0,
                "total_records": total_records,
                "coverage_pct":  round(
                    sum(d.get("coverage", {}).get("overall", 0) for d in quality_docs) / max(len(quality_docs), 1),
                    1
                ),
                "data_source":   "Kaggle India Air Quality (2015-2020)",
            },
            "cities": cities_out,
        })
    finally:
        client.close()


# ── /api/health ────────────────────────────────────────────────────────────────
@app.route("/api/health")
def health():
    """
    Return pipeline health status for Architecture tab status indicators.
    """
    db, client = get_db()
    try:
        rt_docs     = list(db["realtime_views"].find({}, {"_id": 0}))
        fresh_count = sum(1 for d in rt_docs if is_fresh(d.get("updated_at")))

        return jsonify({
            "status":    "ok",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "batch_layer": {
                "documents": db["batch_views"].count_documents({}),
                "ready":     db["batch_views"].count_documents({}) > 0,
            },
            "speed_layer": {
                "documents":      len(rt_docs),
                "fresh_readings": fresh_count,
                "ready":          len(rt_docs) > 0,
            },
            "correlations": {
                "documents": db["correlations"].count_documents({}),
                "ready":     db["correlations"].count_documents({}) > 0,
            },
            "data_quality": {
                "documents": db["data_quality"].count_documents({}),
                "ready":     db["data_quality"].count_documents({}) > 0,
            },
        })
    finally:
        client.close()


# ── /api/export/csv ────────────────────────────────────────────────────────────
@app.route("/api/export/csv")
def export_csv():
    """
    Export all city data as CSV.
    Used by: Export CSV button in dashboard header.
    """
    db, client = get_db()
    try:
        batch_docs    = list(db["batch_views"].find({}, {"_id": 0}))
        realtime_docs = list(db["realtime_views"].find({}, {"_id": 0}))
        rt_by_city    = {d["city"]: d for d in realtime_docs}
        rank_map      = build_rank_map(batch_docs)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "city", "health_rank", "health_score",
            "avg_usi", "avg_aqi", "avg_temp", "avg_humidity",
            "max_usi", "avg_pm25", "total_records",
            "current_usi", "current_aqi", "current_temp", "current_humidity",
            "current_risk", "is_anomaly", "is_fresh", "last_updated",
        ])

        for batch in sorted(batch_docs, key=lambda x: x.get("stats", {}).get("avg_usi") or 0, reverse=True):
            city  = batch["city"]
            rt    = rt_by_city.get(city, {})
            fresh = is_fresh(rt.get("updated_at"))
            avg_u = batch.get("stats", {}).get("avg_usi")
            writer.writerow([
                city,
                rank_map.get(city, ""),
                health_score(avg_u),
                avg_u,
                batch.get("stats", {}).get("avg_aqi"),
                batch.get("stats", {}).get("avg_temp"),
                batch.get("stats", {}).get("avg_humidity"),
                batch.get("stats", {}).get("max_usi"),
                batch.get("stats", {}).get("avg_pm25"),
                batch.get("stats", {}).get("total_records"),
                rt.get("usi") if fresh else "",
                rt.get("aqi") if fresh else "",
                rt.get("temperature") if fresh else "",
                rt.get("humidity") if fresh else "",
                rt.get("risk_level") if fresh else "",
                rt.get("is_anomaly", False) if fresh else "",
                fresh,
                rt.get("updated_at", ""),
            ])

        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=ueris_export.csv"},
        )
    finally:
        client.close()


# ── /api/realtime ──────────────────────────────────────────────────────────────
@app.route("/api/realtime")
def get_realtime():
    """Return raw realtime readings from speed layer."""
    db, client = get_db()
    try:
        docs = list(db["realtime_views"].find({}, {"_id": 0}))
        return jsonify({"status": "ok", "readings": docs, "count": len(docs)})
    finally:
        client.close()


# ── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n{'='*60}")
    print(f"  UERIS Serving Layer")
    print(f"  Dashboard : http://localhost:{port}")
    print(f"  API health: http://localhost:{port}/api/health")
    print(f"  MongoDB   : {MONGO_URI[:40]}...")
    print(f"{'='*60}\n")
    app.run(debug=False, host="0.0.0.0", port=port)


# ── Background Live Data Worker ────────────────────────────────────────────────
"""
Production Speed Layer: runs as a background thread inside Flask/Gunicorn.
Fetches real environmental data from Open-Meteo (no key) + WAQI (free token)
every 60 seconds and writes to MongoDB Atlas realtime_views collection.

This replaces stream_simulator.py + speed_processing.py on the server.
PySpark streaming is used only in local/on-premise deployments.
"""

import threading
import requests as _requests
import math as _math

WAQI_TOKEN = os.environ.get("WAQI_TOKEN", "demo")
LIVE_FETCH_INTERVAL = int(os.environ.get("LIVE_FETCH_INTERVAL", 60))

CITIES_COORDS = {
    "Ahmedabad":         {"lat": 23.0225, "lon": 72.5714},
    "Aizawl":            {"lat": 23.7271, "lon": 92.7176},
    "Amaravati":         {"lat": 16.5730, "lon": 80.3582},
    "Amritsar":          {"lat": 31.6340, "lon": 74.8723},
    "Bengaluru":         {"lat": 12.9716, "lon": 77.5946},
    "Bhopal":            {"lat": 23.2599, "lon": 77.4126},
    "Brajrajnagar":      {"lat": 21.8167, "lon": 83.9167},
    "Chandigarh":        {"lat": 30.7333, "lon": 76.7794},
    "Chennai":           {"lat": 13.0827, "lon": 80.2707},
    "Coimbatore":        {"lat": 11.0168, "lon": 76.9558},
    "Delhi":             {"lat": 28.6139, "lon": 77.2090},
    "Ernakulam":         {"lat":  9.9816, "lon": 76.2999},
    "Gurugram":          {"lat": 28.4595, "lon": 77.0266},
    "Guwahati":          {"lat": 26.1445, "lon": 91.7362},
    "Hyderabad":         {"lat": 17.3850, "lon": 78.4867},
    "Jaipur":            {"lat": 26.9124, "lon": 75.7873},
    "Jorapokhar":        {"lat": 23.6800, "lon": 86.4200},
    "Kochi":             {"lat":  9.9312, "lon": 76.2673},
    "Kolkata":           {"lat": 22.5726, "lon": 88.3639},
    "Lucknow":           {"lat": 26.8467, "lon": 80.9462},
    "Mumbai":            {"lat": 19.0760, "lon": 72.8777},
    "Patna":             {"lat": 25.5941, "lon": 85.1376},
    "Shillong":          {"lat": 25.5788, "lon": 91.8933},
    "Talcher":           {"lat": 20.9500, "lon": 85.2333},
    "Thiruvananthapuram":{"lat":  8.5241, "lon": 76.9366},
    "Visakhapatnam":     {"lat": 17.6868, "lon": 83.2185},
}


def _fetch_weather(lat: float, lon: float) -> tuple:
    """Fetch real temperature + humidity from Open-Meteo (no API key needed)."""
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&current=temperature_2m,relative_humidity_2m"
            f"&timezone=Asia/Kolkata"
        )
        r = _requests.get(url, timeout=8)
        c = r.json()["current"]
        return float(c["temperature_2m"]), float(c["relative_humidity_2m"])
    except Exception:
        return None, None


def _fetch_aqi(city: str, lat: float, lon: float) -> float | None:
    """Fetch real AQI — WAQI first, Open-Meteo Air Quality as fallback."""
    # Try WAQI
    if WAQI_TOKEN and WAQI_TOKEN != "demo":
        try:
            url = f"https://api.waqi.info/feed/geo:{lat};{lon}/?token={WAQI_TOKEN}"
            r   = _requests.get(url, timeout=8)
            d   = r.json()
            if d.get("status") == "ok":
                return float(d["data"]["aqi"])
        except Exception:
            pass
    # Fallback: Open-Meteo Air Quality
    try:
        url = (
            f"https://air-quality-api.open-meteo.com/v1/air-quality"
            f"?latitude={lat}&longitude={lon}"
            f"&current=us_aqi&timezone=Asia/Kolkata"
        )
        r   = _requests.get(url, timeout=8)
        aqi = r.json().get("current", {}).get("us_aqi")
        if aqi is not None:
            return float(aqi)
    except Exception:
        pass
    return None


def _live_fetch_loop():
    """Background thread: fetch all cities and upsert into MongoDB Atlas."""
    import time
    print("[LiveWorker] Started — fetching every", LIVE_FETCH_INTERVAL, "seconds")
    while True:
        try:
            db, client = get_db()
            col        = db["realtime_views"]
            success    = 0
            for city, coords in CITIES_COORDS.items():
                lat, lon     = coords["lat"], coords["lon"]
                temp, humidity = _fetch_weather(lat, lon)
                aqi            = _fetch_aqi(city, lat, lon)
                if temp is None or aqi is None:
                    continue
                usi  = compute_usi(aqi, temp, humidity or 50)
                risk = classify_risk(usi)
                doc  = {
                    "city":        city,
                    "aqi":         round(aqi, 1),
                    "temperature": round(temp, 1),
                    "humidity":    round(float(humidity), 1) if humidity else None,
                    "usi":         usi,
                    "risk_level":  risk,
                    "is_anomaly":  aqi > 200,
                    "anomaly_method": "threshold",
                    "updated_at":  datetime.now(timezone.utc).isoformat(),
                    "data_source": "Open-Meteo + WAQI (realtime)",
                }
                col.update_one({"city": city}, {"$set": doc}, upsert=True)
                success += 1
                time.sleep(0.5)   # small delay to avoid rate-limiting
            client.close()
            print(f"[LiveWorker] Updated {success}/{len(CITIES_COORDS)} cities")
        except Exception as e:
            print(f"[LiveWorker] Error: {e}")
        time.sleep(LIVE_FETCH_INTERVAL)


def start_live_worker():
    """Start background live data thread (daemon = stops when Flask exits)."""
    t = threading.Thread(target=_live_fetch_loop, daemon=True, name="LiveWorker")
    t.start()
    return t


# Start worker when module loads (both `python app.py` and gunicorn)
_worker_started = False
def _init_worker():
    global _worker_started
    if not _worker_started:
        _worker_started = True
        start_live_worker()

_init_worker()