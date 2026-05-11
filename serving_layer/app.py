"""
serving_layer/app.py
=====================
SERVING LAYER — Lambda Architecture

New endpoints added:
  GET /api/forecast/<city>      12-month USI forecast with confidence bands
  GET /api/correlation/<city>   Pearson correlation matrix (AQI/Temp/Hum/USI)
  GET /api/ranking              City health leaderboard sorted by health score
  GET /api/quality              Data quality report (coverage, null rates)
  GET /api/export/csv           Download all stats as CSV
  GET /api/architecture         Lambda architecture metadata

Lambda merge:
  Realtime readings (< 30 min fresh) take priority over batch averages.
  Batch stats (avg, max, distribution, forecast, correlation) always shown.

Authentication:
  Set API_KEY env var to enable simple API key auth on /api/* endpoints.
  Pass as header: X-API-Key: your_key
  Dashboard is always public (no auth on /).

Run:
  python serving_layer/app.py
"""

import os, io, csv
from datetime import datetime, timezone
from functools import wraps

from flask import Flask, jsonify, send_from_directory, Response, request, abort
from flask_cors import CORS
import pymongo

MONGO_URI            = os.environ.get("MONGO_URI",  "mongodb://localhost:27017/")
DB_NAME              = os.environ.get("DB_NAME",    "urban_env_db")
PORT                 = int(os.environ.get("PORT",   5000))
FRESHNESS_WINDOW_MIN = int(os.environ.get("FRESHNESS_WINDOW_MIN", "30"))
API_KEY              = os.environ.get("API_KEY", "")  # optional auth

CITY_ALIASES = {"Bangalore": "Bengaluru", "Bengaluru": "Bengaluru"}

app = Flask(
    __name__,
    static_folder=os.path.join(os.path.dirname(__file__), "../dashboard"),
    static_url_path=""
)
CORS(app)


# ── Auth decorator (optional) ─────────────────────────────────────────────────
def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if API_KEY:
            key = request.headers.get("X-API-Key", "")
            if key != API_KEY:
                return jsonify({"status": "error", "message": "Invalid or missing X-API-Key"}), 401
        return f(*args, **kwargs)
    return decorated


def get_db():
    client = pymongo.MongoClient(MONGO_URI)
    return client[DB_NAME], client


def realtime_is_fresh(rt):
    if not rt or not rt.get("updated_at"):
        return False
    try:
        updated = datetime.fromisoformat(rt["updated_at"])
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age_min = (datetime.now(timezone.utc) - updated).total_seconds() / 60
        return age_min <= FRESHNESS_WINDOW_MIN
    except Exception:
        return False


def merge_city(batch, rt):
    """Core Lambda merge: realtime priority with batch fallback."""
    fresh = realtime_is_fresh(rt)
    stats = batch.get("stats", {})
    rt    = rt or {}
    return {
        "city":             batch["city"],
        "current_usi":      rt.get("usi")         if fresh else stats.get("avg_usi"),
        "current_aqi":      rt.get("aqi")         if fresh else stats.get("avg_aqi"),
        "current_temp":     rt.get("temperature") if fresh else stats.get("avg_temp"),
        "current_humidity": rt.get("humidity"),
        "current_risk":     rt.get("risk_level", "N/A"),
        "is_anomaly":       rt.get("is_anomaly", False),
        "anomaly_method":   rt.get("anomaly_method", "threshold"),
        "last_updated":     rt.get("updated_at"),
        "avg_usi":          stats.get("avg_usi"),
        "avg_aqi":          stats.get("avg_aqi"),
        "avg_temp":         stats.get("avg_temp"),
        "max_usi":          stats.get("max_usi"),
        "stddev_usi":       stats.get("stddev_usi"),
        "health_score":     stats.get("health_score"),
        "health_rank":      stats.get("health_rank"),
        "total_records":    stats.get("total_records"),
        "risk_distribution": batch.get("risk_distribution", {}),
        "freshness": {
            "has_realtime":         bool(rt),
            "is_fresh":             fresh,
            "updated_at":           rt.get("updated_at"),
            "freshness_window_min": FRESHNESS_WINDOW_MIN,
            "fallback_to_batch":    not fresh,
            "active_source":        "realtime" if fresh else "batch_fallback",
        },
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/health", methods=["GET"])
def health():
    db, client = get_db()
    try:
        batch_count = db["batch_views"].count_documents({})
        rt_docs     = list(db["realtime_views"].find({}, {"updated_at": 1, "_id": 0}))
        fresh_count = sum(1 for d in rt_docs if realtime_is_fresh(d))
        return jsonify({
            "status":    "ok",
            "timestamp": datetime.now().isoformat(),
            "batch_layer": {
                "collection": "batch_views",
                "documents":  batch_count,
                "ready":      batch_count > 0,
            },
            "speed_layer": {
                "collection":      "realtime_views",
                "documents":       len(rt_docs),
                "fresh_readings":  fresh_count,
                "freshness_window_min": FRESHNESS_WINDOW_MIN,
                "ready":           len(rt_docs) > 0,
            },
            "auth_enabled": bool(API_KEY),
        })
    finally:
        client.close()


@app.route("/api/cities", methods=["GET"])
@require_api_key
def get_cities():
    db, client = get_db()
    try:
        batch_docs = list(db["batch_views"].find({}, {"_id": 0, "anomaly_model": 0}))
        rt_docs    = list(db["realtime_views"].find({}, {"_id": 0}))
        rt_by_city = {d["city"]: d for d in rt_docs}
        result     = [merge_city(b, rt_by_city.get(b["city"])) for b in batch_docs]
        return jsonify({"status": "ok", "cities": result, "count": len(result)})
    finally:
        client.close()


@app.route("/api/city/<city_name>", methods=["GET"])
@require_api_key
def get_city_detail(city_name):
    db, client = get_db()
    try:
        lookup = CITY_ALIASES.get(city_name, city_name)
        batch  = db["batch_views"].find_one({"city": lookup},    {"_id": 0, "anomaly_model.model_b64": 0})
        rt     = db["realtime_views"].find_one({"city": lookup}, {"_id": 0})
        if not batch:
            return jsonify({"status": "error", "message": f"City '{city_name}' not found"}), 404

        fresh = realtime_is_fresh(rt)
        stats = batch.get("stats", {})
        rt    = rt or {}

        return jsonify({
            "status":            "ok",
            "city":              batch["city"],
            "batch_stats":       stats,
            "risk_distribution": batch.get("risk_distribution", {}),
            "monthly_trend":     batch.get("monthly_trend", []),
            "yearly_trend":      batch.get("yearly_trend", []),
            "forecast":          batch.get("forecast", []),
            "correlation":       batch.get("correlation", {}),
            "usi_formula":       batch.get("usi_formula", {}),
            "realtime": {
                "usi":            rt.get("usi"),
                "aqi":            rt.get("aqi"),
                "temperature":    rt.get("temperature"),
                "humidity":       rt.get("humidity"),
                "risk_level":     rt.get("risk_level", "No data"),
                "is_anomaly":     rt.get("is_anomaly", False),
                "anomaly_method": rt.get("anomaly_method", "threshold"),
                "updated_at":     rt.get("updated_at"),
                "is_fresh":       fresh,
            },
            "lambda_merge": {
                "active_source":        "realtime" if fresh else "batch_fallback",
                "current_usi":          rt.get("usi") if fresh else stats.get("avg_usi"),
                "current_aqi":          rt.get("aqi") if fresh else stats.get("avg_aqi"),
                "freshness_window_min": FRESHNESS_WINDOW_MIN,
            }
        })
    finally:
        client.close()


@app.route("/api/realtime", methods=["GET"])
@require_api_key
def get_realtime():
    db, client = get_db()
    try:
        docs = list(db["realtime_views"].find({}, {"_id": 0}))
        for d in docs:
            d["is_fresh"] = realtime_is_fresh(d)
        return jsonify({"status": "ok", "readings": docs, "count": len(docs)})
    finally:
        client.close()


@app.route("/api/forecast/<city_name>", methods=["GET"])
@require_api_key
def get_forecast(city_name):
    """Return 12-month USI forecast with confidence bands."""
    db, client = get_db()
    try:
        lookup = CITY_ALIASES.get(city_name, city_name)
        batch  = db["batch_views"].find_one({"city": lookup},
                    {"forecast": 1, "stats": 1, "city": 1, "_id": 0})
        if not batch:
            return jsonify({"status": "error", "message": f"City '{city_name}' not found"}), 404
        return jsonify({
            "status":   "ok",
            "city":     batch["city"],
            "forecast": batch.get("forecast", []),
            "avg_usi":  batch.get("stats", {}).get("avg_usi"),
            "stddev_usi": batch.get("stats", {}).get("stddev_usi"),
            "method":   "linear_trend_extrapolation_with_seasonal_blending",
        })
    finally:
        client.close()


@app.route("/api/correlation/<city_name>", methods=["GET"])
@require_api_key
def get_correlation(city_name):
    """Return Pearson correlation matrix for a city."""
    db, client = get_db()
    try:
        lookup = CITY_ALIASES.get(city_name, city_name)
        batch  = db["batch_views"].find_one({"city": lookup},
                    {"correlation": 1, "city": 1, "_id": 0})
        if not batch:
            return jsonify({"status": "error", "message": f"City '{city_name}' not found"}), 404
        return jsonify({
            "status":      "ok",
            "city":        batch["city"],
            "correlation": batch.get("correlation", {}),
            "description": "Pearson correlation between AQI, temperature, humidity, and USI",
        })
    finally:
        client.close()


@app.route("/api/ranking", methods=["GET"])
@require_api_key
def get_ranking():
    """Return all cities ranked by health score (best to worst)."""
    db, client = get_db()
    try:
        batch_docs = list(db["batch_views"].find({}, {"_id": 0, "anomaly_model": 0,
                                                       "monthly_trend": 0, "yearly_trend": 0,
                                                       "forecast": 0, "correlation": 0}))
        rt_docs    = list(db["realtime_views"].find({}, {"_id": 0}))
        rt_by_city = {d["city"]: d for d in rt_docs}

        ranking = []
        for batch in batch_docs:
            stats = batch.get("stats", {})
            rt    = rt_by_city.get(batch["city"], {})
            fresh = realtime_is_fresh(rt)
            ranking.append({
                "city":          batch["city"],
                "health_score":  stats.get("health_score"),
                "health_rank":   stats.get("health_rank"),
                "avg_usi":       stats.get("avg_usi"),
                "avg_aqi":       stats.get("avg_aqi"),
                "current_usi":   rt.get("usi") if fresh else stats.get("avg_usi"),
                "current_risk":  rt.get("risk_level", "N/A"),
                "is_anomaly":    rt.get("is_anomaly", False),
                "total_records": stats.get("total_records"),
            })

        ranking.sort(key=lambda x: (x["health_score"] or 0), reverse=True)
        # Add live rank position
        for i, r in enumerate(ranking, 1):
            r["live_rank"] = i

        return jsonify({"status": "ok", "ranking": ranking, "count": len(ranking)})
    finally:
        client.close()


@app.route("/api/quality", methods=["GET"])
@require_api_key
def get_quality():
    """Data quality report: coverage, freshness, anomaly model status."""
    db, client = get_db()
    try:
        batch_docs = list(db["batch_views"].find({}, {"city": 1, "stats": 1,
                                                       "anomaly_model": 1, "_id": 0}))
        rt_docs    = list(db["realtime_views"].find({}, {"_id": 0}))
        rt_by_city = {d["city"]: d for d in rt_docs}

        total_cities  = len(batch_docs)
        live_cities   = sum(1 for d in batch_docs if d["city"] in rt_by_city)
        fresh_cities  = sum(1 for d in batch_docs
                            if realtime_is_fresh(rt_by_city.get(d["city"])))
        model_cities  = sum(1 for d in batch_docs if d.get("anomaly_model", {}).get("threshold"))
        total_records = sum(d.get("stats", {}).get("total_records", 0) for d in batch_docs)

        city_quality = []
        for d in batch_docs:
            city = d["city"]
            rt   = rt_by_city.get(city, {})
            am   = d.get("anomaly_model", {})
            city_quality.append({
                "city":           city,
                "batch_records":  d.get("stats", {}).get("total_records", 0),
                "has_realtime":   city in rt_by_city,
                "is_fresh":       realtime_is_fresh(rt),
                "has_ml_model":   bool(am.get("threshold")),
                "model_trained_on": am.get("trained_on", 0),
                "last_updated":   rt.get("updated_at"),
            })

        return jsonify({
            "status": "ok",
            "summary": {
                "total_cities":   total_cities,
                "live_cities":    live_cities,
                "fresh_cities":   fresh_cities,
                "model_cities":   model_cities,
                "total_records":  total_records,
                "coverage_pct":   round(live_cities / total_cities * 100, 1) if total_cities else 0,
                "freshness_pct":  round(fresh_cities / total_cities * 100, 1) if total_cities else 0,
            },
            "cities": city_quality,
        })
    finally:
        client.close()


@app.route("/api/export/csv", methods=["GET"])
@require_api_key
def export_csv():
    """Download all city stats as a single CSV file."""
    db, client = get_db()
    try:
        batch_docs = list(db["batch_views"].find({}, {"_id": 0, "anomaly_model": 0,
                                                       "monthly_trend": 0, "yearly_trend": 0,
                                                       "forecast": 0, "correlation": 0}))
        rt_docs    = list(db["realtime_views"].find({}, {"_id": 0}))
        rt_by_city = {d["city"]: d for d in rt_docs}

        out = io.StringIO()
        w   = csv.writer(out)
        w.writerow([
            "City", "Health Score", "Health Rank",
            "Current USI", "Current AQI", "Current Temp (C)", "Current Humidity (%)",
            "Current Risk", "Is Anomaly", "Anomaly Method",
            "Avg USI", "Max USI", "StdDev USI", "Avg AQI", "Avg PM2.5",
            "Avg Temp", "Total Records",
            "Risk Low", "Risk Moderate", "Risk High", "Risk Very High", "Risk Severe",
            "Data Source", "Last Updated",
        ])

        for batch in sorted(batch_docs, key=lambda b: b.get("stats", {}).get("health_rank", 99)):
            stats = batch.get("stats", {})
            rd    = batch.get("risk_distribution", {})
            rt    = rt_by_city.get(batch["city"], {})
            fresh = realtime_is_fresh(rt)
            w.writerow([
                batch["city"],
                stats.get("health_score", ""),
                stats.get("health_rank", ""),
                rt.get("usi") if fresh else stats.get("avg_usi", ""),
                rt.get("aqi") if fresh else stats.get("avg_aqi", ""),
                rt.get("temperature") if fresh else stats.get("avg_temp", ""),
                rt.get("humidity", ""),
                rt.get("risk_level", "N/A"),
                "Yes" if rt.get("is_anomaly") else "No",
                rt.get("anomaly_method", "threshold"),
                stats.get("avg_usi", ""),
                stats.get("max_usi", ""),
                stats.get("stddev_usi", ""),
                stats.get("avg_aqi", ""),
                stats.get("avg_pm25", ""),
                stats.get("avg_temp", ""),
                stats.get("total_records", ""),
                rd.get("Low", 0),
                rd.get("Moderate", 0),
                rd.get("High", 0),
                rd.get("Very High", 0),
                rd.get("Severe", 0),
                batch.get("data_source", ""),
                rt.get("updated_at", ""),
            ])

        out.seek(0)
        fname = f"ueris_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return Response(
            out.getvalue(), mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={fname}"}
        )
    finally:
        client.close()


@app.route("/api/architecture", methods=["GET"])
def architecture():
    return jsonify({
        "status": "ok",
        "architecture": {
            "name": "Lambda Architecture",
            "layers": {
                "batch":   {"technology": "Apache PySpark", "output": "batch_views",   "latency": "minutes"},
                "speed":   {"technology": "PySpark micro-batch + file simulation", "output": "realtime_views", "latency": "5s"},
                "serving": {"technology": "Flask REST API", "merge": "realtime-priority with batch fallback"},
            },
            "ml_features": {
                "anomaly_detection": "Isolation Forest (per-city, trained on historical data)",
                "forecasting":       "Linear trend + seasonal blending (12-month USI forecast)",
                "correlation":       "Pearson correlation matrix (AQI, Temp, Humidity, USI)",
                "health_ranking":    "Composite score normalized from avg_usi across all cities",
            },
            "usi_formula": {
                "formula":   "USI = (0.5xAQI_norm + 0.3xTemp_norm + 0.2xHum_norm) x 100",
                "reference": "WHO AQI Guidelines 2021 / Lancet Countdown 2021",
            }
        }
    })


if __name__ == "__main__":
    print("\n" + "="*60)
    print("  SERVING LAYER — Urban Environmental Risk System")
    print(f"  Dashboard   : http://localhost:{PORT}")
    print(f"  Health      : http://localhost:{PORT}/api/health")
    print(f"  Ranking     : http://localhost:{PORT}/api/ranking")
    print(f"  CSV Export  : http://localhost:{PORT}/api/export/csv")
    print(f"  Data Quality: http://localhost:{PORT}/api/quality")
    print(f"  Auth        : {'API key required (X-API-Key header)' if API_KEY else 'Disabled'}")
    print("="*60 + "\n")
    
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
