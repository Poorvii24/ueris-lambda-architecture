"""
serving_layer/app.py — COMPLETE SERVING LAYER
Matches all API endpoints used by index.html
"""
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import pymongo
from datetime import datetime, timezone
import os
import math

app = Flask(__name__,
    static_folder=os.path.join(os.path.dirname(__file__), "../dashboard"),
    static_url_path="")
CORS(app)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME   = "urban_env_db"
CITY_ALIASES = {"Bangalore": "Bengaluru", "Bengaluru": "Bengaluru"}
FRESHNESS_MINUTES = 30

def get_db():
    client = pymongo.MongoClient(MONGO_URI)
    return client[DB_NAME], client

def is_fresh(updated_at_str):
    """Check if a realtime reading is within 30 minutes."""
    if not updated_at_str:
        return False
    try:
        updated = datetime.fromisoformat(updated_at_str)
        now = datetime.now()
        diff = (now - updated).total_seconds() / 60
        return diff < FRESHNESS_MINUTES
    except:
        return False

def health_score(avg_usi):
    """Convert avg USI to a 0-100 health score (higher = better)."""
    if avg_usi is None:
        return None
    return round(max(0, 100 - avg_usi), 1)

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")

# ── /api/cities ────────────────────────────────────────────────
@app.route("/api/cities", methods=["GET"])
def get_cities():
    db, client = get_db()
    try:
        batch_docs    = list(db["batch_views"].find({}, {"_id": 0}))
        realtime_docs = list(db["realtime_views"].find({}, {"_id": 0}))
        rt_by_city    = {d["city"]: d for d in realtime_docs}

        # Compute rankings
        all_avg_usi = [(b["city"], b.get("stats", {}).get("avg_usi") or 0) for b in batch_docs]
        all_avg_usi.sort(key=lambda x: x[1], reverse=True)
        rank_map = {city: i+1 for i, (city, _) in enumerate(all_avg_usi)}

        result = []
        for batch in batch_docs:
            city  = batch["city"]
            rt    = rt_by_city.get(city, {})
            fresh = is_fresh(rt.get("updated_at"))
            avg_u = batch.get("stats", {}).get("avg_usi")
            hs    = health_score(avg_u)
            result.append({
                "city":             city,
                "current_usi":      rt.get("usi") if fresh else None,
                "current_aqi":      rt.get("aqi") if fresh else None,
                "current_temp":     rt.get("temperature") if fresh else None,
                "current_humidity": rt.get("humidity") if fresh else None,
                "current_risk":     rt.get("risk_level", "N/A") if fresh else "N/A",
                "is_anomaly":       rt.get("is_anomaly", False) if fresh else False,
                "anomaly_method":   rt.get("anomaly_method", "threshold"),
                "last_updated":     rt.get("updated_at"),
                "freshness": {
                    "is_fresh":       fresh,
                    "updated_at":     rt.get("updated_at"),
                },
                "avg_usi":          avg_u,
                "avg_aqi":          batch.get("stats", {}).get("avg_aqi"),
                "avg_temp":         batch.get("stats", {}).get("avg_temp"),
                "max_usi":          batch.get("stats", {}).get("max_usi"),
                "health_score":     hs,
                "health_rank":      rank_map.get(city),
                "risk_distribution": batch.get("risk_distribution", {}),
            })
        return jsonify({"status": "ok", "cities": result})
    finally:
        client.close()

# ── /api/city/<name> ───────────────────────────────────────────
@app.route("/api/city/<city_name>", methods=["GET"])
def get_city_detail(city_name):
    db, client = get_db()
    try:
        lookup = CITY_ALIASES.get(city_name, city_name)
        batch  = db["batch_views"].find_one({"city": lookup}, {"_id": 0})
        rt     = db["realtime_views"].find_one({"city": lookup}, {"_id": 0})
        if not batch:
            return jsonify({"status": "error", "message": f"City '{city_name}' not found"}), 404

        fresh  = is_fresh(rt.get("updated_at")) if rt else False
        avg_u  = batch.get("stats", {}).get("avg_usi")

        # All batch docs for rank
        all_docs = list(db["batch_views"].find({}, {"city":1,"stats":1,"_id":0}))
        all_docs.sort(key=lambda x: x.get("stats",{}).get("avg_usi") or 0, reverse=True)
        rank = next((i+1 for i,d in enumerate(all_docs) if d["city"]==lookup), None)

        stats = batch.get("stats", {})
        stats["health_score"] = health_score(avg_u)
        stats["health_rank"]  = rank

        # Lambda merge: prefer fresh realtime, fallback to batch
        current_usi = (rt.get("usi") if fresh else None) or avg_u
        lambda_merge = {
            "current_usi":  current_usi,
            "source":       "realtime" if fresh else "batch_fallback",
            "is_fresh":     fresh,
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
                "usi":            rt.get("usi")          if rt else None,
                "aqi":            rt.get("aqi")          if rt else None,
                "temperature":    rt.get("temperature")  if rt else None,
                "humidity":       rt.get("humidity")     if rt else None,
                "risk_level":     rt.get("risk_level")   if rt else "No data",
                "is_anomaly":     rt.get("is_anomaly", False) if rt else False,
                "anomaly_method": rt.get("anomaly_method", "threshold") if rt else "threshold",
                "updated_at":     rt.get("updated_at")   if rt else None,
                "is_fresh":       fresh,
            }
        })
    finally:
        client.close()

# ── /api/ranking ───────────────────────────────────────────────
@app.route("/api/ranking", methods=["GET"])
def get_ranking():
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
            ranking.append({
                "city":          city,
                "avg_usi":       avg_u,
                "avg_aqi":       batch.get("stats", {}).get("avg_aqi"),
                "max_usi":       batch.get("stats", {}).get("max_usi"),
                "avg_pm25":      batch.get("stats", {}).get("avg_pm25"),
                "health_score":  health_score(avg_u),
                "current_usi":   rt.get("usi") if fresh else None,
                "current_aqi":   rt.get("aqi") if fresh else None,
                "current_risk":  rt.get("risk_level", "N/A") if fresh else "N/A",
                "total_records": batch.get("stats", {}).get("total_records", 0),
                "is_fresh":      fresh,
            })

        # Sort by avg_usi descending (most polluted first)
        ranking.sort(key=lambda x: x["avg_usi"] or 0, reverse=True)
        for i, r in enumerate(ranking):
            r["live_rank"] = i + 1

        return jsonify({"status": "ok", "ranking": ranking})
    finally:
        client.close()

# ── /api/forecast/<city> ───────────────────────────────────────
@app.route("/api/forecast/<city_name>", methods=["GET"])
def get_forecast(city_name):
    """Generate 12-month USI forecast using linear trend + seasonal blending."""
    db, client = get_db()
    try:
        lookup = CITY_ALIASES.get(city_name, city_name)
        batch  = db["batch_views"].find_one({"city": lookup}, {"_id": 0})
        if not batch:
            return jsonify({"status": "error", "message": "City not found"}), 404

        monthly = sorted(batch.get("monthly_trend", []), key=lambda x: x["month"])
        avg_usi = batch.get("stats", {}).get("avg_usi", 40)
        std_usi = batch.get("stats", {}).get("stddev_usi", 5) or 5

        if not monthly:
            return jsonify({"status": "ok", "city": lookup, "forecast": []})

        # Build seasonal pattern from monthly data
        monthly_avg = {m["month"]: m["avg_usi"] for m in monthly}
        grand_avg   = sum(monthly_avg.values()) / len(monthly_avg)

        MO = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        now   = datetime.now()
        start = now.month  # start from current month

        forecast = []
        for i in range(12):
            month_idx = (start + i - 1) % 12 + 1
            seasonal  = monthly_avg.get(month_idx, grand_avg)
            # Small linear upward trend (0.1 USI per month)
            trend     = avg_usi + (i * 0.1)
            predicted = round((seasonal * 0.7 + trend * 0.3), 2)
            upper     = round(predicted + std_usi * 0.8, 2)
            lower     = round(max(0, predicted - std_usi * 0.8), 2)
            forecast.append({
                "month":         MO[month_idx - 1],
                "predicted_usi": predicted,
                "upper_bound":   upper,
                "lower_bound":   lower,
            })

        return jsonify({"status": "ok", "city": lookup, "forecast": forecast})
    finally:
        client.close()

# ── /api/correlation/<city> ────────────────────────────────────
@app.route("/api/correlation/<city_name>", methods=["GET"])
def get_city_correlation(city_name):
    """Return correlation matrix for dashboard heatmap."""
    db, client = get_db()
    try:
        lookup = CITY_ALIASES.get(city_name, city_name)
        doc    = db["correlations"].find_one({"city": lookup}, {"_id": 0})
        if not doc:
            return jsonify({"status": "error", "message": f"No correlation data for '{city_name}'"}), 404

        corr = doc.get("correlations", {})
        # Build 4x4 matrix: [AQI, Temp, Hum, USI]
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
                "labels":      ["AQI", "Temp", "Hum", "USI"],
                "sample_size": doc.get("sample_size", 0),
                "raw":         corr,
            }
        })
    finally:
        client.close()

# ── /api/quality ───────────────────────────────────────────────
@app.route("/api/quality", methods=["GET"])
def get_quality():
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
                "city":           city,
                "batch_records":  d.get("total_records", 0),
                "has_realtime":   bool(rt),
                "is_fresh":       fresh,
                "has_ml_model":   False,  # threshold-based for now
                "model_trained_on": d.get("total_records", 0),
                "last_updated":   rt.get("updated_at"),
                "coverage":       d.get("coverage", {}),
                "quality_score":  d.get("quality_score", 0),
            })

        return jsonify({
            "status": "ok",
            "summary": {
                "total_cities":   len(quality_docs),
                "live_cities":    live_cities,
                "fresh_cities":   fresh_cities,
                "model_cities":   0,
                "total_records":  total_records,
                "data_source":    "Kaggle India Air Quality (2015-2020)",
            },
            "cities": cities_out
        })
    finally:
        client.close()

# ── /api/correlations (all cities) ────────────────────────────
@app.route("/api/correlations", methods=["GET"])
def get_all_correlations():
    db, client = get_db()
    try:
        docs = list(db["correlations"].find({}, {"_id": 0}))
        return jsonify({"status": "ok", "correlations": docs})
    finally:
        client.close()

# ── /api/realtime ──────────────────────────────────────────────
@app.route("/api/realtime", methods=["GET"])
def get_realtime():
    db, client = get_db()
    try:
        docs = list(db["realtime_views"].find({}, {"_id": 0}))
        return jsonify({"status": "ok", "readings": docs, "count": len(docs)})
    finally:
        client.close()

# ── /api/health ────────────────────────────────────────────────
@app.route("/api/health", methods=["GET"])
def health():
    db, client = get_db()
    try:
        rt_docs      = list(db["realtime_views"].find({}, {"_id": 0}))
        fresh_count  = sum(1 for d in rt_docs if is_fresh(d.get("updated_at")))
        return jsonify({
            "status":    "ok",
            "timestamp": datetime.now().isoformat(),
            "batch_layer": {
                "documents": db["batch_views"].count_documents({}),
                "ready":     db["batch_views"].count_documents({}) > 0
            },
            "speed_layer": {
                "documents":      len(rt_docs),
                "fresh_readings": fresh_count,
                "ready":          len(rt_docs) > 0
            },
            "correlations": {
                "documents": db["correlations"].count_documents({}),
                "ready":     db["correlations"].count_documents({}) > 0
            },
            "data_quality": {
                "documents": db["data_quality"].count_documents({}),
                "ready":     db["data_quality"].count_documents({}) > 0
            },
        })
    finally:
        client.close()

if __name__ == "__main__":
    print("\n" + "="*60)
    print("  SERVING LAYER - Urban Environmental Risk System")
    print("  Dashboard: http://localhost:5000")
    print("="*60 + "\n")
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
