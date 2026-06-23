"""
serving_layer/app.py — SERVING LAYER
"""
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import pymongo
from datetime import datetime
import os

app = Flask(__name__,
    static_folder=os.path.join(os.path.dirname(__file__), "../dashboard"),
    static_url_path="")
CORS(app)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME   = "urban_env_db"
CITY_ALIASES = {"Bangalore": "Bengaluru", "Bengaluru": "Bengaluru"}

def get_db():
    client = pymongo.MongoClient(MONGO_URI)
    return client[DB_NAME], client

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")

@app.route("/api/cities", methods=["GET"])
def get_cities():
    db, client = get_db()
    try:
        batch_docs    = list(db["batch_views"].find({}, {"_id": 0}))
        realtime_docs = list(db["realtime_views"].find({}, {"_id": 0}))
        rt_by_city    = {d["city"]: d for d in realtime_docs}
        result = []
        for batch in batch_docs:
            city = batch["city"]
            rt   = rt_by_city.get(city, {})
            result.append({
                "city":             city,
                "current_usi":      rt.get("usi"),
                "current_aqi":      rt.get("aqi"),
                "current_temp":     rt.get("temperature"),
                "current_humidity": rt.get("humidity"),
                "current_risk":     rt.get("risk_level", "N/A"),
                "is_anomaly":       rt.get("is_anomaly", False),
                "last_updated":     rt.get("updated_at"),
                "avg_usi":          batch.get("stats", {}).get("avg_usi"),
                "avg_aqi":          batch.get("stats", {}).get("avg_aqi"),
                "avg_temp":         batch.get("stats", {}).get("avg_temp"),
                "max_usi":          batch.get("stats", {}).get("max_usi"),
                "risk_distribution": batch.get("risk_distribution", {}),
            })
        return jsonify({"status": "ok", "cities": result})
    finally:
        client.close()

@app.route("/api/city/<city_name>", methods=["GET"])
def get_city_detail(city_name):
    db, client = get_db()
    try:
        lookup = CITY_ALIASES.get(city_name, city_name)
        batch  = db["batch_views"].find_one({"city": lookup}, {"_id": 0})
        rt     = db["realtime_views"].find_one({"city": lookup}, {"_id": 0})
        if not batch:
            return jsonify({"status": "error", "message": f"City '{city_name}' not found"}), 404
        return jsonify({
            "status":            "ok",
            "city":              batch["city"],
            "batch_stats":       batch.get("stats", {}),
            "risk_distribution": batch.get("risk_distribution", {}),
            "monthly_trend":     batch.get("monthly_trend", []),
            "yearly_trend":      batch.get("yearly_trend", []),
            "realtime": {
                "usi":         rt.get("usi")         if rt else None,
                "aqi":         rt.get("aqi")         if rt else None,
                "temperature": rt.get("temperature") if rt else None,
                "humidity":    rt.get("humidity")    if rt else None,
                "risk_level":  rt.get("risk_level")  if rt else "No data",
                "is_anomaly":  rt.get("is_anomaly", False) if rt else False,
                "updated_at":  rt.get("updated_at")  if rt else None,
            }
        })
    finally:
        client.close()

@app.route("/api/correlations", methods=["GET"])
def get_correlations():
    db, client = get_db()
    try:
        docs = list(db["correlations"].find({}, {"_id": 0}))
        return jsonify({"status": "ok", "correlations": docs})
    finally:
        client.close()

@app.route("/api/correlations/<city_name>", methods=["GET"])
def get_city_correlation(city_name):
    db, client = get_db()
    try:
        lookup = CITY_ALIASES.get(city_name, city_name)
        doc = db["correlations"].find_one({"city": lookup}, {"_id": 0})
        if not doc:
            return jsonify({"status": "error", "message": f"No correlation data for '{city_name}'"}), 404
        return jsonify({"status": "ok", "data": doc})
    finally:
        client.close()

@app.route("/api/quality", methods=["GET"])
def get_quality():
    db, client = get_db()
    try:
        docs = list(db["data_quality"].find({}, {"_id": 0}))
        # Overall pipeline stats
        total_records = sum(d.get("total_records", 0) for d in docs)
        avg_coverage  = round(sum(d.get("coverage", {}).get("overall", 0) for d in docs) / len(docs), 1) if docs else 0
        return jsonify({
            "status": "ok",
            "pipeline": {
                "total_records":   total_records,
                "cities_covered":  len(docs),
                "avg_coverage":    avg_coverage,
                "batch_computed":  datetime.now().isoformat(),
                "data_source":     "Kaggle India Air Quality (2015-2020)",
            },
            "cities": docs
        })
    finally:
        client.close()

@app.route("/api/ranking", methods=["GET"])
def get_ranking():
    db, client = get_db()
    try:
        batch_docs    = list(db["batch_views"].find({}, {"_id": 0}))
        realtime_docs = list(db["realtime_views"].find({}, {"_id": 0}))
        rt_by_city    = {d["city"]: d for d in realtime_docs}
        ranking = []
        for batch in batch_docs:
            city = batch["city"]
            rt   = rt_by_city.get(city, {})
            ranking.append({
                "city":      city,
                "avg_usi":   batch.get("stats", {}).get("avg_usi"),
                "avg_aqi":   batch.get("stats", {}).get("avg_aqi"),
                "max_usi":   batch.get("stats", {}).get("max_usi"),
                "avg_pm25":  batch.get("stats", {}).get("avg_pm25"),
                "current_usi": rt.get("usi"),
                "current_aqi": rt.get("aqi"),
                "risk":      rt.get("risk_level", "N/A"),
                "records":   batch.get("stats", {}).get("total_records", 0),
            })
        ranking.sort(key=lambda x: x["avg_usi"] or 0, reverse=True)
        for i, r in enumerate(ranking):
            r["rank"] = i + 1
        return jsonify({"status": "ok", "ranking": ranking})
    finally:
        client.close()

@app.route("/api/realtime", methods=["GET"])
def get_realtime():
    db, client = get_db()
    try:
        docs = list(db["realtime_views"].find({}, {"_id": 0}))
        return jsonify({"status": "ok", "readings": docs, "count": len(docs)})
    finally:
        client.close()

@app.route("/api/health", methods=["GET"])
def health():
    db, client = get_db()
    try:
        return jsonify({
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "batch_layer":   {"documents": db["batch_views"].count_documents({}),   "ready": True},
            "correlations":  {"documents": db["correlations"].count_documents({}),  "ready": True},
            "data_quality":  {"documents": db["data_quality"].count_documents({}),  "ready": True},
        })
    finally:
        client.close()

if __name__ == "__main__":
    print("\n" + "="*60)
    print("  SERVING LAYER - Urban Environmental Risk System")
    print("  Dashboard: http://localhost:5000")
    print("="*60 + "\n")
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
