"""
batch_layer/batch_processing.py
================================
BATCH LAYER — Lambda Architecture

Steps:
  1. Load Kaggle India AQ dataset (26 cities, 2015-2020)
  2. Estimate temperature & humidity from IMD baselines
  3. Compute Urban Stress Index (USI) per record
  4. Aggregate: city stats, monthly trends, yearly trends
  5. Train Isolation Forest anomaly model per city
  6. Compute Pearson correlation matrix per city (AQI/Temp/Humidity/USI)
  7. Generate 12-month USI forecast per city (ARIMA-style linear extrapolation)
  8. Build city health ranking leaderboard
  9. Write all results to MongoDB batch_views

USI Formula:
  USI = (0.5 x AQI_norm + 0.3 x Temp_norm + 0.2 x Hum_norm) x 100
  AQI_norm  = min(AQI/300, 1.0)         — WHO hazardous threshold
  Temp_norm = clamp((T-15)/25, 0, 1)    — baseline 15C, max stress at 40C
  Hum_norm  = |H-50| / 50               — 50% RH is ideal comfort midpoint
  Weights: 50% AQI (WHO 2021), 30% Temp (Lancet 2021), 20% Humidity

Run:
  python batch_layer/batch_processing.py
"""

import os, sys, json, pickle, base64
import numpy as np

_py = os.environ.get("PYSPARK_PYTHON", sys.executable)
os.environ["PYSPARK_PYTHON"]        = _py
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ.get("PYSPARK_DRIVER_PYTHON", _py)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pymongo
from datetime import datetime
from sklearn.ensemble import IsolationForest

MONGO_URI        = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME          = os.environ.get("DB_NAME",   "urban_env_db")
BATCH_COLLECTION = "batch_views"
HISTORICAL_CSV   = os.environ.get(
    "HISTORICAL_CSV",
    os.path.join(os.path.dirname(__file__), "../data/historical/city_day.csv")
)

ALL_CITIES = [
    'Ahmedabad','Aizawl','Amaravati','Amritsar','Bengaluru','Bhopal',
    'Brajrajnagar','Chandigarh','Chennai','Coimbatore','Delhi','Ernakulam',
    'Gurugram','Guwahati','Hyderabad','Jaipur','Jorapokhar','Kochi',
    'Kolkata','Lucknow','Mumbai','Patna','Shillong','Talcher',
    'Thiruvananthapuram','Visakhapatnam'
]

CITY_TEMPS = {
    'Delhi':28.0,'Mumbai':29.0,'Bengaluru':24.0,'Chennai':30.0,'Hyderabad':28.0,
    'Kolkata':27.0,'Ahmedabad':28.0,'Jaipur':27.0,'Lucknow':26.0,'Patna':26.0,
    'Chandigarh':23.0,'Amritsar':22.0,'Gurugram':28.0,'Guwahati':25.0,
    'Bhopal':26.0,'Coimbatore':27.0,'Kochi':28.0,'Ernakulam':28.0,
    'Visakhapatnam':28.0,'Thiruvananthapuram':28.0,'Shillong':17.0,
    'Aizawl':18.0,'Amaravati':29.0,'Jorapokhar':24.0,'Brajrajnagar':25.0,'Talcher':27.0
}

spark = SparkSession.builder \
    .appName("UrbanEnvRisk_BatchLayer") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*60)
print("  BATCH LAYER — Urban Environmental Risk System")
print(f"  {len(ALL_CITIES)} cities · Python: {_py}")
print("="*60)

# ── 1: Load ───────────────────────────────────────────────────────────────────
print("\n[1/8] Loading Kaggle dataset...")
df = spark.read.csv(HISTORICAL_CSV, header=True, inferSchema=True)
df = df.filter(F.col("City").isin(ALL_CITIES)) \
       .filter(F.col("AQI").isNotNull()) \
       .filter(F.col("AQI") > 0)
df = df.withColumn("date_parsed", F.to_date("Date", "yyyy-MM-dd")) \
       .withColumn("month", F.month("date_parsed")) \
       .withColumn("year",  F.year("date_parsed"))
print(f"   Records: {df.count():,}")

# ── 2: Climate estimation ─────────────────────────────────────────────────────
print("\n[2/8] IMD temperature & humidity estimation...")
temp_expr = F.lit(26.0)
for city, temp in CITY_TEMPS.items():
    temp_expr = F.when(F.col("City") == city, F.lit(temp)).otherwise(temp_expr)

df = df.withColumn("temp_base", temp_expr) \
       .withColumn("temperature",
           F.round(F.col("temp_base") + 5 * F.sin((F.col("month") - 3) * 3.14159 / 6), 1)) \
       .withColumn("humidity",
           F.round(
               F.when((F.col("month") >= 6) & (F.col("month") <= 9),
                   70 - (F.col("temperature") - 25) * 0.8)
               .otherwise(55 - (F.col("temperature") - 25) * 1.2), 1)) \
       .withColumn("humidity",
           F.greatest(F.lit(20.0), F.least(F.lit(95.0), F.col("humidity"))))

# ── 3: USI ────────────────────────────────────────────────────────────────────
print("\n[3/8] Computing USI...")
df = df.withColumn("usi",
    F.round(
        (F.least(F.col("AQI") / 300.0, F.lit(1.0)) * 0.5 +
         F.least(F.greatest((F.col("temperature") - 15.0) / 25.0, F.lit(0.0)), F.lit(1.0)) * 0.3 +
         F.abs(F.col("humidity") - 50.0) / 50.0 * 0.2) * 100.0, 2)) \
    .withColumn("risk_level",
        F.when(F.col("usi") < 20, "Low")
         .when(F.col("usi") < 40, "Moderate")
         .when(F.col("usi") < 60, "High")
         .when(F.col("usi") < 80, "Very High")
         .otherwise("Severe"))

# ── 4: Aggregations ───────────────────────────────────────────────────────────
print("\n[4/8] Aggregating city statistics...")
city_stats = df.groupBy("City").agg(
    F.round(F.avg("AQI"), 2).alias("avg_aqi"),
    F.round(F.max("AQI"), 2).alias("max_aqi"),
    F.round(F.min("AQI"), 2).alias("min_aqi"),
    F.round(F.avg("`PM2.5`"), 2).alias("avg_pm25"),
    F.round(F.avg("PM10"), 2).alias("avg_pm10"),
    F.round(F.avg("`NO2`"), 2).alias("avg_no2"),
    F.round(F.avg("`SO2`"), 2).alias("avg_so2"),
    F.round(F.avg("`CO`"), 2).alias("avg_co"),
    F.round(F.avg("temperature"), 2).alias("avg_temp"),
    F.round(F.avg("humidity"), 2).alias("avg_humidity"),
    F.round(F.avg("usi"), 2).alias("avg_usi"),
    F.round(F.max("usi"), 2).alias("max_usi"),
    F.round(F.min("usi"), 2).alias("min_usi"),
    F.round(F.stddev("usi"), 2).alias("stddev_usi"),
    F.count("*").alias("total_records"),
    F.sum(F.when(F.col("risk_level") == "Low",       1).otherwise(0)).alias("count_low"),
    F.sum(F.when(F.col("risk_level") == "Moderate",  1).otherwise(0)).alias("count_moderate"),
    F.sum(F.when(F.col("risk_level") == "High",      1).otherwise(0)).alias("count_high"),
    F.sum(F.when(F.col("risk_level") == "Very High", 1).otherwise(0)).alias("count_very_high"),
    F.sum(F.when(F.col("risk_level") == "Severe",    1).otherwise(0)).alias("count_severe"),
)

monthly_trend = df.groupBy("City", "month").agg(
    F.round(F.avg("usi"), 2).alias("avg_usi"),
    F.round(F.avg("AQI"), 2).alias("avg_aqi"),
    F.round(F.avg("temperature"), 2).alias("avg_temp"),
    F.round(F.avg("`PM2.5`"), 2).alias("avg_pm25"),
).orderBy("City", "month")

yearly_trend = df.groupBy("City", "year").agg(
    F.round(F.avg("usi"), 2).alias("avg_usi"),
    F.round(F.avg("AQI"), 2).alias("avg_aqi"),
    F.round(F.avg("`PM2.5`"), 2).alias("avg_pm25"),
).orderBy("City", "year")

stats_rows   = city_stats.collect()
monthly_rows = monthly_trend.collect()
yearly_rows  = yearly_trend.collect()

# ── 5: Isolation Forest anomaly models ───────────────────────────────────────
print("\n[5/8] Training Isolation Forest anomaly models per city...")
city_raw = df.select("City", "AQI", "temperature", "humidity", "usi").collect()
city_data_map = {}
for row in city_raw:
    city_data_map.setdefault(row["City"], []).append(
        [float(row["AQI"] or 0), float(row["temperature"] or 26),
         float(row["humidity"] or 50), float(row["usi"] or 0)]
    )

anomaly_models = {}
for city, data in city_data_map.items():
    if len(data) < 20:
        continue
    X = np.array(data)
    clf = IsolationForest(contamination=0.05, random_state=42, n_estimators=100)
    clf.fit(X)
    scores = clf.decision_function(X)
    threshold = float(np.mean(scores) - 2 * np.std(scores))
    model_b64 = base64.b64encode(pickle.dumps(clf)).decode("utf-8")
    anomaly_models[city] = {
        "threshold":     round(threshold, 6),
        "contamination": 0.05,
        "features":      ["aqi", "temperature", "humidity", "usi"],
        "trained_on":    len(data),
        "mean_score":    round(float(np.mean(scores)), 6),
        "std_score":     round(float(np.std(scores)), 6),
        "model_b64":     model_b64,
    }
    print(f"   {city:<24} | n={len(data):4d} | threshold={threshold:.4f}")

# ── 6: Pearson correlation matrices ──────────────────────────────────────────
print("\n[6/8] Computing Pearson correlation matrices...")
CORR_LABELS = ["AQI", "temperature", "humidity", "usi"]
city_corr_map = {}
for city, data in city_data_map.items():
    if len(data) < 10:
        continue
    arr = np.array(data)
    corr = np.corrcoef(arr.T)
    city_corr_map[city] = {
        "labels": CORR_LABELS,
        "matrix": [[round(float(corr[i][j]), 3) for j in range(4)] for i in range(4)]
    }
print(f"   Matrices: {len(city_corr_map)} cities")

# ── 7: USI Forecast (linear trend extrapolation per month) ───────────────────
print("\n[7/8] Generating 12-month USI forecasts...")
# Build month -> avg_usi per city from monthly trend
from collections import defaultdict
city_monthly_map = defaultdict(dict)
for row in monthly_rows:
    city_monthly_map[row["City"]][row["month"]] = row["avg_usi"]

MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
city_forecast_map = {}
for city, month_data in city_monthly_map.items():
    if len(month_data) < 6:
        continue
    xs = sorted(month_data.keys())
    ys = [month_data[m] for m in xs]
    # Linear trend fit over the 12-month cycle
    x_arr = np.array(xs, dtype=float)
    y_arr = np.array(ys, dtype=float)
    coeffs = np.polyfit(x_arr, y_arr, 1)  # slope, intercept
    slope, intercept = coeffs[0], coeffs[1]
    # Forecast next 12 months (months 13-24 = next year cycle)
    forecast = []
    for step in range(12):
        month_idx = (xs[-1] + step) % 12 + 1
        x_forecast = xs[-1] + step + 1
        predicted = float(slope * x_forecast + intercept)
        # Clamp to realistic range
        predicted = max(10.0, min(95.0, predicted))
        # Add seasonal noise using existing monthly pattern
        seasonal_val = month_data.get(month_idx, predicted)
        blended = round(0.6 * predicted + 0.4 * seasonal_val, 2)
        forecast.append({
            "month": MONTH_NAMES[(month_idx - 1) % 12],
            "month_num": month_idx,
            "predicted_usi": blended,
            "lower_bound":   round(max(0, blended - 5.0), 2),
            "upper_bound":   round(min(100, blended + 5.0), 2),
        })
    city_forecast_map[city] = forecast

print(f"   Forecasts: {len(city_forecast_map)} cities")

# ── 8: Health ranking & MongoDB write ────────────────────────────────────────
print("\n[8/8] Building health ranking & writing to MongoDB...")

usi_vals = [r["avg_usi"] for r in stats_rows if r["avg_usi"] is not None]
usi_min, usi_max = min(usi_vals), max(usi_vals)

def health_score(avg_usi):
    if usi_max == usi_min:
        return 50.0
    return round((1 - (avg_usi - usi_min) / (usi_max - usi_min)) * 100, 1)

client = pymongo.MongoClient(MONGO_URI)
db     = client[DB_NAME]
col    = db[BATCH_COLLECTION]
col.drop()

sorted_by_usi = sorted(stats_rows, key=lambda r: r["avg_usi"])
rank_map = {r["City"]: i + 1 for i, r in enumerate(sorted_by_usi)}

city_docs = {}
for row in stats_rows:
    city = row["City"]
    city_docs[city] = {
        "city":        city,
        "computed_at": datetime.now().isoformat(),
        "layer":       "batch",
        "data_source": "Kaggle India AQ (2015-2020)",
        "usi_formula": {
            "formula":   "USI = (0.5xAQI_norm + 0.3xTemp_norm + 0.2xHum_norm) x 100",
            "weights":   {"aqi": 0.5, "temperature": 0.3, "humidity": 0.2},
            "reference": "WHO AQI Guidelines 2021 / Lancet Countdown 2021"
        },
        "stats": {
            "avg_aqi":       row["avg_aqi"],
            "max_aqi":       row["max_aqi"],
            "min_aqi":       row["min_aqi"],
            "avg_pm25":      row["avg_pm25"],
            "avg_pm10":      row["avg_pm10"],
            "avg_no2":       row["avg_no2"],
            "avg_so2":       row["avg_so2"],
            "avg_co":        row["avg_co"],
            "avg_temp":      row["avg_temp"],
            "avg_humidity":  row["avg_humidity"],
            "avg_usi":       row["avg_usi"],
            "max_usi":       row["max_usi"],
            "min_usi":       row["min_usi"],
            "stddev_usi":    float(row["stddev_usi"]) if row["stddev_usi"] else 0.0,
            "total_records": int(row["total_records"]),
            "health_score":  health_score(row["avg_usi"]),
            "health_rank":   rank_map[city],
        },
        "risk_distribution": {
            "Low":       int(row["count_low"]),
            "Moderate":  int(row["count_moderate"]),
            "High":      int(row["count_high"]),
            "Very High": int(row["count_very_high"]),
            "Severe":    int(row["count_severe"]),
        },
        "anomaly_model": anomaly_models.get(city, {}),
        "correlation":   city_corr_map.get(city, {}),
        "monthly_trend": [],
        "yearly_trend":  [],
        "forecast":      city_forecast_map.get(city, []),
    }

for row in monthly_rows:
    if row["City"] in city_docs:
        city_docs[row["City"]]["monthly_trend"].append({
            "month": row["month"], "avg_usi": row["avg_usi"],
            "avg_aqi": row["avg_aqi"], "avg_temp": row["avg_temp"],
            "avg_pm25": row["avg_pm25"]
        })

for row in yearly_rows:
    if row["City"] in city_docs:
        city_docs[row["City"]]["yearly_trend"].append({
            "year": row["year"], "avg_usi": row["avg_usi"],
            "avg_aqi": row["avg_aqi"], "avg_pm25": row["avg_pm25"]
        })

col.insert_many(list(city_docs.values()))

print(f"\n   Inserted: {len(city_docs)} city documents")
print("\n  Health Ranking (best to worst):")
for i, row in enumerate(sorted_by_usi, 1):
    hs = health_score(row["avg_usi"])
    print(f"  #{i:2d} {row['City']:<22} | Health={hs:5.1f} | AvgUSI={row['avg_usi']:5.2f} | AvgAQI={row['avg_aqi']:6.1f}")

print("\n" + "="*60)
print("  Batch layer complete!")
print("="*60 + "\n")

spark.stop()
client.close()
