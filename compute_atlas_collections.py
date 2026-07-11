"""
compute_atlas_collections.py
=============================
Run this ONCE locally to compute and push all missing collections to MongoDB Atlas.
It reads from city_day.csv and batch_views, then computes:
  - correlations      (Pearson correlation per city)
  - data_quality      (coverage, freshness, completeness stats)
  - yearly_trends     (already in batch_views but we fix it here too)

Run:
    py -3.11 compute_atlas_collections.py
"""

import pymongo
import csv
import math
import os
from datetime import datetime
from collections import defaultdict

ATLAS_URI = "mongodb+srv://uerisadmin:StrongPassword123@cluster0.5motp8l.mongodb.net/?appName=Cluster0"
DB_NAME   = "urban_env_db"
CSV_PATH  = os.path.join(os.path.dirname(__file__), "data/historical/city_day.csv")

print("\n" + "="*60)
print("  Computing missing collections for MongoDB Atlas")
print("="*60)

client = pymongo.MongoClient(ATLAS_URI)
db     = client[DB_NAME]

# ── Load CSV ──────────────────────────────────────────────────
print("\n[1/4] Loading Kaggle dataset...")
rows_by_city = defaultdict(list)
total = 0
with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            aqi = float(row["AQI"]) if row["AQI"] else None
            if aqi and aqi > 0:
                rows_by_city[row["City"]].append({
                    "city":    row["City"],
                    "date":    row["Date"],
                    "aqi":     aqi,
                    "pm25":    float(row["PM2.5"]) if row["PM2.5"] else None,
                    "pm10":    float(row["PM10"])  if row["PM10"]  else None,
                    "no2":     float(row["NO2"])   if row["NO2"]   else None,
                    "so2":     float(row["SO2"])   if row["SO2"]   else None,
                    "co":      float(row["CO"])    if row["CO"]    else None,
                    "o3":      float(row["O3"])    if row["O3"]    else None,
                })
                total += 1
        except:
            pass
print(f"      Loaded {total:,} valid records for {len(rows_by_city)} cities")

# ── Helper: Pearson Correlation ───────────────────────────────
def pearson(x, y):
    n = len(x)
    if n < 2:
        return 0
    mx, my = sum(x)/n, sum(y)/n
    num = sum((xi-mx)*(yi-my) for xi,yi in zip(x,y))
    dx  = math.sqrt(sum((xi-mx)**2 for xi in x))
    dy  = math.sqrt(sum((yi-my)**2 for yi in y))
    if dx == 0 or dy == 0:
        return 0
    return round(num/(dx*dy), 4)

def safe_avg(vals):
    v = [x for x in vals if x is not None]
    return round(sum(v)/len(v), 2) if v else None

def safe_count(vals):
    return sum(1 for x in vals if x is not None)

# ── City temp baseline for USI ────────────────────────────────
TEMP_BASE = {
    'Delhi':28,'Mumbai':29,'Bengaluru':24,'Chennai':30,'Hyderabad':28,
    'Kolkata':27,'Ahmedabad':28,'Jaipur':27,'Lucknow':26,'Patna':26,
    'Chandigarh':23,'Amritsar':22,'Gurugram':28,'Guwahati':25,
    'Bhopal':26,'Coimbatore':27,'Kochi':28,'Ernakulam':28,
    'Visakhapatnam':28,'Thiruvananthapuram':28,'Shillong':17,
    'Aizawl':18,'Amaravati':29,'Jorapokhar':24,'Brajrajnagar':25,'Talcher':27
}

def estimate_temp(city, month):
    base = TEMP_BASE.get(city, 26)
    return round(base + 5 * math.sin((month - 3) * math.pi / 6), 1)

def estimate_humidity(temp, month):
    if 6 <= month <= 9:
        h = 70 - (temp - 25) * 0.8
    else:
        h = 55 - (temp - 25) * 1.2
    return round(max(20, min(95, h)), 1)

def compute_usi(aqi, temp, hum):
    a = min(aqi/300, 1.0)
    t = min(max((temp-15)/25, 0), 1.0)
    h = abs(hum-50)/50
    return round((0.5*a + 0.3*t + 0.2*h)*100, 2)

# ── [2] CORRELATIONS ─────────────────────────────────────────
print("\n[2/4] Computing Pearson correlations...")
corr_docs = []
for city, rows in rows_by_city.items():
    aqis, temps, hums, usis = [], [], [], []
    pm25s, pm10s, no2s = [], [], []
    for r in rows:
        month = int(r["date"][5:7]) if r["date"] else 6
        temp  = estimate_temp(city, month)
        hum   = estimate_humidity(temp, month)
        usi   = compute_usi(r["aqi"], temp, hum)
        aqis.append(r["aqi"]); temps.append(temp)
        hums.append(hum); usis.append(usi)
        if r["pm25"]: pm25s.append(r["pm25"])
        if r["pm10"]: pm10s.append(r["pm10"])
        if r["no2"]:  no2s.append(r["no2"])

    corr_docs.append({
        "city": city,
        "computed_at": datetime.now().isoformat(),
        "correlations": {
            "aqi_vs_usi":   pearson(aqis, usis),
            "aqi_vs_temp":  pearson(aqis, temps),
            "aqi_vs_hum":   pearson(aqis, hums),
            "temp_vs_usi":  pearson(temps, usis),
            "hum_vs_usi":   pearson(hums, usis),
            "temp_vs_hum":  pearson(temps, hums),
            "pm25_vs_aqi":  pearson(pm25s, aqis[:len(pm25s)]) if pm25s else None,
            "pm10_vs_aqi":  pearson(pm10s, aqis[:len(pm10s)]) if pm10s else None,
            "no2_vs_aqi":   pearson(no2s,  aqis[:len(no2s)])  if no2s  else None,
        },
        "sample_size": len(rows)
    })

db["correlations"].drop()
db["correlations"].insert_many(corr_docs)
print(f"      Inserted {len(corr_docs)} correlation documents")

# ── [3] DATA QUALITY ─────────────────────────────────────────
print("\n[3/4] Computing data quality metrics...")
quality_docs = []
all_cities = list(rows_by_city.keys())

for city in all_cities:
    rows = rows_by_city[city]
    n    = len(rows)
    aqis  = [r["aqi"]  for r in rows]
    pm25s = [r["pm25"] for r in rows]
    pm10s = [r["pm10"] for r in rows]
    no2s  = [r["no2"]  for r in rows]
    dates = sorted([r["date"] for r in rows if r["date"]])

    aqi_coverage  = round(safe_count(aqis)  / n * 100, 1) if n else 0
    pm25_coverage = round(safe_count(pm25s) / n * 100, 1) if n else 0
    pm10_coverage = round(safe_count(pm10s) / n * 100, 1) if n else 0
    no2_coverage  = round(safe_count(no2s)  / n * 100, 1) if n else 0
    overall       = round((aqi_coverage + pm25_coverage + pm10_coverage + no2_coverage) / 4, 1)

    quality_docs.append({
        "city": city,
        "computed_at": datetime.now().isoformat(),
        "total_records": n,
        "date_range": {
            "start": dates[0]  if dates else None,
            "end":   dates[-1] if dates else None,
        },
        "coverage": {
            "aqi":     aqi_coverage,
            "pm25":    pm25_coverage,
            "pm10":    pm10_coverage,
            "no2":     no2_coverage,
            "overall": overall,
        },
        "statistics": {
            "avg_aqi":  safe_avg(aqis),
            "max_aqi":  round(max(aqis), 1) if aqis else None,
            "min_aqi":  round(min(aqis), 1) if aqis else None,
            "avg_pm25": safe_avg([x for x in pm25s if x]),
            "avg_pm10": safe_avg([x for x in pm10s if x]),
        },
        "quality_score": overall
    })

db["data_quality"].drop()
db["data_quality"].insert_many(quality_docs)
print(f"      Inserted {len(quality_docs)} data quality documents")

# ── [4] FIX YEARLY TRENDS in batch_views ─────────────────────
print("\n[4/4] Fixing yearly_trend in batch_views...")
yearly_by_city = defaultdict(lambda: defaultdict(list))
for city, rows in rows_by_city.items():
    for r in rows:
        try:
            year  = int(r["date"][:4])
            month = int(r["date"][5:7])
            temp  = estimate_temp(city, month)
            hum   = estimate_humidity(temp, month)
            usi   = compute_usi(r["aqi"], temp, hum)
            yearly_by_city[city][year].append({"aqi": r["aqi"], "usi": usi})
        except:
            pass

fixed = 0
for city, years in yearly_by_city.items():
    yearly_trend = []
    for year in sorted(years.keys()):
        entries = years[year]
        avg_aqi = round(sum(e["aqi"] for e in entries) / len(entries), 2)
        avg_usi = round(sum(e["usi"] for e in entries) / len(entries), 2)
        yearly_trend.append({"year": year, "avg_aqi": avg_aqi, "avg_usi": avg_usi})

    result = db["batch_views"].update_one(
        {"city": city},
        {"$set": {"yearly_trend": yearly_trend}}
    )
    if result.modified_count > 0:
        fixed += 1
        print(f"      {city}: {len(yearly_trend)} years of data")

print(f"\n      Fixed yearly_trend for {fixed} cities")

# ── SUMMARY ───────────────────────────────────────────────────
print("\n" + "="*60)
print("  All collections pushed to MongoDB Atlas!")
print(f"  Collections: {db.list_collection_names()}")
print("="*60 + "\n")

client.close()
