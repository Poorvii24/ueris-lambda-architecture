"""
batch_layer/ai_batch_processor.py
===================================
UERIS — AI Batch Processor

Trains all AI models for all 26 cities and stores results in MongoDB Atlas.
Runs alongside batch_processing.py (or independently after it).

What it does:
  1. Loads city_day.csv (real Kaggle dataset)
  2. Per city:
     a. Feature engineering (FeatureEngineer)
     b. Train forecasting models (RandomForest + XGBoost + LightGBM)
     c. Train anomaly ensemble (IF + LOF + One-Class SVM)
     d. Compute trend intelligence profile
     e. Generate city insights
     f. Store everything in MongoDB
  3. Generate system-wide insight summary
  4. Update batch_views with AI enrichment fields

Run:
    py -3.11 batch_layer/ai_batch_processor.py

Environment variables:
    MONGO_URI         MongoDB connection string
    CSV_PATH          Path to city_day.csv
    CITIES_FILTER     Comma-separated city names (optional, default=all)
    SKIP_FORECASTING  Set to '1' to skip slow model training
    SKIP_ANOMALY      Set to '1' to skip anomaly ensemble training
"""

import os
import sys
import time
import base64
import pickle
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pymongo

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ["PYSPARK_PYTHON"]        = os.environ.get("PYSPARK_PYTHON", sys.executable)
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ.get("PYSPARK_DRIVER_PYTHON", sys.executable)

from ai_layer.feature_engineering import FeatureEngineer
from ai_layer.forecasting         import ForecastingEngine
from ai_layer.anomaly_ensemble    import AnomalyEnsemble
from ai_layer.trend_intelligence  import TrendIntelligence
from ai_layer.insight_generator   import InsightGenerator
from ai_layer.model_registry      import ModelRegistry

# ── Config ─────────────────────────────────────────────────────────────────────
MONGO_URI        = os.environ.get("MONGO_URI",  "mongodb://localhost:27017/")
DB_NAME          = os.environ.get("DB_NAME",    "urban_env_db")
CSV_PATH         = os.environ.get("CSV_PATH",
    os.path.join(os.path.dirname(__file__), "../data/historical/city_day.csv"))
CITIES_FILTER    = os.environ.get("CITIES_FILTER", "")
SKIP_FORECASTING = os.environ.get("SKIP_FORECASTING", "0") == "1"
SKIP_ANOMALY     = os.environ.get("SKIP_ANOMALY",     "0") == "1"
MIN_ROWS_CITY    = 30    # skip cities with fewer rows

ALL_CITIES = [
    "Ahmedabad","Aizawl","Amaravati","Amritsar","Bengaluru","Bhopal",
    "Brajrajnagar","Chandigarh","Chennai","Coimbatore","Delhi","Ernakulam",
    "Gurugram","Guwahati","Hyderabad","Jaipur","Jorapokhar","Kochi",
    "Kolkata","Lucknow","Mumbai","Patna","Shillong","Talcher",
    "Thiruvananthapuram","Visakhapatnam",
]

TEMP_BASELINES = {
    "Delhi":28,"Mumbai":29,"Bengaluru":24,"Chennai":30,"Hyderabad":28,
    "Kolkata":27,"Ahmedabad":28,"Jaipur":27,"Lucknow":26,"Patna":26,
    "Chandigarh":23,"Amritsar":22,"Gurugram":28,"Guwahati":25,
    "Bhopal":26,"Coimbatore":27,"Kochi":28,"Ernakulam":28,
    "Visakhapatnam":28,"Thiruvananthapuram":28,"Shillong":17,
    "Aizawl":18,"Amaravati":29,"Jorapokhar":24,"Brajrajnagar":25,"Talcher":27,
}


def compute_usi(aqi, temp, hum):
    return round(
        (min(aqi/300,1)*0.5 + min(max((temp-15)/25,0),1)*0.3 + abs(hum-50)/50*0.2)*100, 2
    )


def add_temperature_humidity(df, city):
    """Estimate temperature and humidity if not present in dataset."""
    import math
    base = TEMP_BASELINES.get(city, 26)
    if "temperature" not in df.columns:
        df["temperature"] = df["Date"].dt.month.map(
            lambda m: round(base + 5 * math.sin((m - 3) * math.pi / 6), 1)
        )
    if "humidity" not in df.columns:
        df["humidity"] = df.apply(
            lambda r: round(
                max(20, min(95,
                    (70 - (r["temperature"] - 25) * 0.8)
                    if 6 <= r["Date"].month <= 9
                    else (55 - (r["temperature"] - 25) * 1.2)
                )), 1
            ), axis=1
        )
    return df


def add_usi(df):
    """Compute USI column if not present."""
    if "usi" not in df.columns:
        df["usi"] = df.apply(
            lambda r: compute_usi(r["AQI"], r["temperature"], r["humidity"]),
            axis=1
        )
    return df


def main():
    t_total = time.monotonic()
    print("\n" + "="*65)
    print("  UERIS AI Batch Processor")
    print(f"  MongoDB : {MONGO_URI[:50]}...")
    print(f"  CSV     : {CSV_PATH}")
    print(f"  Models  : Forecasting={'OFF' if SKIP_FORECASTING else 'ON'} "
          f"Anomaly={'OFF' if SKIP_ANOMALY else 'ON'}")
    print("="*65 + "\n")

    # ── Connect to MongoDB ─────────────────────────────────────────────────────
    client   = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    db       = client[DB_NAME]
    registry = ModelRegistry(db)

    # ── Load dataset ───────────────────────────────────────────────────────────
    print("[1/5] Loading Kaggle dataset...")
    df_all = pd.read_csv(CSV_PATH)
    df_all["Date"] = pd.to_datetime(df_all["Date"])
    df_all = df_all[df_all["AQI"].notna() & (df_all["AQI"] > 0)]

    cities = (
        [c.strip() for c in CITIES_FILTER.split(",") if c.strip()]
        if CITIES_FILTER
        else ALL_CITIES
    )
    print(f"      Total records: {len(df_all):,} | Cities to process: {len(cities)}")

    # ── Initialise AI components ───────────────────────────────────────────────
    fe          = FeatureEngineer()
    fc_engine   = ForecastingEngine(db)
    trend_intel = TrendIntelligence()
    insight_gen = InsightGenerator()

    # ── Per-city processing ────────────────────────────────────────────────────
    print("\n[2/5] Processing cities...\n")
    city_results = []
    failed_cities = []

    for i, city in enumerate(cities, 1):
        t_city = time.monotonic()
        print(f"  [{i:02d}/{len(cities):02d}] {city}")

        try:
            df_city = df_all[df_all["City"] == city].copy().reset_index(drop=True)

            if len(df_city) < MIN_ROWS_CITY:
                print(f"         Skipping — only {len(df_city)} rows (min {MIN_ROWS_CITY})")
                continue

            df_city = add_temperature_humidity(df_city, city)
            df_city = add_usi(df_city)

            # ── Feature Engineering ────────────────────────────────────────────
            try:
                df_feat = fe.transform(df_city.copy())
                print(f"         Features: {len(fe.feature_names_)} engineered")
            except Exception as e:
                print(f"         Feature engineering failed: {e}")
                df_feat = df_city.copy()

            # ── Forecasting Models ─────────────────────────────────────────────
            fc_results = {}
            if not SKIP_FORECASTING:
                try:
                    fc_results = fc_engine.train_city(city, df_feat)
                except Exception as e:
                    print(f"         Forecasting failed: {e}")

            # ── Anomaly Ensemble ───────────────────────────────────────────────
            anomaly_model_doc = {}
            if not SKIP_ANOMALY:
                try:
                    X_anom = df_city[["AQI","temperature","humidity","usi"]].dropna().values
                    if len(X_anom) >= 20:
                        ensemble = AnomalyEnsemble(contamination=0.05)
                        ensemble.fit(
                            X_anom,
                            feature_names=["aqi","temperature","humidity","usi"]
                        )
                        # Evaluate
                        eval_stats = ensemble.evaluate(X_anom)
                        # Serialize
                        model_b64 = base64.b64encode(pickle.dumps(ensemble)).decode("utf-8")
                        anomaly_model_doc = {
                            "model_b64":      model_b64,
                            "model_type":     "AnomalyEnsemble",
                            "models":         ["IsolationForest","LOF","OneClassSVM"],
                            "contamination":  0.05,
                            "n_train":        len(X_anom),
                            "eval_stats":     eval_stats,
                            "trained_at":     datetime.now(timezone.utc).isoformat(),
                        }
                        # Also save to registry
                        registry.save(
                            city=city,
                            model_type="anomaly_ensemble",
                            model_name="AnomalyEnsemble",
                            model=ensemble,
                            metrics=eval_stats,
                            feature_cols=["aqi","temperature","humidity","usi"],
                            horizon="realtime",
                            is_best=True,
                        )
                        print(f"         Anomaly ensemble: trained on {len(X_anom)} samples, "
                              f"anomaly rate={eval_stats.get('anomaly_rate_pct','?')}%")
                    else:
                        print(f"         Anomaly: insufficient valid rows ({len(X_anom)})")
                except Exception as e:
                    print(f"         Anomaly ensemble failed: {e}")

            # ── Trend Intelligence ─────────────────────────────────────────────
            try:
                trend_profile = trend_intel.analyse_city(city, df_city)
            except Exception as e:
                print(f"         Trend intelligence failed: {e}")
                trend_profile = {}

            # ── City Insight ───────────────────────────────────────────────────
            try:
                batch_doc = db["batch_views"].find_one({"city": city}, {"_id": 0})
                if batch_doc:
                    insight = insight_gen.generate_city_insight(
                        city=city,
                        batch_stats=batch_doc.get("stats", {}),
                        trend_profile=trend_profile,
                    )
                else:
                    insight = {}
            except Exception as e:
                print(f"         Insight generation failed: {e}")
                insight = {}

            # ── Update MongoDB batch_views ─────────────────────────────────────
            update_doc = {
                "ai_enriched_at":     datetime.now(timezone.utc).isoformat(),
                "trend_profile":      trend_profile,
                "city_insight":       insight,
                "forecasting_models": {
                    horizon: {
                        model_name: {
                            "rmse":     m.get("cv_rmse_mean"),
                            "mae":      m.get("cv_mae_mean"),
                            "r2":       m.get("r2"),
                            "n_samples":m.get("n_samples"),
                        }
                        for model_name, m in horizon_results.items()
                    }
                    for horizon, horizon_results in fc_results.items()
                },
            }

            if anomaly_model_doc:
                update_doc["anomaly_model"] = anomaly_model_doc

            db["batch_views"].update_one(
                {"city": city},
                {"$set": update_doc},
                upsert=False,
            )

            # Also store trend profile separately for fast API access
            db["trend_profiles"].update_one(
                {"city": city},
                {"$set": {**trend_profile, "updated_at": datetime.now(timezone.utc).isoformat()}},
                upsert=True,
            )

            elapsed = round(time.monotonic() - t_city, 1)
            print(f"         ✓ Complete in {elapsed}s")
            city_results.append({"city": city, "success": True, "elapsed_s": elapsed})

        except Exception as e:
            print(f"         ✗ FAILED: {e}")
            failed_cities.append(city)
            city_results.append({"city": city, "success": False, "error": str(e)})

    # ── System-wide insight ────────────────────────────────────────────────────
    print("\n[3/5] Generating system-wide insight...")
    try:
        all_batch = list(db["batch_views"].find({}, {"_id": 0, "city": 1, "stats": 1}))
        all_usies = [b.get("stats", {}).get("avg_usi", 0) for b in all_batch if b.get("stats")]
        sys_avg_usi = float(np.mean(all_usies)) if all_usies else 40.0

        cities_data = []
        for b in all_batch:
            rt = db["realtime_views"].find_one({"city": b["city"]}, {"_id": 0})
            cities_data.append({
                "city":         b["city"],
                "current_usi":  rt.get("usi") if rt else b.get("stats", {}).get("avg_usi"),
                "current_aqi":  rt.get("aqi") if rt else b.get("stats", {}).get("avg_aqi"),
                "current_risk": rt.get("risk_level") if rt else "Moderate",
                "is_anomaly":   rt.get("is_anomaly", False) if rt else False,
                "avg_usi":      b.get("stats", {}).get("avg_usi"),
            })

        system_insight = insight_gen.generate_system_insight(cities_data, sys_avg_usi)
        db["system_insights"].update_one(
            {"type": "latest"},
            {"$set": {**system_insight, "type": "latest"}},
            upsert=True,
        )
        print(f"      System insight generated: avg_usi={sys_avg_usi:.1f}, "
              f"anomalies={system_insight.get('anomaly_count', 0)}")
    except Exception as e:
        print(f"      System insight failed: {e}")

    # ── Model registry summary ─────────────────────────────────────────────────
    print("\n[4/5] Model registry summary...")
    reg_summary = registry.get_registry_summary()
    print(f"      Total models stored : {reg_summary.get('total_models', 0)}")
    print(f"      Cities with models  : {reg_summary.get('cities_with_models', 0)}")
    print(f"      Model types         : {reg_summary.get('model_types', [])}")

    # ── Final summary ──────────────────────────────────────────────────────────
    print("\n[5/5] Summary")
    successful = sum(1 for r in city_results if r["success"])
    elapsed    = round(time.monotonic() - t_total, 1)

    print(f"\n{'='*65}")
    print(f"  AI Batch Processing Complete")
    print(f"  Cities processed : {successful}/{len(cities)}")
    print(f"  Failed           : {len(failed_cities)}{': ' + str(failed_cities) if failed_cities else ''}")
    print(f"  Total time       : {elapsed}s ({elapsed/60:.1f} min)")
    print(f"  MongoDB updated  : batch_views, trend_profiles, system_insights, ai_models")
    print(f"{'='*65}\n")

    client.close()


if __name__ == "__main__":
    main()