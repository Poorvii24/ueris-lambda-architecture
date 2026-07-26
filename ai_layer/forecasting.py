"""
ai_layer/forecasting.py
========================
UERIS — Multi-Horizon Forecasting Engine

Models: RandomForest (always) + XGBoost + LightGBM (optional)
Horizons: 6h, 12h, 24h, 7d
Auto-selects best model by CV RMSE per city × horizon.
"""

import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline

warnings.filterwarnings("ignore")

from ai_layer.feature_engineering import FeatureEngineer
from ai_layer.model_registry import ModelRegistry

HORIZONS     = {"6h":6,"12h":12,"24h":24,"7d":7}
CV_SPLITS    = 3
MIN_SAMPLES  = 30
RANDOM_STATE = 42
SEASON_MAP   = {12:0,1:0,2:0,3:1,4:1,5:1,6:2,7:2,8:2,9:3,10:3,11:3}
RUSH_HOURS   = {7,8,9,17,18,19}


def _build_models():
    models = {}
    models["RandomForest"] = Pipeline([
        ("scaler", StandardScaler()),
        ("model", RandomForestRegressor(
            n_estimators=200, max_depth=8, min_samples_leaf=3,
            random_state=RANDOM_STATE, n_jobs=-1,
        ))
    ])
    try:
        from xgboost import XGBRegressor
        models["XGBoost"] = Pipeline([
            ("scaler", StandardScaler()),
            ("model", XGBRegressor(
                n_estimators=300, max_depth=5, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                random_state=RANDOM_STATE, verbosity=0, n_jobs=-1,
            ))
        ])
    except ImportError:
        pass
    try:
        import lightgbm as lgb
        models["LightGBM"] = Pipeline([
            ("scaler", StandardScaler()),
            ("model", lgb.LGBMRegressor(
                n_estimators=300, max_depth=5, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                random_state=RANDOM_STATE, verbose=-1, n_jobs=-1,
            ))
        ])
    except ImportError:
        pass
    return models


def _metrics(y_true, y_pred, n_samples):
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae  = float(mean_absolute_error(y_true, y_pred))
    r2   = float(r2_score(y_true, y_pred))
    mask = y_true != 0
    mape = float(np.mean(np.abs((y_true[mask]-y_pred[mask])/y_true[mask]))*100) if mask.any() else 999
    return {"rmse":round(rmse,3),"mae":round(mae,3),"mape":round(mape,3),"r2":round(r2,4),"n_samples":n_samples}


class ForecastingEngine:
    def __init__(self, db=None):
        self._db = db
        self._registry = ModelRegistry(db) if db is not None else None
        self._fe = FeatureEngineer()

    def train_city(self, city: str, df: pd.DataFrame) -> dict:
        print(f"  [Forecasting] Training city={city} rows={len(df)}")
        df_feat  = self._fe.transform(df.copy())
        results  = {}
        for horizon_name in HORIZONS:
            results[horizon_name] = self._train_horizon(city, horizon_name, df_feat)
        return results

    def _train_horizon(self, city, horizon_name, df_feat):
        horizon_steps = HORIZONS[horizon_name]
        X, y, feat_cols = self._build_Xy(df_feat, horizon_steps, horizon_name)
        if X is None or len(X) < MIN_SAMPLES:
            return {}
        print(f"    [Forecasting] {horizon_name}: {len(X)} samples, {len(feat_cols)} features")

        models      = _build_models()
        cv          = TimeSeriesSplit(n_splits=CV_SPLITS)
        all_metrics = {}
        best_name, best_rmse = None, float("inf")

        for model_name, pipeline in models.items():
            try:
                cv_rmses = []
                for tr, vl in cv.split(X):
                    pipeline.fit(X[tr], y[tr])
                    cv_rmses.append(np.sqrt(mean_squared_error(y[vl], pipeline.predict(X[vl]))))
                pipeline.fit(X, y)
                m = _metrics(y, pipeline.predict(X), len(X))
                m["cv_rmse_mean"] = round(float(np.mean(cv_rmses)), 3)
                m["cv_rmse_std"]  = round(float(np.std(cv_rmses)), 3)
                m["feature_importance"] = self._get_fi(pipeline, feat_cols)
                all_metrics[model_name] = m
                print(f"      {model_name}: RMSE={m['cv_rmse_mean']:.2f} R²={m['r2']:.3f}")
                if m["cv_rmse_mean"] < best_rmse:
                    best_rmse = m["cv_rmse_mean"]
                    best_name = model_name
                if self._registry:
                    self._registry.save(city=city, model_type="forecaster",
                        model_name=model_name, model=pipeline, metrics=m,
                        feature_cols=feat_cols, horizon=horizon_name,
                        is_best=(model_name==best_name))
            except Exception as e:
                print(f"      {model_name} failed: {e}")
        return all_metrics

    def _build_Xy(self, df, horizon_steps, horizon_name):
        if "7d" in horizon_name:
            d = df.groupby(df["Date"].dt.date).agg(
                AQI=("AQI","mean"), thi=("thi","mean"), eri=("eri","mean"),
                pollution_trend_7d=("pollution_trend_7d","last"),
                aqi_rolling_mean_7d=("aqi_rolling_mean_7d","last"),
                aqi_rolling_std_7d=("aqi_rolling_std_7d","last"),
                month=("month","first"), season=("season","first"),
            ).reset_index()
            target    = d["AQI"].shift(-7)
            feat_cols = [c for c in d.columns if c not in ("Date","index","AQI") and not d[c].isna().all()]
            df_work   = d
        else:
            target    = df["AQI"].shift(-1)
            feat_cols = [c for c in df.columns
                         if c not in ("Date","City","AQI","AQI_Bucket")
                         and df[c].dtype in (np.float64,np.int64,float,int)
                         and not df[c].isna().all()]
            df_work   = df

        df_work = df_work.copy()
        df_work["_target"] = target
        valid = [c for c in feat_cols if c in df_work.columns]
        mask  = df_work["_target"].notna()
        for c in valid: mask &= df_work[c].notna()
        clean = df_work[mask]
        if len(clean) < MIN_SAMPLES:
            return None, None, []
        return clean[valid].values.astype(np.float32), clean["_target"].values.astype(np.float32), valid

    @staticmethod
    def _get_fi(pipeline, feat_cols):
        try:
            m  = pipeline.named_steps["model"]
            if hasattr(m, "feature_importances_"):
                fi = m.feature_importances_
                return {k: round(float(v),4) for k,v in sorted(zip(feat_cols,fi), key=lambda x:x[1], reverse=True)[:15]}
        except: pass
        return {}

    def predict(self, city, horizon="24h", current_reading=None, batch_stats=None):
        if not self._registry:
            return self._fallback(city, horizon, batch_stats)
        model, meta = self._registry.load_best(city, "forecaster", horizon)
        if model is None:
            return self._fallback(city, horizon, batch_stats)
        try:
            feat_cols = meta.get("feature_cols", [])
            X = self._build_inference_row(current_reading, batch_stats, feat_cols)
            if X is None: return self._fallback(city, horizon, batch_stats)
            pred = max(0, round(float(model.predict(X)[0]), 1))
            rmse = meta.get("metrics",{}).get("cv_rmse_mean", 20) or 20
            ci   = round(rmse*1.5, 1)
            return {
                "city": city, "horizon": horizon,
                "predicted_aqi": pred,
                "lower_bound":   round(max(0, pred-ci), 1),
                "upper_bound":   round(pred+ci, 1),
                "confidence_pct": min(99, max(20, int(100-rmse/max(pred,1)*100))),
                "model_used":    meta.get("model_name","unknown"),
                "model_rmse":    rmse,
                "feature_importance": meta.get("metrics",{}).get("feature_importance",{}),
                "source": "ml_model",
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            print(f"[Forecasting] predict failed: {e}")
            return self._fallback(city, horizon, batch_stats)

    def predict_all_horizons(self, city, current_reading=None, batch_stats=None):
        return {h: self.predict(city, h, current_reading, batch_stats) for h in HORIZONS}

    def _build_inference_row(self, reading, batch_stats, feat_cols):
        now   = datetime.now(timezone.utc)
        r     = reading or {}
        s     = batch_stats or {}
        feat  = {
            "hour": now.hour, "day_of_week": now.weekday(),
            "month": now.month, "season": SEASON_MAP.get(now.month,0),
            "is_weekend": int(now.weekday()>=5), "is_rush_hour": int(now.hour in RUSH_HOURS),
            "day_of_year": now.timetuple().tm_yday, "week_of_year": now.isocalendar()[1],
            "year": now.year,
            "aqi_normalized": min((r.get("aqi",100) or 100)/500, 1.0),
            "aqi_rolling_mean_7d":  s.get("avg_aqi",100),
            "aqi_rolling_mean_30d": s.get("avg_aqi",100),
            "aqi_rolling_std_7d":   s.get("stddev_usi",10),
            "aqi_rolling_max_7d":   s.get("max_aqi",150),
            "aqi_lag_1d": r.get("aqi", s.get("avg_aqi",100)),
            "aqi_lag_3d": s.get("avg_aqi",100),
            "aqi_lag_7d": s.get("avg_aqi",100),
            "pollution_trend_7d": 0.0, "trend_direction": 0,
            "aqi_delta_1d": 0.0, "aqi_pct_change_1d": 0.0,
            "aqi_zscore_30d": 0.0, "pm_ratio": 0.5,
        }
        temp = float(r.get("temperature",26) or 26)
        hum  = float(r.get("humidity",50) or 50)
        feat["thi"] = round(temp - 0.55*(1-0.01*hum)*(temp-14.5), 2)
        feat["eri"] = round(min(float(r.get("aqi",100) or 100)/300,1)*50 + min(max((temp-20)/20,0),1)*30 + abs(hum-50)/50*20, 2)
        feat["temp_hum_interaction"] = temp*(1-hum/100)
        row = [feat.get(c, 0.0) or 0.0 for c in feat_cols]
        return np.array([row], dtype=np.float32)

    def _fallback(self, city, horizon, batch_stats=None):
        s = batch_stats or {}
        avg = s.get("avg_aqi",100) or 100
        std = s.get("stddev_usi",15) or 15
        return {
            "city": city, "horizon": horizon,
            "predicted_aqi": round(avg,1),
            "lower_bound":   round(max(0,avg-std),1),
            "upper_bound":   round(avg+std,1),
            "confidence_pct": 45,
            "model_used": "historical_average",
            "model_rmse": None,
            "feature_importance": {},
            "source": "fallback",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def get_model_comparison(self, city, horizon="24h"):
        if not self._registry: return []
        return [
            {
                "model":    d.get("model_name"), "rmse": d.get("metrics",{}).get("rmse"),
                "mae":      d.get("metrics",{}).get("mae"), "r2": d.get("metrics",{}).get("r2"),
                "cv_rmse":  d.get("metrics",{}).get("cv_rmse_mean"),
                "is_best":  d.get("is_best",False), "trained_at": d.get("trained_at"),
                "n_samples":d.get("n_samples"),
            }
            for d in self._registry.get_all_metrics(city, "forecaster", horizon)
        ]
