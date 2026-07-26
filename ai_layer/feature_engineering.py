"""
ai_layer/feature_engineering.py
================================
UERIS — Advanced Feature Engineering

Transforms raw environmental readings into ML-ready feature vectors.
"""

import numpy as np
import pandas as pd
from typing import List

ROLLING_WINDOWS = [3, 7, 14, 30]
LAG_DAYS        = [1, 3, 7]
SEASON_MAP      = {12:0,1:0,2:0,3:1,4:1,5:1,6:2,7:2,8:2,9:3,10:3,11:3}
RUSH_HOURS      = {7,8,9,17,18,19}


class FeatureEngineer:
    def __init__(self):
        self.feature_names_: List[str] = []
        self._fitted = False

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = self._ensure_datetime(df)
        df = df.sort_values("Date")
        df = self._time_features(df)
        df = self._environmental_indices(df)
        df = self._rolling_features(df)
        df = self._lag_features(df)
        df = self._trend_features(df)
        df = self._composition_features(df)
        df = self._interaction_features(df)
        raw = {"Date","City","AQI","PM2.5","PM10","NO","NO2","NOx",
               "NH3","CO","SO2","O3","Benzene","Toluene","Xylene","AQI_Bucket"}
        self.feature_names_ = [c for c in df.columns if c not in raw]
        self._fitted = True
        return df

    def transform_realtime(self, record: dict) -> dict:
        from datetime import datetime, timezone
        ts  = record.get("timestamp", datetime.now(timezone.utc).isoformat())
        ts  = pd.Timestamp(ts)
        aqi = float(record.get("aqi", 0) or 0)
        temp= float(record.get("temperature", 26) or 26)
        hum = float(record.get("humidity", 50) or 50)
        record["hour"]         = ts.hour
        record["day_of_week"]  = ts.dayofweek
        record["month"]        = ts.month
        record["season"]       = SEASON_MAP.get(ts.month, 0)
        record["is_weekend"]   = int(ts.dayofweek >= 5)
        record["is_rush_hour"] = int(ts.hour in RUSH_HOURS)
        record["thi"]          = self._compute_thi(temp, hum)
        record["eri"]          = self._compute_eri(aqi, temp, hum)
        record["temp_hum_interaction"] = temp * (1 - hum / 100)
        record["aqi_temp_interaction"] = (aqi/300) * min((temp-15)/25, 1.0)
        return record

    def _time_features(self, df):
        df["hour"]         = df["Date"].dt.hour
        df["day_of_week"]  = df["Date"].dt.dayofweek
        df["month"]        = df["Date"].dt.month
        df["day_of_year"]  = df["Date"].dt.dayofyear
        df["week_of_year"] = df["Date"].dt.isocalendar().week.astype(int)
        df["season"]       = df["month"].map(SEASON_MAP)
        df["is_weekend"]   = (df["day_of_week"] >= 5).astype(int)
        df["is_rush_hour"] = df["hour"].apply(lambda h: int(h in RUSH_HOURS))
        df["year"]         = df["Date"].dt.year
        return df

    def _environmental_indices(self, df):
        if "temperature" not in df.columns: df["temperature"] = 26.0
        if "humidity"    not in df.columns: df["humidity"]    = 50.0
        df["thi"] = df.apply(lambda r: self._compute_thi(r["temperature"], r["humidity"]), axis=1)
        df["eri"] = df.apply(lambda r: self._compute_eri(r.get("AQI",0), r["temperature"], r["humidity"]), axis=1)
        df["aqi_deviation"] = df["AQI"] - df["AQI"].mean()
        return df

    @staticmethod
    def _compute_thi(temperature, humidity):
        t, h = float(temperature), float(humidity)
        return round(t - 0.55*(1-0.01*h)*(t-14.5), 2)

    @staticmethod
    def _compute_eri(aqi, temperature, humidity):
        return round(min(float(aqi)/300,1)*50 + min(max((float(temperature)-20)/20,0),1)*30 + abs(float(humidity)-50)/50*20, 2)

    def _rolling_features(self, df):
        for w in ROLLING_WINDOWS:
            roll = df["AQI"].rolling(w, min_periods=1)
            df[f"aqi_rolling_mean_{w}d"] = roll.mean().round(2)
            df[f"aqi_rolling_std_{w}d"]  = roll.std().round(2).fillna(0)
            df[f"aqi_rolling_max_{w}d"]  = roll.max().round(2)
            df[f"temp_rolling_mean_{w}d"] = df["temperature"].rolling(w, min_periods=1).mean().round(2)
            df[f"hum_rolling_mean_{w}d"]  = df["humidity"].rolling(w, min_periods=1).mean().round(2)
        mean_30 = df["AQI"].rolling(30, min_periods=5).mean()
        std_30  = df["AQI"].rolling(30, min_periods=5).std().replace(0,1)
        df["aqi_zscore_30d"] = ((df["AQI"]-mean_30)/std_30).round(3)
        return df

    def _lag_features(self, df):
        for lag in LAG_DAYS:
            df[f"aqi_lag_{lag}d"] = df["AQI"].shift(lag)
        df["aqi_delta_1d"]      = df["AQI"] - df["AQI"].shift(1)
        df["aqi_pct_change_1d"] = df["AQI"].pct_change(1).round(4)*100
        return df

    def _trend_features(self, df):
        aqi_vals = df["AQI"].values
        slopes   = []
        for i in range(len(aqi_vals)):
            w = aqi_vals[max(0,i-6):i+1]
            slopes.append(round(np.polyfit(np.arange(len(w)),w,1)[0],4) if len(w)>=2 else 0.0)
        df["pollution_trend_7d"] = slopes
        df["trend_direction"]    = np.sign(df["pollution_trend_7d"]).astype(int)
        return df

    def _composition_features(self, df):
        if "PM2.5" in df.columns and "PM10" in df.columns:
            df["pm_ratio"] = (df["PM2.5"]/(df["PM10"].replace(0,np.nan))).round(3).fillna(0)
        if "NO" in df.columns and "NO2" in df.columns:
            df["nox_proxy"] = (df["NO"].fillna(0)+df["NO2"].fillna(0)).round(2)
        if "O3" in df.columns and "NO2" in df.columns:
            df["oxidant_load"] = (df["O3"].fillna(0)+df["NO2"].fillna(0)).round(2)
        df["aqi_normalized"] = (df["AQI"]/500).clip(0,1).round(4)
        return df

    def _interaction_features(self, df):
        df["temp_hum_interaction"] = (df["temperature"]*(1-df["humidity"]/100)).round(3)
        temp_norm = ((df["temperature"]-15)/25).clip(0,1)
        df["aqi_temp_interaction"] = (df["aqi_normalized"]*temp_norm).round(4)
        return df

    @staticmethod
    def _ensure_datetime(df):
        if "Date" not in df.columns:
            df = df.reset_index().rename(columns={"index":"Date"})
        df["Date"] = pd.to_datetime(df["Date"])
        return df
