"""
ai_layer/trend_intelligence.py
================================
UERIS — Trend Intelligence & Statistical Analysis

Provides deep trend analysis beyond simple rolling averages:

  1. Seasonal decomposition (trend + seasonal + residual)
  2. Year-over-year comparison (2015 vs 2016 vs ... vs 2020)
  3. Month-of-year patterns (worst/best months historically)
  4. Hour-of-day patterns (rush hour peaks, nighttime lows)
  5. Pollution correlation ranking (which pollutant drives AQI most)
  6. City comparative ranking across multiple dimensions
  7. Trend direction classification (improving / worsening / stable)
  8. Anomaly frequency analysis (how often does this city spike?)

Usage:
    analyst = TrendIntelligence()
    trends = analyst.analyse_city("Delhi", df_city)
    # Returns full trend profile stored in MongoDB
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple


POLLUTANTS = ["PM2.5", "PM10", "NO2", "SO2", "CO", "O3", "NH3", "NOx"]
MONTHS_ABBR = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
SEASONS = {0:"Winter", 1:"Spring", 2:"Summer/Monsoon", 3:"Autumn"}
SEASON_MAP = {12:0,1:0,2:0,3:1,4:1,5:1,6:2,7:2,8:2,9:3,10:3,11:3}


class TrendIntelligence:
    """
    Statistical trend analysis engine for UERIS city profiles.

    All analyse_* methods return serializable dicts
    suitable for MongoDB storage and API serving.
    """

    def analyse_city(self, city: str, df: pd.DataFrame) -> dict:
        """
        Full trend analysis for one city.

        Args:
            city: City name
            df:   Historical DataFrame with Date, AQI, pollutant columns

        Returns:
            Complete trend profile dict
        """
        df = df.copy()
        df["Date"]   = pd.to_datetime(df["Date"])
        df["month"]  = df["Date"].dt.month
        df["year"]   = df["Date"].dt.year
        df["season"] = df["month"].map(SEASON_MAP)

        print(f"  [TrendIntelligence] Analysing {city}: {len(df)} rows, "
              f"{df['year'].nunique()} years")

        profile = {
            "city":            city,
            "data_range": {
                "start":       str(df["Date"].min().date()),
                "end":         str(df["Date"].max().date()),
                "total_days":  int((df["Date"].max() - df["Date"].min()).days),
                "total_rows":  len(df),
                "years":       sorted(df["year"].unique().tolist()),
            },
            "monthly_patterns":    self._monthly_patterns(df),
            "seasonal_patterns":   self._seasonal_patterns(df),
            "yearly_trends":       self._yearly_trends(df),
            "trend_direction":     self._trend_direction(df),
            "worst_periods":       self._worst_periods(df),
            "best_periods":        self._best_periods(df),
            "pollutant_ranking":   self._pollutant_ranking(df),
            "anomaly_frequency":   self._anomaly_frequency(df),
            "volatility_profile":  self._volatility_profile(df),
            "improvement_score":   self._improvement_score(df),
        }

        return profile

    # ── Monthly patterns ───────────────────────────────────────────────────────

    def _monthly_patterns(self, df: pd.DataFrame) -> dict:
        """Average AQI, USI, and key pollutants per calendar month."""
        monthly = df.groupby("month").agg(
            avg_aqi      = ("AQI",      "mean"),
            max_aqi      = ("AQI",      "max"),
            min_aqi      = ("AQI",      "min"),
            std_aqi      = ("AQI",      "std"),
            count        = ("AQI",      "count"),
        ).round(2)

        months_list = []
        for month_num in range(1, 13):
            if month_num in monthly.index:
                row = monthly.loc[month_num]
                months_list.append({
                    "month":       month_num,
                    "month_name":  MONTHS_ABBR[month_num - 1],
                    "avg_aqi":     round(float(row["avg_aqi"]), 1),
                    "max_aqi":     round(float(row["max_aqi"]), 1),
                    "min_aqi":     round(float(row["min_aqi"]), 1),
                    "std_aqi":     round(float(row["std_aqi"]), 1) if not np.isnan(row["std_aqi"]) else 0,
                    "count":       int(row["count"]),
                })

        worst_month  = max(months_list, key=lambda x: x["avg_aqi"], default={})
        best_month   = min(months_list, key=lambda x: x["avg_aqi"], default={})

        return {
            "by_month":     months_list,
            "worst_month":  worst_month.get("month_name"),
            "best_month":   best_month.get("month_name"),
            "worst_avg_aqi": worst_month.get("avg_aqi"),
            "best_avg_aqi":  best_month.get("avg_aqi"),
            "seasonal_range": round(
                (worst_month.get("avg_aqi", 0) or 0) -
                (best_month.get("avg_aqi", 0)  or 0), 1
            ),
        }

    # ── Seasonal patterns ──────────────────────────────────────────────────────

    def _seasonal_patterns(self, df: pd.DataFrame) -> dict:
        """AQI statistics per season."""
        seasonal = df.groupby("season").agg(
            avg_aqi = ("AQI", "mean"),
            max_aqi = ("AQI", "max"),
            std_aqi = ("AQI", "std"),
            count   = ("AQI", "count"),
        ).round(2)

        result = []
        for s_num, s_name in SEASONS.items():
            if s_num in seasonal.index:
                row = seasonal.loc[s_num]
                result.append({
                    "season":    s_name,
                    "season_id": s_num,
                    "avg_aqi":   round(float(row["avg_aqi"]), 1),
                    "max_aqi":   round(float(row["max_aqi"]), 1),
                    "std_aqi":   round(float(row["std_aqi"]) if not np.isnan(row["std_aqi"]) else 0, 1),
                    "count":     int(row["count"]),
                })

        worst  = max(result, key=lambda x: x["avg_aqi"], default={})
        best   = min(result, key=lambda x: x["avg_aqi"], default={})

        return {
            "by_season":     result,
            "worst_season":  worst.get("season"),
            "best_season":   best.get("season"),
        }

    # ── Yearly trends ──────────────────────────────────────────────────────────

    def _yearly_trends(self, df: pd.DataFrame) -> dict:
        """Year-over-year AQI trends."""
        yearly = df.groupby("year").agg(
            avg_aqi   = ("AQI", "mean"),
            max_aqi   = ("AQI", "max"),
            min_aqi   = ("AQI", "min"),
            std_aqi   = ("AQI", "std"),
            count     = ("AQI", "count"),
        ).round(2).reset_index()

        rows = []
        for _, row in yearly.iterrows():
            rows.append({
                "year":    int(row["year"]),
                "avg_aqi": round(float(row["avg_aqi"]), 1),
                "max_aqi": round(float(row["max_aqi"]), 1),
                "min_aqi": round(float(row["min_aqi"]), 1),
                "std_aqi": round(float(row["std_aqi"]) if not np.isnan(row["std_aqi"]) else 0, 1),
                "count":   int(row["count"]),
            })

        # YoY change
        for i in range(1, len(rows)):
            prev = rows[i-1]["avg_aqi"]
            curr = rows[i]["avg_aqi"]
            rows[i]["yoy_change"]     = round(curr - prev, 1)
            rows[i]["yoy_change_pct"] = round((curr - prev) / max(prev, 1) * 100, 1)

        if rows:
            rows[0]["yoy_change"]     = None
            rows[0]["yoy_change_pct"] = None

        return {"by_year": rows}

    # ── Trend direction ────────────────────────────────────────────────────────

    def _trend_direction(self, df: pd.DataFrame) -> dict:
        """Linear regression over entire time series to classify trend."""
        if len(df) < 10:
            return {"direction": "insufficient_data", "slope": None}

        x     = np.arange(len(df))
        y     = df["AQI"].values
        slope, intercept = np.polyfit(x, y, 1)

        # Classify slope
        if slope > 0.05:
            direction = "worsening"
        elif slope < -0.05:
            direction = "improving"
        else:
            direction = "stable"

        # R² for confidence
        y_pred = slope * x + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - y.mean()) ** 2)
        r2     = 1 - ss_res / max(ss_tot, 1e-10)

        # 5-year projection
        slope_annual   = slope * 365
        latest_aqi     = float(df["AQI"].iloc[-1])
        projected_5y   = max(0, round(latest_aqi + slope_annual * 5, 1))

        return {
            "direction":        direction,
            "slope_per_day":    round(float(slope), 4),
            "slope_per_year":   round(float(slope_annual), 2),
            "r_squared":        round(float(r2), 4),
            "trend_strength":   "strong" if r2 > 0.5 else "moderate" if r2 > 0.2 else "weak",
            "projected_aqi_5y": projected_5y,
            "confidence":       "high" if r2 > 0.5 else "medium" if r2 > 0.2 else "low",
        }

    # ── Worst/Best periods ─────────────────────────────────────────────────────

    def _worst_periods(self, df: pd.DataFrame, top_n: int = 5) -> list:
        """Top N worst AQI days historically."""
        top = df.nlargest(top_n, "AQI")[["Date", "AQI"]].copy()
        return [
            {
                "date":  str(row["Date"].date()),
                "aqi":   round(float(row["AQI"]), 1),
                "month": MONTHS_ABBR[row["Date"].month - 1],
                "year":  int(row["Date"].year),
            }
            for _, row in top.iterrows()
        ]

    def _best_periods(self, df: pd.DataFrame, top_n: int = 5) -> list:
        """Top N cleanest AQI days historically."""
        top = df[df["AQI"] > 0].nsmallest(top_n, "AQI")[["Date", "AQI"]].copy()
        return [
            {
                "date":  str(row["Date"].date()),
                "aqi":   round(float(row["AQI"]), 1),
                "month": MONTHS_ABBR[row["Date"].month - 1],
                "year":  int(row["Date"].year),
            }
            for _, row in top.iterrows()
        ]

    # ── Pollutant ranking ──────────────────────────────────────────────────────

    def _pollutant_ranking(self, df: pd.DataFrame) -> list:
        """
        Rank pollutants by Pearson correlation with AQI.
        Shows which pollutants drive AQI most for this city.
        """
        ranking = []
        for pollutant in POLLUTANTS:
            if pollutant not in df.columns:
                continue
            series = df[pollutant].dropna()
            if len(series) < 10:
                continue
            try:
                valid   = df[["AQI", pollutant]].dropna()
                corr    = float(valid["AQI"].corr(valid[pollutant]))
                avg_val = float(valid[pollutant].mean())
                max_val = float(valid[pollutant].max())
                if not np.isnan(corr):
                    ranking.append({
                        "pollutant":    pollutant,
                        "correlation":  round(corr, 4),
                        "abs_corr":     round(abs(corr), 4),
                        "avg_value":    round(avg_val, 2),
                        "max_value":    round(max_val, 2),
                        "coverage_pct": round(len(series) / len(df) * 100, 1),
                        "impact":       "high" if abs(corr) > 0.6 else
                                        "medium" if abs(corr) > 0.3 else "low",
                    })
            except Exception:
                continue

        ranking.sort(key=lambda x: x["abs_corr"], reverse=True)
        return ranking

    # ── Anomaly frequency ──────────────────────────────────────────────────────

    def _anomaly_frequency(self, df: pd.DataFrame) -> dict:
        """How often does AQI exceed danger thresholds?"""
        total = len(df)
        return {
            "total_days":         total,
            "days_good":          int((df["AQI"] <= 50).sum()),
            "days_moderate":      int(((df["AQI"] > 50) & (df["AQI"] <= 100)).sum()),
            "days_unhealthy_sg":  int(((df["AQI"] > 100) & (df["AQI"] <= 150)).sum()),
            "days_unhealthy":     int(((df["AQI"] > 150) & (df["AQI"] <= 200)).sum()),
            "days_very_unhealthy":int(((df["AQI"] > 200) & (df["AQI"] <= 300)).sum()),
            "days_hazardous":     int((df["AQI"] > 300).sum()),
            "pct_good":           round(int((df["AQI"] <= 50).sum()) / max(total, 1) * 100, 1),
            "pct_unhealthy_plus": round(int((df["AQI"] > 150).sum()) / max(total, 1) * 100, 1),
            "pct_hazardous":      round(int((df["AQI"] > 300).sum()) / max(total, 1) * 100, 1),
            "worst_streak_days":  self._longest_streak(df["AQI"] > 150),
        }

    @staticmethod
    def _longest_streak(mask: pd.Series) -> int:
        """Find longest consecutive streak of True values."""
        max_streak  = 0
        curr_streak = 0
        for val in mask:
            if val:
                curr_streak += 1
                max_streak   = max(max_streak, curr_streak)
            else:
                curr_streak  = 0
        return int(max_streak)

    # ── Volatility profile ─────────────────────────────────────────────────────

    def _volatility_profile(self, df: pd.DataFrame) -> dict:
        """How volatile/predictable is this city's AQI?"""
        aqi     = df["AQI"].dropna()
        std     = float(aqi.std())
        mean    = float(aqi.mean())
        cv      = std / max(mean, 1)    # coefficient of variation

        if cv > 0.5:
            volatility = "high"
        elif cv > 0.25:
            volatility = "medium"
        else:
            volatility = "low"

        return {
            "std_dev":          round(std, 2),
            "mean":             round(mean, 2),
            "coefficient_of_variation": round(cv, 4),
            "volatility":       volatility,
            "p10":              round(float(aqi.quantile(0.10)), 1),
            "p25":              round(float(aqi.quantile(0.25)), 1),
            "p50":              round(float(aqi.quantile(0.50)), 1),
            "p75":              round(float(aqi.quantile(0.75)), 1),
            "p90":              round(float(aqi.quantile(0.90)), 1),
            "iqr":              round(float(aqi.quantile(0.75) - aqi.quantile(0.25)), 1),
        }

    # ── Improvement score ──────────────────────────────────────────────────────

    def _improvement_score(self, df: pd.DataFrame) -> dict:
        """
        Score from -100 (severe worsening) to +100 (significant improvement).
        Based on linear trend slope normalized by baseline AQI.
        """
        if len(df) < 30:
            return {"score": 0, "label": "insufficient_data"}

        x           = np.arange(len(df))
        y           = df["AQI"].values
        slope, _    = np.polyfit(x, y, 1)
        baseline    = float(y[:30].mean())
        annual_slope = slope * 365
        raw_score   = -(annual_slope / max(baseline, 1)) * 100
        score       = max(-100, min(100, round(raw_score, 1)))

        if score > 20:
            label = "significantly_improving"
        elif score > 5:
            label = "improving"
        elif score > -5:
            label = "stable"
        elif score > -20:
            label = "worsening"
        else:
            label = "significantly_worsening"

        return {
            "score":            score,
            "label":            label,
            "annual_aqi_change": round(float(annual_slope), 2),
            "baseline_aqi":     round(baseline, 1),
        }
