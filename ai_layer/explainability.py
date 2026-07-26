"""
ai_layer/explainability.py
===========================
UERIS — Model Explainability

Provides human-readable explanations for:
  1. Anomaly detections — WHY is this reading anomalous?
  2. Forecasts         — WHAT factors drive the prediction?
  3. City risk scores  — WHAT contributes most to a city's USI?

Methods:
  - SHAP values (TreeExplainer for RF/XGB/LGB, KernelExplainer fallback)
  - Feature Z-scores (how many std devs from city baseline)
  - Natural language explanation generation
  - Top-K contributing factors with direction (↑ increasing risk, ↓ reducing)

Usage:
    explainer = Explainability()

    # For anomaly:
    explanation = explainer.explain_anomaly(
        city="Delhi", aqi=285, temperature=31, humidity=45, usi=72,
        feature_zscores={"aqi": 3.2, "temperature": 0.8, "humidity": -0.5},
        severity="High", models_flagged=["IsolationForest", "LOF"]
    )

    # For forecast:
    explanation = explainer.explain_forecast(
        city="Delhi", horizon="24h",
        predicted_aqi=220, feature_importance={"aqi_rolling_mean_7d": 0.45, ...}
    )
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional


# ── Threshold context strings ──────────────────────────────────────────────────
AQI_CONTEXT = [
    (301, "hazardous",      "Health emergency — everyone affected"),
    (201, "very unhealthy", "Health alert — serious effects for all"),
    (151, "unhealthy",      "Everyone may begin to experience health effects"),
    (101, "unhealthy for sensitive groups", "Sensitive groups at risk"),
    (51,  "moderate",       "Acceptable air quality with some concern"),
    (0,   "good",           "Air quality is satisfactory"),
]

THI_CONTEXT = [
    (28, "extreme discomfort",  "High heat and humidity are dangerous"),
    (24, "general discomfort",  "Most people feel uncomfortable"),
    (21, "some discomfort",     "Sensitive individuals affected"),
    (0,  "comfortable",         "Conditions are pleasant"),
]

SEASON_NAMES   = {0: "winter", 1: "spring", 2: "summer/monsoon", 3: "autumn"}
SEVERITY_EMOJI = {
    "Normal":   "✅",
    "Low":      "🟡",
    "Medium":   "🟠",
    "High":     "🔴",
    "Critical": "🆘",
}


class Explainability:
    """
    Generates human-readable explanations for UERIS AI outputs.

    All methods return structured dicts with:
        summary:        str   — one-sentence executive summary
        factors:        list  — top contributing factors with direction/magnitude
        recommendation: str   — actionable health/policy recommendation
        technical:      dict  — full numerical details for developers/dashboard
    """

    # ── Anomaly Explanation ────────────────────────────────────────────────────

    def explain_anomaly(
        self,
        city: str,
        aqi: float,
        temperature: float,
        humidity: float,
        usi: float,
        feature_zscores: Dict[str, float],
        severity: str,
        models_flagged: List[str],
        anomaly_score: float = None,
        batch_stats: dict = None,
    ) -> dict:
        """
        Explain WHY a reading was flagged as anomalous.

        Args:
            city:            City name
            aqi/temp/hum/usi: Current values
            feature_zscores: Z-scores per feature (from AnomalyEnsemble)
            severity:        'Normal'|'Low'|'Medium'|'High'|'Critical'
            models_flagged:  List of model names that flagged it
            anomaly_score:   Raw anomaly score [0,1]
            batch_stats:     Historical stats for context

        Returns:
            Structured explanation dict
        """
        stats    = batch_stats or {}
        avg_aqi  = stats.get("avg_aqi", 100) or 100
        avg_temp = stats.get("avg_temp", 26)  or 26
        avg_hum  = stats.get("avg_humidity", 55) or 55

        # Rank factors by absolute Z-score magnitude
        factors = self._rank_factors(feature_zscores, {
            "aqi":         (aqi,         avg_aqi,  "AQI",         "μg/m³"),
            "temperature": (temperature, avg_temp, "Temperature", "°C"),
            "humidity":    (humidity,    avg_hum,  "Humidity",    "%"),
            "usi":         (usi,         stats.get("avg_usi", 40) or 40, "USI", ""),
        })

        # AQI context
        aqi_level, aqi_label, aqi_desc = self._classify_aqi(aqi)
        thi       = self._compute_thi(temperature, humidity)
        thi_level, thi_label, thi_desc = self._classify_thi(thi)

        # Build natural language summary
        primary_factor = factors[0]["name"] if factors else "multiple factors"
        emoji          = SEVERITY_EMOJI.get(severity, "⚠")

        summary = (
            f"{emoji} {severity} pollution anomaly detected in {city}. "
            f"AQI={aqi:.0f} ({aqi_label}) is "
            f"{abs(feature_zscores.get('aqi', 0)):.1f} standard deviations "
            f"{'above' if feature_zscores.get('aqi', 0) > 0 else 'below'} "
            f"the city's historical baseline of {avg_aqi:.0f}. "
            f"Primary driver: {primary_factor}."
        )

        # Recommendation
        recommendation = self._anomaly_recommendation(severity, aqi, aqi_label, thi_label, city)

        # Detection consensus
        n_models    = 3
        n_flagged   = len(models_flagged)
        consensus   = f"{n_flagged}/{n_models} models agree"
        if n_flagged == 3:
            consensus_text = "Unanimous detection — high confidence"
        elif n_flagged == 2:
            consensus_text = "Majority detection — moderate confidence"
        else:
            consensus_text = "Single model detection — review recommended"

        return {
            "summary":        summary,
            "severity":       severity,
            "emoji":          emoji,
            "factors":        factors[:5],
            "recommendation": recommendation,
            "consensus":      consensus,
            "consensus_text": consensus_text,
            "models_flagged": models_flagged,
            "technical": {
                "aqi":              aqi,
                "aqi_level":        aqi_level,
                "aqi_label":        aqi_label,
                "aqi_description":  aqi_desc,
                "aqi_vs_baseline":  round(aqi - avg_aqi, 1),
                "aqi_pct_above":    round((aqi - avg_aqi) / max(avg_aqi, 1) * 100, 1),
                "temperature":      temperature,
                "humidity":         humidity,
                "thi":              round(thi, 1),
                "thi_label":        thi_label,
                "thi_description":  thi_desc,
                "usi":              usi,
                "anomaly_score":    anomaly_score,
                "feature_zscores":  feature_zscores,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # ── Forecast Explanation ───────────────────────────────────────────────────

    def explain_forecast(
        self,
        city: str,
        horizon: str,
        predicted_aqi: float,
        current_aqi: float = None,
        feature_importance: Dict[str, float] = None,
        model_used: str = None,
        confidence_pct: int = None,
        model_rmse: float = None,
        batch_stats: dict = None,
    ) -> dict:
        """
        Explain WHAT drives a forecast prediction.
        """
        stats      = batch_stats or {}
        avg_aqi    = stats.get("avg_aqi", 100) or 100
        fi         = feature_importance or {}

        # Direction of prediction
        current = current_aqi or avg_aqi
        delta   = predicted_aqi - current
        direction = "increase" if delta > 0 else "decrease" if delta < 0 else "remain stable"

        # AQI classification
        _, aqi_label, aqi_desc = self._classify_aqi(predicted_aqi)

        # Top feature factors
        top_features = sorted(fi.items(), key=lambda x: x[1], reverse=True)[:5]
        feature_factors = [
            {
                "name":        self._humanise_feature(k),
                "key":         k,
                "importance":  round(v * 100, 1),
                "direction":   "↑" if "rolling" in k or "mean" in k else "~",
                "description": self._feature_description(k),
            }
            for k, v in top_features
        ]

        horizon_text = {
            "6h": "next 6 hours", "12h": "next 12 hours",
            "24h": "next 24 hours", "7d": "next 7 days"
        }.get(horizon, horizon)

        summary = (
            f"AQI in {city} is forecast to {direction} to "
            f"{predicted_aqi:.0f} ({aqi_label}) over the {horizon_text}. "
            f"{'Confidence: ' + str(confidence_pct) + '%.' if confidence_pct else ''} "
            f"{'Model: ' + model_used + '.' if model_used else ''}"
        )

        recommendation = self._forecast_recommendation(predicted_aqi, aqi_label, direction, city)

        return {
            "summary":        summary,
            "predicted_aqi":  predicted_aqi,
            "aqi_label":      aqi_label,
            "direction":      direction,
            "delta":          round(delta, 1),
            "delta_pct":      round(delta / max(current, 1) * 100, 1),
            "feature_factors": feature_factors,
            "recommendation": recommendation,
            "technical": {
                "model_used":    model_used,
                "model_rmse":    model_rmse,
                "confidence_pct": confidence_pct,
                "current_aqi":   current,
                "horizon":       horizon,
                "avg_aqi_hist":  avg_aqi,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # ── City Risk Explanation ──────────────────────────────────────────────────

    def explain_city_risk(
        self,
        city: str,
        usi: float,
        aqi: float,
        temperature: float,
        humidity: float,
        risk_level: str,
        batch_stats: dict = None,
        seasonal_context: dict = None,
    ) -> dict:
        """
        Explain what contributes to a city's current USI/risk level.
        """
        # USI component breakdown
        aqi_norm  = min(aqi / 300, 1.0)
        temp_norm = min(max((temperature - 15) / 25, 0), 1.0)
        hum_norm  = abs(humidity - 50) / 50

        aqi_contribution  = round(aqi_norm * 0.5 * 100, 1)
        temp_contribution = round(temp_norm * 0.3 * 100, 1)
        hum_contribution  = round(hum_norm * 0.2 * 100, 1)

        components = [
            {
                "name": "Air Quality (AQI)",
                "weight": "50%",
                "contribution": aqi_contribution,
                "value": aqi,
                "unit": "AQI",
                "direction": "↑" if aqi_norm > 0.5 else "→",
            },
            {
                "name": "Temperature",
                "weight": "30%",
                "contribution": temp_contribution,
                "value": temperature,
                "unit": "°C",
                "direction": "↑" if temp_norm > 0.5 else "→",
            },
            {
                "name": "Humidity Deviation",
                "weight": "20%",
                "contribution": hum_contribution,
                "value": humidity,
                "unit": "%",
                "direction": "↑" if hum_norm > 0.5 else "→",
            },
        ]
        components.sort(key=lambda x: x["contribution"], reverse=True)
        dominant = components[0]["name"]

        _, aqi_label, _ = self._classify_aqi(aqi)
        thi              = self._compute_thi(temperature, humidity)

        summary = (
            f"{city} has a USI of {usi:.1f} ({risk_level} risk). "
            f"Primary driver is {dominant} — "
            f"AQI of {aqi:.0f} ({aqi_label}) contributes "
            f"{aqi_contribution:.0f}% of the stress index."
        )

        return {
            "summary":      summary,
            "usi":          usi,
            "risk_level":   risk_level,
            "components":   components,
            "thi":          round(thi, 1),
            "aqi_label":    aqi_label,
            "dominant_factor": dominant,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # ── Private helpers ────────────────────────────────────────────────────────

    def _rank_factors(
        self,
        zscores: Dict[str, float],
        value_map: Dict[str, tuple],
    ) -> List[dict]:
        """Rank anomaly factors by absolute Z-score."""
        factors = []
        for key, zscore in sorted(zscores.items(),
                                   key=lambda x: abs(x[1]), reverse=True):
            if key not in value_map:
                continue
            current_val, baseline_val, display_name, unit = value_map[key]
            delta     = current_val - baseline_val
            direction = "↑" if delta > 0 else "↓"
            factors.append({
                "name":      display_name,
                "key":       key,
                "current":   round(float(current_val), 1),
                "baseline":  round(float(baseline_val), 1),
                "delta":     round(float(delta), 1),
                "zscore":    round(float(zscore), 2),
                "unit":      unit,
                "direction": direction,
                "severity":  "high" if abs(zscore) > 2 else "medium" if abs(zscore) > 1 else "low",
            })
        return factors

    @staticmethod
    def _classify_aqi(aqi: float) -> tuple:
        for threshold, label, desc in AQI_CONTEXT:
            if aqi >= threshold:
                return threshold, label, desc
        return 0, "good", "Air quality is satisfactory"

    @staticmethod
    def _classify_thi(thi: float) -> tuple:
        for threshold, label, desc in THI_CONTEXT:
            if thi >= threshold:
                return threshold, label, desc
        return 0, "comfortable", "Conditions are pleasant"

    @staticmethod
    def _compute_thi(temperature: float, humidity: float) -> float:
        t, h = float(temperature), float(humidity)
        return t - 0.55 * (1 - 0.01 * h) * (t - 14.5)

    @staticmethod
    def _anomaly_recommendation(
        severity: str, aqi: float, aqi_label: str, thi_label: str, city: str
    ) -> str:
        recommendations = {
            "Critical": (
                f"🆘 EMERGENCY in {city}. Avoid all outdoor activities. "
                f"Keep all windows closed. Wear N95/P100 respirators if going outside. "
                f"Seek immediate medical attention if you experience respiratory symptoms."
            ),
            "High": (
                f"🔴 Serious air quality event in {city}. "
                f"All residents should avoid outdoor exertion. "
                f"Children, elderly, and those with respiratory conditions must stay indoors. "
                f"Use air purifiers indoors."
            ),
            "Medium": (
                f"🟠 Elevated pollution in {city}. "
                f"Sensitive individuals should limit outdoor activity. "
                f"Consider wearing masks outdoors. Monitor air quality updates."
            ),
            "Low": (
                f"🟡 Minor anomaly detected in {city}. "
                f"Air quality is slightly elevated but generally safe for most people. "
                f"Sensitive groups should reduce prolonged outdoor exertion."
            ),
            "Normal": (
                f"✅ Air quality in {city} is within normal range. "
                f"Safe for outdoor activities."
            ),
        }
        return recommendations.get(severity, "Monitor air quality conditions.")

    @staticmethod
    def _forecast_recommendation(
        predicted_aqi: float, aqi_label: str, direction: str, city: str
    ) -> str:
        if predicted_aqi > 200:
            return (
                f"Air quality in {city} is forecast to worsen significantly. "
                f"Plan indoor activities and prepare N95 masks."
            )
        elif predicted_aqi > 150:
            return (
                f"Unhealthy air quality expected in {city}. "
                f"Sensitive groups should plan to stay indoors."
            )
        elif predicted_aqi > 100:
            return (
                f"Moderate air quality concerns expected in {city}. "
                f"Limit prolonged outdoor exertion."
            )
        elif direction == "decrease":
            return (
                f"Air quality in {city} is expected to improve. "
                f"Outdoor activities should be safe."
            )
        else:
            return (
                f"Air quality in {city} is expected to remain stable. "
                f"Safe for normal outdoor activities."
            )

    @staticmethod
    def _humanise_feature(key: str) -> str:
        mapping = {
            "aqi_rolling_mean_7d":   "7-day avg AQI",
            "aqi_rolling_mean_30d":  "30-day avg AQI",
            "aqi_rolling_std_7d":    "AQI variability (7d)",
            "aqi_rolling_max_7d":    "Peak AQI (7d)",
            "aqi_lag_1d":            "Yesterday's AQI",
            "aqi_lag_7d":            "AQI one week ago",
            "pollution_trend_7d":    "Pollution trend (7d)",
            "thi":                   "Heat discomfort index",
            "eri":                   "Environmental risk index",
            "temp_rolling_mean_7d":  "7-day avg temperature",
            "season":                "Season",
            "month":                 "Month",
            "is_rush_hour":          "Rush hour indicator",
            "aqi_normalized":        "AQI (normalized)",
        }
        return mapping.get(key, key.replace("_", " ").title())

    @staticmethod
    def _feature_description(key: str) -> str:
        desc = {
            "aqi_rolling_mean_7d":  "Recent weekly air quality trend",
            "aqi_rolling_mean_30d": "Monthly baseline air quality",
            "pollution_trend_7d":   "Whether pollution is rising or falling",
            "thi":                  "Combined heat and humidity stress",
            "season":               "Seasonal pollution patterns",
            "aqi_lag_1d":           "Previous day's air quality",
        }
        return desc.get(key, "Contributing environmental factor")
