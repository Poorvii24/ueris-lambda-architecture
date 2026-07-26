"""
ai_layer/insight_generator.py
==============================
UERIS — Automated Insight Generator

Generates executive-level natural language insights by combining:
  - Current realtime readings
  - Historical batch statistics
  - Trend intelligence profile
  - Anomaly detection results
  - Forecast predictions
  - Cross-city comparisons

Output format (per city):
    {
        "headline":      str  — one bold headline
        "summary":       str  — 2-3 sentence executive summary
        "key_insights":  list — 3-5 bullet point insights
        "alerts":        list — active warnings/anomalies
        "actions":       list — recommended actions for policymakers
        "comparison":    dict — how this city ranks vs others
        "outlook":       str  — 24h / 7d outlook sentence
    }

Usage:
    gen = InsightGenerator()
    insight = gen.generate_city_insight(
        city="Delhi",
        realtime=rt_doc,
        batch_stats=batch_doc,
        trend_profile=trend_doc,
        forecast=forecast_doc,
        rank=1,
        total_cities=26,
    )
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional


# ── Templates ─────────────────────────────────────────────────────────────────
RISK_ADJECTIVES = {
    "Low":       "minimal",
    "Moderate":  "moderate",
    "High":      "significant",
    "Very High": "serious",
    "Severe":    "critical",
}

TREND_PHRASES = {
    "significantly_improving": "showing strong improvement over time",
    "improving":               "gradually improving year-over-year",
    "stable":                  "relatively stable over the historical period",
    "worsening":               "showing a gradual deterioration trend",
    "significantly_worsening": "experiencing significant long-term worsening",
    "insufficient_data":       "with limited historical data for trend analysis",
}

AQI_HEALTH_MESSAGES = {
    "good":                          "Safe for all activities.",
    "moderate":                      "Acceptable for most people.",
    "unhealthy for sensitive groups": "Sensitive groups should take precautions.",
    "unhealthy":                      "Limit outdoor exposure.",
    "very unhealthy":                 "Avoid outdoor activities.",
    "hazardous":                      "Emergency conditions — stay indoors.",
}

MONTH_NAMES = [
    "January","February","March","April","May","June",
    "July","August","September","October","November","December"
]


class InsightGenerator:
    """
    Automated insight generation for UERIS city environmental profiles.
    """

    def generate_city_insight(
        self,
        city: str,
        realtime: dict = None,
        batch_stats: dict = None,
        trend_profile: dict = None,
        forecast: dict = None,
        rank: int = None,
        total_cities: int = 26,
        all_cities_avg_usi: float = None,
    ) -> dict:
        """
        Generate a complete insight package for one city.

        Args:
            city:               City name
            realtime:           Latest realtime_views document
            batch_stats:        batch_views.stats dict
            trend_profile:      TrendIntelligence.analyse_city() output
            forecast:           ForecastingEngine.predict() output (24h)
            rank:               City's health rank (1 = most polluted)
            total_cities:       Total number of monitored cities
            all_cities_avg_usi: System-wide average USI for comparison

        Returns:
            Complete insight dict
        """
        rt      = realtime or {}
        stats   = batch_stats or {}
        trend   = trend_profile or {}
        fc      = forecast or {}

        avg_usi      = stats.get("avg_usi", 40) or 40
        avg_aqi      = stats.get("avg_aqi", 100) or 100
        current_usi  = rt.get("usi", avg_usi) or avg_usi
        current_aqi  = rt.get("aqi", avg_aqi) or avg_aqi
        risk_level   = rt.get("risk_level", "Moderate") or "Moderate"
        is_anomaly   = rt.get("is_anomaly", False)
        health_score = max(0, round(100 - avg_usi, 1))

        # Build components
        headline    = self._build_headline(city, current_usi, risk_level, is_anomaly)
        summary     = self._build_summary(
            city, current_aqi, current_usi, risk_level, avg_aqi,
            trend, rank, total_cities, stats
        )
        key_insights = self._build_key_insights(
            city, stats, trend, rt, rank, total_cities, all_cities_avg_usi
        )
        alerts       = self._build_alerts(city, rt, stats, trend)
        actions      = self._build_actions(city, risk_level, trend, stats)
        outlook      = self._build_outlook(city, fc, rt, trend)
        comparison   = self._build_comparison(
            city, avg_usi, avg_aqi, health_score, rank, total_cities, all_cities_avg_usi
        )

        return {
            "city":            city,
            "headline":        headline,
            "summary":         summary,
            "key_insights":    key_insights,
            "alerts":          alerts,
            "actions":         actions,
            "outlook":         outlook,
            "comparison":      comparison,
            "health_score":    health_score,
            "risk_level":      risk_level,
            "is_anomaly":      is_anomaly,
            "generated_at":    datetime.now(timezone.utc).isoformat(),
        }

    def generate_system_insight(
        self,
        cities_data: List[dict],
        all_cities_avg_usi: float,
    ) -> dict:
        """
        Generate a system-wide executive summary across all cities.

        Args:
            cities_data:        List of enriched city dicts (from /api/cities)
            all_cities_avg_usi: Mean USI across all cities

        Returns:
            System-level insight dict
        """
        if not cities_data:
            return {"summary": "No city data available.", "cities": []}

        # Sort by current USI
        ranked = sorted(
            cities_data,
            key=lambda x: x.get("current_usi") or x.get("avg_usi") or 0,
            reverse=True,
        )
        most_polluted   = ranked[0]  if ranked else {}
        cleanest        = ranked[-1] if ranked else {}
        anomaly_cities  = [c for c in cities_data if c.get("is_anomaly")]
        high_risk       = [c for c in cities_data
                           if c.get("current_risk") in ("Very High", "Severe", "High")]

        now  = datetime.now(timezone.utc)
        date = now.strftime("%B %d, %Y %H:%M UTC")

        summary = (
            f"As of {date}, UERIS is monitoring {len(cities_data)} Indian cities. "
            f"The system-wide average Urban Stress Index is {all_cities_avg_usi:.1f}. "
            f"{'⚠ ' + str(len(anomaly_cities)) + ' cities are experiencing active pollution anomalies. ' if anomaly_cities else ''}"
            f"Most polluted: {most_polluted.get('city', '—')} "
            f"(USI {most_polluted.get('current_usi', '—')}). "
            f"Cleanest: {cleanest.get('city', '—')} "
            f"(USI {cleanest.get('current_usi', '—')})."
        )

        key_insights = []

        if anomaly_cities:
            key_insights.append({
                "type":  "alert",
                "icon":  "⚠",
                "text":  f"{len(anomaly_cities)} cities with active anomalies: "
                         f"{', '.join(c['city'] for c in anomaly_cities[:3])}"
                         f"{'...' if len(anomaly_cities) > 3 else ''}.",
            })

        if high_risk:
            key_insights.append({
                "type":  "warning",
                "icon":  "🔴",
                "text":  f"{len(high_risk)} cities at High or above risk level. "
                         f"Residents in {', '.join(c['city'] for c in high_risk[:3])} "
                         f"should limit outdoor exposure.",
            })

        key_insights.append({
            "type":  "info",
            "icon":  "📊",
            "text":  f"System average USI is {all_cities_avg_usi:.1f}. "
                     f"{'Above' if all_cities_avg_usi > 45 else 'Within'} normal range.",
        })

        if most_polluted.get("city"):
            key_insights.append({
                "type":  "ranking",
                "icon":  "🏭",
                "text":  f"{most_polluted['city']} has the highest stress index "
                         f"(USI {most_polluted.get('current_usi', '—')}, "
                         f"AQI {most_polluted.get('current_aqi', '—')}).",
            })

        if cleanest.get("city"):
            key_insights.append({
                "type":  "positive",
                "icon":  "🌿",
                "text":  f"{cleanest['city']} has the best air quality today "
                         f"(USI {cleanest.get('current_usi', '—')}).",
            })

        return {
            "summary":           summary,
            "total_cities":      len(cities_data),
            "avg_usi":           round(all_cities_avg_usi, 1),
            "anomaly_count":     len(anomaly_cities),
            "high_risk_count":   len(high_risk),
            "most_polluted":     most_polluted.get("city"),
            "cleanest":          cleanest.get("city"),
            "key_insights":      key_insights,
            "ranked_cities":     [
                {
                    "city":  c.get("city"),
                    "usi":   c.get("current_usi") or c.get("avg_usi"),
                    "risk":  c.get("current_risk"),
                    "anom":  c.get("is_anomaly", False),
                }
                for c in ranked
            ],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # ── Private builders ───────────────────────────────────────────────────────

    def _build_headline(
        self, city: str, usi: float, risk: str, is_anomaly: bool
    ) -> str:
        if is_anomaly:
            return f"⚠ POLLUTION SPIKE: {city} experiencing anomalous conditions (USI {usi:.0f})"
        emoji_map = {
            "Low": "🌿", "Moderate": "🌤", "High": "🔶",
            "Very High": "🔴", "Severe": "🆘"
        }
        emoji = emoji_map.get(risk, "📊")
        return f"{emoji} {city}: {risk} environmental stress (USI {usi:.0f})"

    def _build_summary(
        self,
        city: str,
        current_aqi: float,
        current_usi: float,
        risk_level: str,
        avg_aqi: float,
        trend: dict,
        rank: int,
        total_cities: int,
        stats: dict,
    ) -> str:
        risk_adj   = RISK_ADJECTIVES.get(risk_level, "moderate")
        trend_dir  = trend.get("trend_direction", {}).get("direction", "stable")
        trend_phr  = TREND_PHRASES.get(trend_dir, "with stable conditions")
        rank_str   = f"ranked #{rank} of {total_cities} cities" if rank else ""
        health_sc  = max(0, round(100 - avg_aqi / 5, 0))

        vs_baseline = ""
        if current_aqi and avg_aqi:
            delta = current_aqi - avg_aqi
            if abs(delta) > 5:
                direction = "above" if delta > 0 else "below"
                vs_baseline = (
                    f"Current AQI of {current_aqi:.0f} is "
                    f"{abs(delta):.0f} units {direction} the historical average of {avg_aqi:.0f}. "
                )

        return (
            f"{city} is currently experiencing {risk_adj} environmental stress "
            f"with a USI of {current_usi:.1f}{', ' + rank_str if rank_str else ''}. "
            f"{vs_baseline}"
            f"Historically, {city} is {trend_phr}."
        )

    def _build_key_insights(
        self,
        city: str,
        stats: dict,
        trend: dict,
        rt: dict,
        rank: int,
        total_cities: int,
        sys_avg_usi: float,
    ) -> List[dict]:
        insights = []
        avg_usi  = stats.get("avg_usi", 40) or 40
        avg_aqi  = stats.get("avg_aqi", 100) or 100

        # Trend insight
        td    = trend.get("trend_direction", {})
        t_dir = td.get("direction", "stable")
        if t_dir in ("worsening", "significantly_worsening"):
            insights.append({
                "type": "warning",
                "icon": "📈",
                "text": (
                    f"Long-term trend: AQI increasing by "
                    f"{abs(td.get('slope_per_year', 0)):.1f} units/year. "
                    f"5-year projection: {td.get('projected_aqi_5y', '—')} AQI."
                ),
            })
        elif t_dir in ("improving", "significantly_improving"):
            insights.append({
                "type": "positive",
                "icon": "📉",
                "text": (
                    f"Good news: Air quality improving by "
                    f"{abs(td.get('slope_per_year', 0)):.1f} AQI units/year."
                ),
            })

        # Seasonal insight
        mp = trend.get("monthly_patterns", {})
        if mp.get("worst_month") and mp.get("best_month"):
            insights.append({
                "type": "info",
                "icon": "📅",
                "text": (
                    f"Seasonal pattern: {mp['worst_month']} is typically worst "
                    f"(avg AQI {mp.get('worst_avg_aqi', '—')}), "
                    f"{mp['best_month']} is cleanest "
                    f"(avg AQI {mp.get('best_avg_aqi', '—')})."
                ),
            })

        # Volatility insight
        vp = trend.get("volatility_profile", {})
        if vp.get("volatility") == "high":
            insights.append({
                "type": "warning",
                "icon": "📊",
                "text": (
                    f"High volatility: AQI fluctuates significantly "
                    f"(std dev {vp.get('std_dev', '—')}). "
                    f"Forecasts may be less reliable on high-wind days."
                ),
            })

        # Anomaly frequency insight
        af = trend.get("anomaly_frequency", {})
        pct_unhealthy = af.get("pct_unhealthy_plus", 0)
        if pct_unhealthy > 20:
            insights.append({
                "type": "warning",
                "icon": "🔴",
                "text": (
                    f"{pct_unhealthy:.0f}% of historical days had unhealthy AQI (>150). "
                    f"Longest consecutive unhealthy streak: "
                    f"{af.get('worst_streak_days', '—')} days."
                ),
            })
        elif pct_unhealthy < 5:
            insights.append({
                "type": "positive",
                "icon": "✅",
                "text": (
                    f"Only {pct_unhealthy:.0f}% of historical days exceeded unhealthy AQI. "
                    f"This city generally maintains good air quality."
                ),
            })

        # Rank vs system average
        if sys_avg_usi and rank:
            delta = avg_usi - sys_avg_usi
            if abs(delta) > 5:
                direction = "higher" if delta > 0 else "lower"
                insights.append({
                    "type": "info",
                    "icon": "🏙",
                    "text": (
                        f"City ranks #{rank} of {total_cities}. "
                        f"Historical USI is {abs(delta):.1f} points {direction} "
                        f"than the national monitoring average of {sys_avg_usi:.1f}."
                    ),
                })

        # Pollutant driver
        pr = trend.get("pollutant_ranking", [])
        if pr:
            top_poll = pr[0]
            insights.append({
                "type": "info",
                "icon": "🏭",
                "text": (
                    f"Primary AQI driver: {top_poll['pollutant']} "
                    f"(correlation {top_poll['correlation']:.2f}, "
                    f"avg {top_poll['avg_value']:.1f})."
                ),
            })

        return insights[:5]

    def _build_alerts(
        self,
        city: str,
        rt: dict,
        stats: dict,
        trend: dict,
    ) -> List[dict]:
        alerts = []

        # Active anomaly
        if rt.get("is_anomaly"):
            alerts.append({
                "level":   "critical",
                "icon":    "🆘",
                "title":   "Active Pollution Anomaly",
                "message": (
                    f"ML ensemble detected anomalous conditions in {city}. "
                    f"AQI {rt.get('aqi', '—')} is significantly above baseline. "
                    f"Detection: {rt.get('anomaly_method', 'ensemble')}."
                ),
                "time": rt.get("updated_at", datetime.now(timezone.utc).isoformat()),
            })

        # High current AQI
        aqi = rt.get("aqi", 0) or 0
        if aqi > 300:
            alerts.append({
                "level":   "hazardous",
                "icon":    "☠",
                "title":   "Hazardous Air Quality",
                "message": f"AQI {aqi:.0f} is in the hazardous range. Health emergency.",
                "time":    rt.get("updated_at", ""),
            })
        elif aqi > 200:
            alerts.append({
                "level":   "very_unhealthy",
                "icon":    "🔴",
                "title":   "Very Unhealthy Air",
                "message": f"AQI {aqi:.0f} — everyone should avoid outdoor activities.",
                "time":    rt.get("updated_at", ""),
            })

        # Worsening trend
        td = trend.get("trend_direction", {})
        if td.get("direction") == "significantly_worsening":
            alerts.append({
                "level":   "warning",
                "icon":    "📈",
                "title":   "Long-term Deterioration",
                "message": (
                    f"Air quality in {city} is deteriorating by "
                    f"{abs(td.get('slope_per_year', 0)):.1f} AQI units per year. "
                    f"5-year projection: AQI {td.get('projected_aqi_5y', '—')}."
                ),
                "time":    datetime.now(timezone.utc).isoformat(),
            })

        return alerts

    def _build_actions(
        self,
        city: str,
        risk_level: str,
        trend: dict,
        stats: dict,
    ) -> List[dict]:
        actions = []

        risk_actions = {
            "Severe": [
                {"priority": "immediate", "action": "Issue public health emergency advisory"},
                {"priority": "immediate", "action": "Activate all air quality monitoring stations"},
                {"priority": "urgent",    "action": "Suspend outdoor construction and heavy industry"},
                {"priority": "urgent",    "action": "Provide free N95 masks at public distribution points"},
            ],
            "Very High": [
                {"priority": "urgent",    "action": "Issue health advisory for sensitive groups"},
                {"priority": "urgent",    "action": "Restrict heavy vehicle traffic in city centre"},
                {"priority": "medium",    "action": "Enable odd-even vehicle scheme if applicable"},
            ],
            "High": [
                {"priority": "medium",    "action": "Increase public transport frequency to reduce vehicle use"},
                {"priority": "medium",    "action": "Alert schools to cancel outdoor activities"},
                {"priority": "low",       "action": "Monitor industrial emission compliance"},
            ],
            "Moderate": [
                {"priority": "low",       "action": "Remind sensitive populations to check AQI before outdoor activity"},
                {"priority": "low",       "action": "Ensure air quality monitoring stations are operational"},
            ],
            "Low": [
                {"priority": "routine",   "action": "Continue regular environmental monitoring"},
                {"priority": "routine",   "action": "Use clean air day for outdoor community events"},
            ],
        }

        city_actions = risk_actions.get(risk_level, risk_actions["Moderate"])
        actions.extend(city_actions[:3])

        # Trend-based action
        td = trend.get("trend_direction", {})
        if td.get("direction") in ("worsening", "significantly_worsening"):
            actions.append({
                "priority": "strategic",
                "action":   (
                    f"Initiate long-term air quality improvement programme in {city}. "
                    f"Current trajectory: +{abs(td.get('slope_per_year', 0)):.1f} AQI/year."
                ),
            })

        return actions

    def _build_outlook(
        self,
        city: str,
        forecast: dict,
        rt: dict,
        trend: dict,
    ) -> str:
        if not forecast:
            current_aqi = rt.get("aqi", 0)
            if current_aqi > 200:
                return f"Air quality in {city} is currently poor. Monitor conditions closely."
            return f"Air quality in {city} is within normal range. No immediate concerns."

        pred_aqi  = forecast.get("predicted_aqi", 0) or 0
        horizon   = forecast.get("horizon", "24h")
        conf      = forecast.get("confidence_pct", 0) or 0
        model     = forecast.get("model_used", "ML model")

        horizon_text = {
            "6h": "next 6 hours", "12h": "next 12 hours",
            "24h": "next 24 hours", "7d": "next 7 days"
        }.get(horizon, horizon)

        current   = rt.get("aqi") or forecast.get("current_aqi", pred_aqi)
        delta     = pred_aqi - (current or pred_aqi)
        direction = "worsen to" if delta > 5 else "improve to" if delta < -5 else "remain near"

        if pred_aqi > 200:
            concern = "Air quality is expected to remain poor. Precautions advised."
        elif pred_aqi > 100:
            concern = "Moderate concerns — sensitive groups should plan accordingly."
        else:
            concern = "Generally safe conditions expected."

        return (
            f"Forecast ({horizon_text}): AQI expected to {direction} {pred_aqi:.0f} "
            f"({forecast.get('lower_bound', '—')}–{forecast.get('upper_bound', '—')} "
            f"confidence interval, {conf}% confidence via {model}). {concern}"
        )

    def _build_comparison(
        self,
        city: str,
        avg_usi: float,
        avg_aqi: float,
        health_score: float,
        rank: int,
        total_cities: int,
        sys_avg_usi: float,
    ) -> dict:
        if rank and total_cities:
            percentile = round((total_cities - rank) / total_cities * 100)
        else:
            percentile = None

        vs_system = None
        if sys_avg_usi:
            delta     = avg_usi - sys_avg_usi
            vs_system = {
                "delta":     round(delta, 1),
                "direction": "above" if delta > 0 else "below",
                "sys_avg":   round(sys_avg_usi, 1),
            }

        return {
            "rank":          rank,
            "total_cities":  total_cities,
            "percentile":    percentile,
            "health_score":  health_score,
            "vs_system":     vs_system,
            "label":         (
                "among most polluted" if rank and rank <= total_cities // 4 else
                "above average pollution" if rank and rank <= total_cities // 2 else
                "below average pollution" if rank else "unknown"
            ),
        }
