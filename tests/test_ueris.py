"""
tests/test_ueris.py
====================
pytest unit and integration tests for UERIS.

Tests cover:
  - USI formula correctness
  - Risk classification
  - Lambda merge freshness logic
  - Isolation Forest anomaly detection
  - Flask API endpoints (integration tests — requires app running)

Run (unit tests only):
  pytest tests/test_ueris.py -v -k "not integration"

Run (all including API):
  pytest tests/test_ueris.py -v
  (requires: python serving_layer/app.py running on port 5000)
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import numpy as np
from datetime import datetime, timezone, timedelta


# ── USI Formula ───────────────────────────────────────────────────────────────

def compute_usi(aqi, temperature, humidity):
    aqi_norm  = min(aqi / 300.0, 1.0)
    temp_norm = min(max((temperature - 15.0) / 25.0, 0.0), 1.0)
    hum_norm  = abs(humidity - 50.0) / 50.0
    return round((0.5 * aqi_norm + 0.3 * temp_norm + 0.2 * hum_norm) * 100.0, 2)


def classify_risk(usi):
    if usi < 20:  return "Low"
    if usi < 40:  return "Moderate"
    if usi < 60:  return "High"
    if usi < 80:  return "Very High"
    return "Severe"


class TestUSIFormula:
    def test_perfect_conditions(self):
        """AQI=0, T=15, H=50 -> minimum USI"""
        usi = compute_usi(0, 15, 50)
        assert usi == 0.0

    def test_worst_case(self):
        """AQI=300, T=40, H=0 or 100 -> maximum USI"""
        usi = compute_usi(300, 40, 0)
        assert usi == 100.0

    def test_aqi_cap(self):
        """AQI > 300 should be capped at 300"""
        usi_300 = compute_usi(300, 25, 50)
        usi_999 = compute_usi(999, 25, 50)
        assert usi_300 == usi_999

    def test_humidity_symmetry(self):
        """H=30 and H=70 should give same humidity component (both 20 away from 50)"""
        usi_low  = compute_usi(100, 25, 30)
        usi_high = compute_usi(100, 25, 70)
        assert usi_low == usi_high

    def test_weights_sum(self):
        """Components: AQI=50%, Temp=30%, Hum=20%"""
        # Pure AQI contribution: aqi_norm=1, temp_norm=0, hum_norm=0
        usi = compute_usi(300, 15, 50)   # max AQI, perfect temp, perfect hum
        assert abs(usi - 50.0) < 0.5

    def test_delhi_typical(self):
        """Delhi typical: AQI=180, T=28, H=55"""
        usi = compute_usi(180, 28, 55)
        assert 30 < usi < 60, f"Expected USI between 30-60 for Delhi typical, got {usi}"

    def test_shillong_typical(self):
        """Shillong (cooler, cleaner): AQI=60, T=17, H=75"""
        usi = compute_usi(60, 17, 75)
        assert usi < 35, f"Expected USI < 35 for Shillong typical, got {usi}"

    def test_result_in_range(self):
        """USI must always be 0-100"""
        for aqi in [0, 50, 150, 300, 500]:
            for temp in [0, 15, 25, 40, 50]:
                for hum in [0, 25, 50, 75, 100]:
                    usi = compute_usi(aqi, temp, hum)
                    assert 0 <= usi <= 100, f"USI out of range: {usi} for AQI={aqi},T={temp},H={hum}"

    def test_temperature_below_baseline(self):
        """Temperature below 15 should contribute 0 to temp_norm"""
        usi_5  = compute_usi(100, 5,  50)
        usi_15 = compute_usi(100, 15, 50)
        assert usi_5 == usi_15  # both have temp_norm = 0


class TestRiskClassification:
    def test_low(self):
        assert classify_risk(0)  == "Low"
        assert classify_risk(19) == "Low"

    def test_moderate(self):
        assert classify_risk(20) == "Moderate"
        assert classify_risk(39) == "Moderate"

    def test_high(self):
        assert classify_risk(40) == "High"
        assert classify_risk(59) == "High"

    def test_very_high(self):
        assert classify_risk(60) == "Very High"
        assert classify_risk(79) == "Very High"

    def test_severe(self):
        assert classify_risk(80)  == "Severe"
        assert classify_risk(100) == "Severe"

    def test_boundary_values(self):
        """Exact boundary values"""
        assert classify_risk(20) == "Moderate"
        assert classify_risk(40) == "High"
        assert classify_risk(60) == "Very High"
        assert classify_risk(80) == "Severe"


# ── Lambda Merge Freshness ────────────────────────────────────────────────────

def realtime_is_fresh(rt, window_min=30):
    if not rt or not rt.get("updated_at"):
        return False
    try:
        updated = datetime.fromisoformat(rt["updated_at"])
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age_min = (datetime.now(timezone.utc) - updated).total_seconds() / 60
        return age_min <= window_min
    except Exception:
        return False


class TestLambdaMerge:
    def test_fresh_reading(self):
        rt = {"updated_at": datetime.now(timezone.utc).isoformat()}
        assert realtime_is_fresh(rt) is True

    def test_stale_reading(self):
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        rt  = {"updated_at": old.isoformat()}
        assert realtime_is_fresh(rt) is False

    def test_borderline_fresh(self):
        """29 minutes old should be fresh"""
        t   = datetime.now(timezone.utc) - timedelta(minutes=29)
        rt  = {"updated_at": t.isoformat()}
        assert realtime_is_fresh(rt) is True

    def test_borderline_stale(self):
        """31 minutes old should be stale"""
        t   = datetime.now(timezone.utc) - timedelta(minutes=31)
        rt  = {"updated_at": t.isoformat()}
        assert realtime_is_fresh(rt) is False

    def test_no_updated_at(self):
        assert realtime_is_fresh({}) is False

    def test_none_rt(self):
        assert realtime_is_fresh(None) is False

    def test_custom_window(self):
        t  = datetime.now(timezone.utc) - timedelta(minutes=10)
        rt = {"updated_at": t.isoformat()}
        assert realtime_is_fresh(rt, window_min=5)  is False
        assert realtime_is_fresh(rt, window_min=15) is True


# ── Isolation Forest ──────────────────────────────────────────────────────────

class TestIsolationForest:
    def test_normal_data_not_anomaly(self):
        from sklearn.ensemble import IsolationForest
        # Generate clean data around AQI=100, Temp=28, Hum=55, USI=35
        np.random.seed(42)
        X_train = np.random.normal(loc=[100, 28, 55, 35], scale=[15, 2, 5, 5], size=(200, 4))
        clf = IsolationForest(contamination=0.05, random_state=42)
        clf.fit(X_train)
        normal = np.array([[100, 28, 55, 35]])
        assert clf.predict(normal)[0] == 1, "Normal reading should not be flagged"

    def test_extreme_aqi_anomaly(self):
        from sklearn.ensemble import IsolationForest
        np.random.seed(42)
        X_train = np.random.normal(loc=[100, 28, 55, 35], scale=[15, 2, 5, 5], size=(200, 4))
        clf = IsolationForest(contamination=0.05, random_state=42)
        clf.fit(X_train)
        extreme = np.array([[500, 28, 55, 90]])  # extreme AQI
        assert clf.predict(extreme)[0] == -1, "Extreme AQI should be flagged as anomaly"

    def test_model_trained_on_city_data(self):
        """Model should detect anomalies relative to city baseline, not absolute AQI"""
        from sklearn.ensemble import IsolationForest
        np.random.seed(0)
        # City A: normally clean — AQI around 50
        X_a = np.random.normal(loc=[50, 25, 60, 20], scale=[10, 2, 5, 4], size=(100, 4))
        clf_a = IsolationForest(contamination=0.05, random_state=42)
        clf_a.fit(X_a)
        # AQI=80 is a spike for this clean city
        assert clf_a.predict([[80, 25, 60, 30]])[0] == -1

        # City B: normally polluted — AQI around 200
        X_b = np.random.normal(loc=[200, 28, 55, 60], scale=[20, 2, 5, 8], size=(100, 4))
        clf_b = IsolationForest(contamination=0.05, random_state=42)
        clf_b.fit(X_b)
        # AQI=80 is actually very clean for this city — not an anomaly
        assert clf_b.predict([[80, 28, 55, 25]])[0] == 1


# ── Flask API Integration Tests ───────────────────────────────────────────────
# These require the Flask app running on port 5000
# Run with: pytest tests/test_ueris.py -v (after starting app.py)

BASE = "http://localhost:5000"

@pytest.mark.integration
class TestFlaskAPI:
    def test_health(self):
        import requests
        r = requests.get(f"{BASE}/api/health", timeout=5)
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_cities_returns_list(self):
        import requests
        r = requests.get(f"{BASE}/api/cities", timeout=5)
        assert r.status_code == 200
        d = r.json()
        assert "cities" in d
        assert isinstance(d["cities"], list)

    def test_ranking_sorted(self):
        import requests
        r = requests.get(f"{BASE}/api/ranking", timeout=5)
        assert r.status_code == 200
        ranking = r.json()["ranking"]
        scores = [c["health_score"] for c in ranking if c["health_score"] is not None]
        assert scores == sorted(scores, reverse=True), "Ranking should be sorted by health_score desc"

    def test_city_detail_delhi(self):
        import requests
        r = requests.get(f"{BASE}/api/city/Delhi", timeout=5)
        assert r.status_code == 200
        d = r.json()
        assert d["city"] == "Delhi"
        assert "batch_stats" in d
        assert "monthly_trend" in d
        assert "forecast" in d
        assert "correlation" in d

    def test_forecast_structure(self):
        import requests
        r = requests.get(f"{BASE}/api/forecast/Delhi", timeout=5)
        assert r.status_code == 200
        fc = r.json()["forecast"]
        assert len(fc) == 12
        for item in fc:
            assert "month" in item
            assert "predicted_usi" in item
            assert "lower_bound" in item
            assert "upper_bound" in item
            assert item["lower_bound"] <= item["predicted_usi"] <= item["upper_bound"]

    def test_correlation_matrix(self):
        import requests
        r = requests.get(f"{BASE}/api/correlation/Delhi", timeout=5)
        assert r.status_code == 200
        d = r.json()["correlation"]
        assert d["labels"] == ["AQI", "temperature", "humidity", "usi"]
        m = d["matrix"]
        assert len(m) == 4
        for i in range(4):
            assert abs(m[i][i] - 1.0) < 0.01, "Diagonal must be ~1.0 (self-correlation)"

    def test_quality_report(self):
        import requests
        r = requests.get(f"{BASE}/api/quality", timeout=5)
        assert r.status_code == 200
        s = r.json()["summary"]
        assert s["total_cities"] > 0
        assert 0 <= s["coverage_pct"] <= 100

    def test_city_not_found(self):
        import requests
        r = requests.get(f"{BASE}/api/city/InvalidCityXYZ", timeout=5)
        assert r.status_code == 404

    def test_csv_export(self):
        import requests
        r = requests.get(f"{BASE}/api/export/csv", timeout=10)
        assert r.status_code == 200
        assert "text/csv" in r.headers["Content-Type"]
        lines = r.text.strip().split("\n")
        assert len(lines) > 1  # header + at least one city
