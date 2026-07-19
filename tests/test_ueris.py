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
        """Model should detect anomalies relative to city baseline."""
        from sklearn.ensemble import IsolationForest
        np.random.seed(42)
        # City A: normally clean — AQI around 50, tight distribution
        X_a = np.random.normal(loc=[50, 25, 60, 20], scale=[5, 1, 3, 2], size=(500, 4))
        clf_a = IsolationForest(contamination=0.05, n_estimators=200, random_state=42)
        clf_a.fit(X_a)
        # AQI=500 is a massive spike — must be anomaly
        assert clf_a.predict([[500, 25, 60, 90]])[0] == -1

        # City B: normally polluted — AQI around 200, tight distribution
        X_b = np.random.normal(loc=[200, 28, 55, 60], scale=[5, 1, 3, 2], size=(500, 4))
        clf_b = IsolationForest(contamination=0.05, n_estimators=200, random_state=42)
        clf_b.fit(X_b)
        # Within normal range for this city — not an anomaly
        assert clf_b.predict([[200, 28, 55, 60]])[0] == 1


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


# ═══════════════════════════════════════════════════════════════════════════════
# PART 8 — Enterprise Streaming Engine Tests
# Added by Phase 1 upgrade
# ═══════════════════════════════════════════════════════════════════════════════

import json
import os
import tempfile
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone, timedelta


# ── Schema Tests ───────────────────────────────────────────────────────────────

class TestSchema:
    """Tests for streaming/schema.py"""

    def test_build_reading_valid(self):
        from streaming.schema import build_reading
        r = build_reading(
            city="Delhi", aqi=185.0, temperature=29.5, humidity=55.0,
            lat=28.6139, lon=77.209, source="simulator"
        )
        assert r["city"] == "Delhi"
        assert r["aqi"]  == 185.0
        assert r["schema_version"] == "1.0"
        assert r["usi"]  is None    # computed by speed layer, not producer
        assert r["is_anomaly"] is None

    def test_validate_missing_field(self):
        from streaming.schema import validate, ValidationError
        with pytest.raises(ValidationError, match="Missing required field"):
            validate({"city": "Delhi", "aqi": 100})  # missing temperature + humidity

    def test_validate_wrong_type(self):
        from streaming.schema import validate, ValidationError
        with pytest.raises(ValidationError):
            validate({
                "city": "Delhi", "aqi": "not_a_number",
                "temperature": 29.5, "humidity": 55.0,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })

    def test_validate_aqi_out_of_range(self):
        from streaming.schema import validate, ValidationError
        with pytest.raises(ValidationError, match="out of valid range"):
            validate({
                "city": "Delhi", "aqi": 9999,
                "temperature": 29.5, "humidity": 55.0,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })

    def test_validate_humidity_out_of_range(self):
        from streaming.schema import validate, ValidationError
        with pytest.raises(ValidationError, match="out of valid range"):
            validate({
                "city": "Delhi", "aqi": 100,
                "temperature": 29.5, "humidity": 150.0,  # > 100
                "timestamp": datetime.now(timezone.utc).isoformat()
            })

    def test_validate_bad_timestamp(self):
        from streaming.schema import validate, ValidationError
        with pytest.raises(ValidationError, match="Invalid timestamp"):
            validate({
                "city": "Delhi", "aqi": 100,
                "temperature": 29.5, "humidity": 55.0,
                "timestamp": "not-a-date"
            })

    def test_validate_normalises_floats(self):
        from streaming.schema import validate
        r = validate({
            "city": "Delhi", "aqi": 100,
            "temperature": 29, "humidity": 55,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        assert isinstance(r["aqi"],         float)
        assert isinstance(r["temperature"], float)
        assert isinstance(r["humidity"],    float)

    def test_build_dlq_message(self):
        from streaming.schema import build_dlq_message
        dlq = build_dlq_message(
            original_message='{"bad": "msg"}',
            error="ValidationError: missing aqi",
            topic="ueris.env.readings",
            partition=0, offset=42, retry_count=2
        )
        assert dlq["error"]       == "ValidationError: missing aqi"
        assert dlq["offset"]      == 42
        assert dlq["retry_count"] == 2
        assert "timestamp" in dlq

    def test_valid_message_round_trip(self):
        """Build → serialise to JSON → parse → validate"""
        from streaming.schema import build_reading, validate
        record  = build_reading("Mumbai", 93.0, 28.5, 58.0)
        as_json = json.dumps(record)
        parsed  = json.loads(as_json)
        result  = validate(parsed)
        assert result["city"] == "Mumbai"


# ── Monitoring Tests ───────────────────────────────────────────────────────────

class TestPipelineMetrics:
    """Tests for streaming/monitoring.py PipelineMetrics"""

    def test_message_counts(self):
        from streaming.monitoring import PipelineMetrics
        m = PipelineMetrics(window_seconds=60)
        m.record_message_sent("Delhi")
        m.record_message_sent("Mumbai")
        m.record_message_received("Delhi", latency_ms=45.0)
        snap = m.get_snapshot()
        assert snap["messages_sent"]     == 2
        assert snap["messages_received"] == 1
        assert snap["cities_active"]     == 2

    def test_error_counting(self):
        from streaming.monitoring import PipelineMetrics
        m = PipelineMetrics()
        m.record_error("ValidationError", city="Delhi")
        m.record_error("ValidationError", city="Mumbai")
        m.record_error("JSONDecodeError")
        snap = m.get_snapshot()
        assert snap["messages_failed"] == 3
        assert snap["error_types"]["ValidationError"] == 2
        assert snap["error_types"]["JSONDecodeError"] == 1

    def test_dlq_counting(self):
        from streaming.monitoring import PipelineMetrics
        m = PipelineMetrics()
        m.record_dlq("ValidationError")
        m.record_dlq("MaxRetriesExceeded")
        snap = m.get_snapshot()
        assert snap["dlq_count"] == 2

    def test_anomaly_counting(self):
        from streaming.monitoring import PipelineMetrics
        m = PipelineMetrics()
        m.record_message_received("Delhi")
        m.record_message_received("Mumbai")
        m.record_anomaly("Delhi")
        snap = m.get_snapshot()
        assert snap["anomaly_count"] == 1

    def test_error_rate(self):
        from streaming.monitoring import PipelineMetrics
        m = PipelineMetrics()
        for _ in range(8): m.record_message_sent("Delhi")
        for _ in range(2): m.record_error("SomeError")
        snap = m.get_snapshot()
        assert abs(snap["error_rate_pct"] - 25.0) < 0.1  # 2/8 = 25%

    def test_latency_tracking(self):
        from streaming.monitoring import PipelineMetrics
        m = PipelineMetrics()
        for lat in [10.0, 20.0, 30.0, 40.0, 50.0]:
            m.record_message_received("Delhi", latency_ms=lat)
        snap = m.get_snapshot()
        assert snap["avg_latency_ms"] == 30.0


# ── DLQ Tests ──────────────────────────────────────────────────────────────────

class TestDLQHandler:
    """Tests for streaming/dlq_handler.py"""

    def test_writes_to_local_file(self):
        from streaming.dlq_handler import DLQHandler
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"DLQ_DIR": tmpdir}):
                dlq = DLQHandler()
                dlq.send(
                    raw_message='{"city": "Delhi", "aqi": "bad"}',
                    error="ValidationError: aqi wrong type",
                    topic="ueris.env.readings",
                    partition=0, offset=10
                )
                # Check file was created and contains the message
                files = list(os.listdir(tmpdir))
                assert len(files) == 1
                assert files[0].endswith(".jsonl")
                with open(os.path.join(tmpdir, files[0])) as f:
                    lines = f.readlines()
                assert len(lines) == 1
                msg = json.loads(lines[0])
                assert msg["error"] == "ValidationError: aqi wrong type"
                assert msg["offset"] == 10

    def test_multiple_messages_same_file(self):
        from streaming.dlq_handler import DLQHandler
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"DLQ_DIR": tmpdir}):
                dlq = DLQHandler()
                for i in range(5):
                    dlq.send(f"bad_msg_{i}", f"Error {i}", "ueris.env.readings")
                files = list(os.listdir(tmpdir))
                assert len(files) == 1
                with open(os.path.join(tmpdir, files[0])) as f:
                    lines = f.readlines()
                assert len(lines) == 5

    def test_should_retry_logic(self):
        from streaming.dlq_handler import DLQHandler
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"DLQ_DIR": tmpdir, "DLQ_MAX_RETRIES": "3"}):
                dlq = DLQHandler()
                assert dlq.should_retry(0) is True
                assert dlq.should_retry(2) is True
                assert dlq.should_retry(3) is False
                assert dlq.should_retry(5) is False

    def test_kafka_producer_called(self):
        from streaming.dlq_handler import DLQHandler
        mock_producer = MagicMock()
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"DLQ_DIR": tmpdir}):
                dlq = DLQHandler(kafka_producer=mock_producer, dlq_topic="ueris.env.dlq")
                dlq.send("bad_msg", "TestError", "ueris.env.readings")
                mock_producer.produce.assert_called_once()
                # confluent-kafka Producer.produce() takes topic as positional arg
                call_args = mock_producer.produce.call_args
                # topic can be positional (args[0]) or keyword
                topic_val = (
                    call_args[0][0]
                    if call_args[0]
                    else call_args[1].get("topic", "")
                )
                assert topic_val == "ueris.env.dlq"


# ── Kafka Config Tests ─────────────────────────────────────────────────────────

class TestKafkaConfig:
    """Tests for streaming/kafka_config.py"""

    def test_producer_config_defaults(self):
        from streaming import kafka_config
        cfg = kafka_config.get_producer_config()
        assert "bootstrap.servers" in cfg
        assert cfg["acks"]             == "all"
        assert cfg["enable.idempotence"] is True

    def test_consumer_config_defaults(self):
        from streaming import kafka_config
        cfg = kafka_config.get_consumer_config()
        assert "bootstrap.servers" in cfg
        assert "group.id"           in cfg
        assert cfg["enable.auto.commit"] is False  # must be manual

    def test_spark_kafka_options(self):
        from streaming import kafka_config
        opts = kafka_config.get_spark_kafka_options()
        assert "kafka.bootstrap.servers" in opts
        assert "subscribe"               in opts
        assert opts["subscribe"] == kafka_config.KAFKA_TOPIC

    def test_env_var_override(self):
        with patch.dict(os.environ, {
            "KAFKA_BROKER": "my-broker:9092",
            "KAFKA_TOPIC":  "my-topic",
        }):
            import importlib
            from streaming import kafka_config as kc
            importlib.reload(kc)
            assert kc.KAFKA_BROKER == "my-broker:9092"
            assert kc.KAFKA_TOPIC  == "my-topic"

    def test_sasl_config_added_when_non_plaintext(self):
        with patch.dict(os.environ, {
            "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_SASL_USERNAME":     "user",
            "KAFKA_SASL_PASSWORD":     "pass",
        }):
            import importlib
            from streaming import kafka_config as kc
            importlib.reload(kc)
            cfg = kc.get_producer_config()
            assert "sasl.username" in cfg
            assert cfg["sasl.username"] == "user"

    def test_plaintext_no_sasl(self):
        with patch.dict(os.environ, {"KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"}):
            import importlib
            from streaming import kafka_config as kc
            importlib.reload(kc)
            cfg = kc.get_producer_config()
            assert "sasl.username" not in cfg


# ── Simulation Mode Tests ──────────────────────────────────────────────────────

class TestSimulationMode:
    """Tests for data/stream_simulator.py SimulationMode"""

    def test_writes_json_file(self):
        from streaming.schema import build_reading
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"STREAM_OUTPUT_DIR": tmpdir}):
                # Import here to get fresh instance with patched env
                import importlib
                import data.stream_simulator as sim
                importlib.reload(sim)
                backend = sim.SimulationMode()
                record  = build_reading("Delhi", 185.0, 29.5, 55.0)
                result  = backend.publish(record)
                assert result is True
                files = list(os.listdir(tmpdir))
                assert len(files) == 1
                with open(os.path.join(tmpdir, files[0])) as f:
                    saved = json.load(f)
                assert saved["city"] == "Delhi"
                assert saved["aqi"]  == 185.0

    def test_cleans_stale_files_on_init(self):
        from streaming.schema import build_reading
        with tempfile.TemporaryDirectory() as tmpdir:
            # Put a stale file in the dir
            stale = os.path.join(tmpdir, "stream_000000.json")
            with open(stale, "w") as f:
                json.dump({"city": "old"}, f)
            assert os.path.exists(stale)
            with patch.dict(os.environ, {"STREAM_OUTPUT_DIR": tmpdir}):
                import importlib
                import data.stream_simulator as sim
                importlib.reload(sim)
                sim.SimulationMode()  # should delete stale file
                assert not os.path.exists(stale)

    def test_counter_increments(self):
        from streaming.schema import build_reading
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"STREAM_OUTPUT_DIR": tmpdir}):
                import importlib
                import data.stream_simulator as sim
                importlib.reload(sim)
                backend = sim.SimulationMode()
                for city in ["Delhi", "Mumbai", "Chennai"]:
                    backend.publish(build_reading(city, 100.0, 28.0, 55.0))
                files = sorted(os.listdir(tmpdir))
                assert files[0] == "stream_000000.json"
                assert files[1] == "stream_000001.json"
                assert files[2] == "stream_000002.json"


# ── Speed Layer Unit Tests ─────────────────────────────────────────────────────

class TestSpeedLayerFunctions:
    """Tests for speed_layer/functions.py (PySpark-free pure functions)"""

    def test_upsert_with_retry_success(self):
        """Should succeed on first attempt"""
        from speed_layer.functions import upsert_with_retry
        mock_col = MagicMock()
        mock_col.update_one.return_value = MagicMock()
        result = upsert_with_retry(mock_col, "Delhi", {"city": "Delhi", "aqi": 185})
        assert result is True
        mock_col.update_one.assert_called_once()

    def test_upsert_with_retry_retries_on_autoreconnect(self):
        """Should retry on AutoReconnect and succeed on 3rd attempt"""
        import pymongo.errors
        from speed_layer.functions import upsert_with_retry
        mock_col = MagicMock()
        mock_col.update_one.side_effect = [
            pymongo.errors.AutoReconnect("connection lost"),
            pymongo.errors.AutoReconnect("connection lost"),
            MagicMock(),  # success on 3rd
        ]
        with patch("speed_layer.functions.time.sleep"):
            result = upsert_with_retry(
                mock_col, "Delhi", {"city": "Delhi"}, max_retries=3
            )
        assert result is True
        assert mock_col.update_one.call_count == 3

    def test_upsert_with_retry_fails_after_max(self):
        """Should return False after exhausting all retries"""
        import pymongo.errors
        from speed_layer.functions import upsert_with_retry
        mock_col = MagicMock()
        mock_col.update_one.side_effect = pymongo.errors.AutoReconnect("down")
        with patch("speed_layer.functions.time.sleep"):
            result = upsert_with_retry(
                mock_col, "Delhi", {"city": "Delhi"}, max_retries=3
            )
        assert result is False
        assert mock_col.update_one.call_count == 3

    def test_is_ml_anomaly_threshold_fallback(self):
        """When no model available, fall back to AQI > 200"""
        from speed_layer.functions import is_ml_anomaly
        assert is_ml_anomaly("UnknownCity", 250, 28, 55, 65, {}) == (True,  "threshold")
        assert is_ml_anomaly("UnknownCity", 150, 28, 55, 45, {}) == (False, "threshold")

    def test_is_ml_anomaly_uses_model(self):
        """When model available, use Isolation Forest prediction"""
        from sklearn.ensemble import IsolationForest
        from speed_layer.functions import is_ml_anomaly
        import numpy as np
        np.random.seed(42)
        X_train = np.random.normal(loc=[100, 28, 55, 35], scale=[10, 2, 5, 5], size=(200, 4))
        clf = IsolationForest(contamination=0.05, random_state=42)
        clf.fit(X_train)
        models = {"Delhi": clf}
        is_anom, method = is_ml_anomaly("Delhi", 100, 28, 55, 35, models)
        assert method == "IsolationForest"

    def test_save_checkpoint(self):
        from speed_layer.functions import save_checkpoint as _save_checkpoint
        with tempfile.TemporaryDirectory() as tmpdir:
            ckpt = os.path.join(tmpdir, "checkpoint.txt")
            processed = {"/path/to/file1.json", "/path/to/file2.json"}
            _save_checkpoint(ckpt, processed)
            with open(ckpt) as f:
                lines = set(f.read().splitlines())
            assert lines == processed
