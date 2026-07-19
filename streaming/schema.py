"""
streaming/schema.py
====================
UERIS — Kafka Message Schema & Validator

Defines the canonical JSON schema for all environmental readings
flowing through the Kafka pipeline.

Schema version: 1.0
Topic: ueris.env.readings
Key: city name (string)

Message format:
{
    "schema_version": "1.0",
    "source":         "simulator" | "live",
    "timestamp":      "2026-01-01T12:00:00+05:30",  # ISO 8601 with TZ
    "city":           "Delhi",
    "lat":            28.6139,
    "lon":            77.2090,
    "aqi":            185.0,
    "temperature":    29.5,
    "humidity":       55.0,
    "usi":            null,           # null = will be computed by speed layer
    "risk_level":     null,           # null = will be computed by speed layer
    "is_anomaly":     null,           # null = will be set by Isolation Forest
    "data_source":    "Open-Meteo+WAQI",
    "fetch_duration_ms": 245
}

DLQ message format (ueris.env.dlq):
{
    "original_message": "<raw string>",
    "error":            "ValidationError: missing field aqi",
    "topic":            "ueris.env.readings",
    "partition":        0,
    "offset":           12345,
    "timestamp":        "2026-01-01T12:00:05+00:00",
    "retry_count":      2
}
"""

from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = "1.0"

# Required fields and their expected types
REQUIRED_FIELDS: dict[str, type] = {
    "city":        str,
    "aqi":         (int, float),
    "temperature": (int, float),
    "humidity":    (int, float),
    "timestamp":   str,
}

# Optional fields with defaults
OPTIONAL_FIELDS: dict[str, Any] = {
    "schema_version": SCHEMA_VERSION,
    "source":         "simulator",
    "lat":            None,
    "lon":            None,
    "usi":            None,
    "risk_level":     None,
    "is_anomaly":     None,
    "data_source":    "unknown",
    "fetch_duration_ms": None,
}

# Field value constraints
FIELD_CONSTRAINTS: dict[str, tuple] = {
    "aqi":         (0,    2000),   # AQI valid range
    "temperature": (-30,  60),     # Celsius valid range
    "humidity":    (0,    100),    # % valid range
}

VALID_CITIES = {
    'Ahmedabad', 'Aizawl', 'Amaravati', 'Amritsar', 'Bengaluru', 'Bhopal',
    'Brajrajnagar', 'Chandigarh', 'Chennai', 'Coimbatore', 'Delhi', 'Ernakulam',
    'Gurugram', 'Guwahati', 'Hyderabad', 'Jaipur', 'Jorapokhar', 'Kochi',
    'Kolkata', 'Lucknow', 'Mumbai', 'Patna', 'Shillong', 'Talcher',
    'Thiruvananthapuram', 'Visakhapatnam',
    # Aliases
    'Bangalore',
}


class ValidationError(Exception):
    """Raised when a message fails schema validation."""
    pass


def validate(record: dict) -> dict:
    """
    Validate and normalise a reading record against the UERIS message schema.

    Args:
        record: dict parsed from JSON

    Returns:
        Normalised and validated record dict

    Raises:
        ValidationError: if any required field is missing, wrong type, or out of range
    """
    if not isinstance(record, dict):
        raise ValidationError(f"Expected dict, got {type(record).__name__}")

    # Check required fields
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in record:
            raise ValidationError(f"Missing required field: '{field}'")
        if not isinstance(record[field], expected_type):
            raise ValidationError(
                f"Field '{field}' expected {expected_type}, "
                f"got {type(record[field]).__name__}: {record[field]!r}"
            )

    # Validate city
    city = record["city"].strip()
    if not city:
        raise ValidationError("Field 'city' cannot be empty")
    # Non-strict: unknown cities pass with a warning (future expansion)

    # Validate numeric ranges
    for field, (min_val, max_val) in FIELD_CONSTRAINTS.items():
        val = record.get(field)
        if val is not None and not (min_val <= float(val) <= max_val):
            raise ValidationError(
                f"Field '{field}' value {val} is out of valid range "
                f"[{min_val}, {max_val}]"
            )

    # Validate timestamp is parseable
    try:
        ts = record["timestamp"]
        if "T" in ts:
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError) as e:
        raise ValidationError(f"Invalid timestamp format: {record['timestamp']!r} — {e}")

    # Build normalised record with all optional defaults
    normalised = {}
    normalised.update(OPTIONAL_FIELDS)    # start with defaults
    normalised.update(record)             # override with actual values
    normalised["schema_version"] = SCHEMA_VERSION
    normalised["city"]           = city
    normalised["aqi"]            = float(record["aqi"])
    normalised["temperature"]    = float(record["temperature"])
    normalised["humidity"]       = float(record["humidity"])

    return normalised


def build_reading(
    city: str,
    aqi: float,
    temperature: float,
    humidity: float,
    lat: float = None,
    lon: float = None,
    source: str = "simulator",
    data_source: str = "Open-Meteo+WAQI",
    fetch_duration_ms: int = None,
) -> dict:
    """
    Build a validated UERIS message record ready for Kafka publishing.
    USI, risk_level and is_anomaly are intentionally null here —
    they are computed by the speed layer consumer.
    """
    record = {
        "schema_version":    SCHEMA_VERSION,
        "source":            source,
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "city":              city,
        "lat":               lat,
        "lon":               lon,
        "aqi":               round(float(aqi), 1),
        "temperature":       round(float(temperature), 1),
        "humidity":          round(float(humidity), 1),
        "usi":               None,
        "risk_level":        None,
        "is_anomaly":        None,
        "data_source":       data_source,
        "fetch_duration_ms": fetch_duration_ms,
    }
    return validate(record)


def build_dlq_message(
    original_message: str,
    error: str,
    topic: str,
    partition: int = 0,
    offset: int = -1,
    retry_count: int = 0,
) -> dict:
    """Build a Dead Letter Queue message."""
    return {
        "original_message": original_message,
        "error":            error,
        "topic":            topic,
        "partition":        partition,
        "offset":           offset,
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "retry_count":      retry_count,
    }
