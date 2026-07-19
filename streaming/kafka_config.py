"""
streaming/kafka_config.py
==========================
UERIS — Centralised Kafka Configuration

All Kafka settings are loaded exclusively from environment variables.
No hardcoded addresses. Supports both local (KRaft) and cloud (Confluent/MSK) brokers.

Environment variables:
    KAFKA_BROKER            Kafka bootstrap server (default: localhost:9092)
    KAFKA_TOPIC             Main stream topic (default: ueris.env.readings)
    KAFKA_DLQ_TOPIC         Dead Letter Queue topic (default: ueris.env.dlq)
    KAFKA_CONSUMER_GROUP    Consumer group ID (default: ueris-speed-layer)
    KAFKA_AUTO_OFFSET       Auto offset reset (default: latest)
    KAFKA_SECURITY_PROTOCOL PLAINTEXT / SASL_SSL (default: PLAINTEXT)
    KAFKA_SASL_MECHANISM    PLAIN / SCRAM-SHA-256 (for cloud brokers)
    KAFKA_SASL_USERNAME     SASL username (for cloud brokers)
    KAFKA_SASL_PASSWORD     SASL password (for cloud brokers)
    KAFKA_PRODUCER_RETRIES  Producer retry count (default: 5)
    KAFKA_REQUEST_TIMEOUT   Producer request timeout ms (default: 30000)
    KAFKA_DELIVERY_TIMEOUT  Producer delivery timeout ms (default: 120000)
    KAFKA_BATCH_SIZE        Producer batch size bytes (default: 16384)
    KAFKA_LINGER_MS         Producer linger ms (default: 10)
    KAFKA_ACKS              Producer acks: 0/1/all (default: all)
"""

import os


# ── Topic names ────────────────────────────────────────────────────────────────
KAFKA_TOPIC       = os.environ.get("KAFKA_TOPIC",       "ueris.env.readings")
KAFKA_DLQ_TOPIC   = os.environ.get("KAFKA_DLQ_TOPIC",   "ueris.env.dlq")

# ── Connection ─────────────────────────────────────────────────────────────────
KAFKA_BROKER            = os.environ.get("KAFKA_BROKER",            "localhost:9092")
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM    = os.environ.get("KAFKA_SASL_MECHANISM",    "PLAIN")
KAFKA_SASL_USERNAME     = os.environ.get("KAFKA_SASL_USERNAME",     "")
KAFKA_SASL_PASSWORD     = os.environ.get("KAFKA_SASL_PASSWORD",     "")

# ── Consumer ───────────────────────────────────────────────────────────────────
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "ueris-speed-layer")
KAFKA_AUTO_OFFSET    = os.environ.get("KAFKA_AUTO_OFFSET",    "latest")

# ── Producer tuning ────────────────────────────────────────────────────────────
KAFKA_PRODUCER_RETRIES  = int(os.environ.get("KAFKA_PRODUCER_RETRIES",  "5"))
KAFKA_REQUEST_TIMEOUT   = int(os.environ.get("KAFKA_REQUEST_TIMEOUT",   "30000"))
KAFKA_DELIVERY_TIMEOUT  = int(os.environ.get("KAFKA_DELIVERY_TIMEOUT",  "120000"))
KAFKA_BATCH_SIZE        = int(os.environ.get("KAFKA_BATCH_SIZE",        "16384"))
KAFKA_LINGER_MS         = int(os.environ.get("KAFKA_LINGER_MS",         "10"))
KAFKA_ACKS              = os.environ.get("KAFKA_ACKS",                   "all")


def get_producer_config() -> dict:
    """
    Return confluent-kafka producer configuration dict.
    Includes SASL/SSL settings when KAFKA_SECURITY_PROTOCOL != PLAINTEXT.
    """
    cfg = {
        "bootstrap.servers":                    KAFKA_BROKER,
        "acks":                                 KAFKA_ACKS,
        "retries":                              KAFKA_PRODUCER_RETRIES,
        "request.timeout.ms":                   KAFKA_REQUEST_TIMEOUT,
        "delivery.timeout.ms":                  KAFKA_DELIVERY_TIMEOUT,
        "batch.size":                           KAFKA_BATCH_SIZE,
        "linger.ms":                            KAFKA_LINGER_MS,
        "enable.idempotence":                   True,
        # Required when enable.idempotence=True — must be <= 5
        "max.in.flight.requests.per.connection": 5,
        "compression.type":                     "snappy",
        "message.max.bytes":                    1048576,
    }
    if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        cfg.update({
            "security.protocol": KAFKA_SECURITY_PROTOCOL,
            "sasl.mechanism":    KAFKA_SASL_MECHANISM,
            "sasl.username":     KAFKA_SASL_USERNAME,
            "sasl.password":     KAFKA_SASL_PASSWORD,
        })
    return cfg


def get_consumer_config(group_id: str = None) -> dict:
    """
    Return confluent-kafka consumer configuration dict.
    """
    cfg = {
        "bootstrap.servers":        KAFKA_BROKER,
        "group.id":                 group_id or KAFKA_CONSUMER_GROUP,
        "auto.offset.reset":        KAFKA_AUTO_OFFSET,
        "enable.auto.commit":       False,   # manual commit for at-least-once
        "max.poll.interval.ms":     300000,  # 5 min max processing time
        "session.timeout.ms":       30000,
        "heartbeat.interval.ms":    10000,
        "fetch.min.bytes":          1,
        "fetch.wait.max.ms":        500,
    }
    if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        cfg.update({
            "security.protocol":    KAFKA_SECURITY_PROTOCOL,
            "sasl.mechanism":       KAFKA_SASL_MECHANISM,
            "sasl.username":        KAFKA_SASL_USERNAME,
            "sasl.password":        KAFKA_SASL_PASSWORD,
        })
    return cfg


def get_spark_kafka_options() -> dict:
    """
    Return Spark Structured Streaming Kafka options dict.
    Used in: spark.readStream.format('kafka').options(**get_spark_kafka_options())
    """
    opts = {
        "kafka.bootstrap.servers": KAFKA_BROKER,
        "subscribe":               KAFKA_TOPIC,
        "startingOffsets":         "latest",
        "failOnDataLoss":          "false",
        "maxOffsetsPerTrigger":    "1000",
        "kafka.request.timeout.ms": str(KAFKA_REQUEST_TIMEOUT),
        "kafka.session.timeout.ms": "30000",
    }
    if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        opts.update({
            "kafka.security.protocol":  KAFKA_SECURITY_PROTOCOL,
            "kafka.sasl.mechanism":     KAFKA_SASL_MECHANISM,
            "kafka.sasl.jaas.config":   (
                f"org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{KAFKA_SASL_USERNAME}" password="{KAFKA_SASL_PASSWORD}";'
            ),
        })
    return opts
