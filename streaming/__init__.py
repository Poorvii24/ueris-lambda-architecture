"""
streaming/
==========
UERIS Enterprise Streaming Engine

Modules:
    kafka_config  — centralised Kafka configuration (env vars only)
    schema        — JSON message schema, validator, builder
    producer      — Kafka producer with retry + DLQ
    consumer      — Kafka consumer with Isolation Forest + MongoDB upsert
    dlq_handler   — Dead Letter Queue (Kafka + local file)
    monitoring    — structured logging + pipeline metrics
"""
