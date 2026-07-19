# =============================================================================
# UERIS — Dockerfile
# =============================================================================
# Multi-purpose image:
#   - Flask serving layer (default CMD)
#   - Stream simulator (docker-compose override)
#   - Speed layer file consumer (docker-compose override)
#   - Spark Kafka consumer (docker-compose override, needs Java)
#
# Build args:
#   INSTALL_SPARK=false  (default) — lean image for Flask + simulator
#   INSTALL_SPARK=true   — includes Java + PySpark for speed layers
#
# Build examples:
#   docker build -t ueris-app .
#   docker build --build-arg INSTALL_SPARK=true -t ueris-spark .
# =============================================================================

ARG INSTALL_SPARK=false

FROM python:3.11-slim AS base

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# ── Spark stage: adds Java + PySpark ──────────────────────────────────────────
FROM base AS with-spark

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# ── Final stage selection ──────────────────────────────────────────────────────
FROM base AS final-false
FROM with-spark AS final-true
FROM final-${INSTALL_SPARK} AS final

WORKDIR /app

# Install Python dependencies (includes confluent-kafka for Kafka mode)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# For Spark-enabled builds, also install PySpark
ARG INSTALL_SPARK=false
RUN if [ "$INSTALL_SPARK" = "true" ]; then \
      pip install --no-cache-dir "pyspark>=3.4.0"; \
    fi

# Copy application code
COPY . .

# Create required directories
RUN mkdir -p \
    data/historical \
    data/streaming_input \
    data/checkpoint \
    data/checkpoint_kafka \
    data/dlq \
    dashboard

EXPOSE 5000

# Default: run Flask serving layer with gunicorn
CMD ["gunicorn", \
     "-w", "1", \
     "--threads", "4", \
     "-b", "0.0.0.0:5000", \
     "--timeout", "120", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "serving_layer.app:app"]
