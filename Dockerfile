FROM python:3.11-slim

# Java is required by PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data/historical data/streaming_input data/checkpoint dashboard

EXPOSE 5000

CMD ["python", "serving_layer/app.py"]