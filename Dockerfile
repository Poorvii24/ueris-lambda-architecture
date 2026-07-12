FROM python:3.11-slim

# No Java needed — PySpark runs locally only
# Server only runs Flask + background live data worker

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data/historical data/streaming_input data/checkpoint dashboard

EXPOSE 5000

# Use gunicorn for production
CMD ["gunicorn", "-w", "1", "--threads", "4", "-b", "0.0.0.0:5000", "--timeout", "120", "serving_layer.app:app"]