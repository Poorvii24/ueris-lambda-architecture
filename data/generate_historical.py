"""
generate_historical.py
Generates a realistic historical environmental dataset (1 year of hourly data).
Run this ONCE before starting the project.
Output: data/historical/env_data.csv
"""

import csv
import random
import math
import os
from datetime import datetime, timedelta

random.seed(42)

CITIES = ["Bangalore", "Mumbai", "Delhi", "Hyderabad", "Chennai"]

def simulate_aqi(hour, day_of_year, city):
    """Simulate AQI with daily & seasonal patterns + city baseline."""
    baselines = {"Delhi": 180, "Mumbai": 130, "Bangalore": 80, "Hyderabad": 100, "Chennai": 90}
    base = baselines[city]
    # Rush hour spikes (8am, 6pm)
    rush = 40 * (math.exp(-((hour - 8) ** 2) / 4) + math.exp(-((hour - 18) ** 2) / 4))
    # Seasonal: worse in winter (days 300-365 / 1-60)
    season = 30 * math.cos((day_of_year / 365) * 2 * math.pi)
    noise = random.gauss(0, 10)
    return max(10, round(base + rush + season + noise, 1))

def simulate_temperature(hour, day_of_year, city):
    """Simulate temperature with diurnal cycle and seasons."""
    city_base = {"Delhi": 28, "Mumbai": 30, "Bangalore": 24, "Hyderabad": 29, "Chennai": 31}
    base = city_base[city]
    daily = 8 * math.sin((hour - 6) / 24 * 2 * math.pi)
    seasonal = 6 * math.sin((day_of_year / 365) * 2 * math.pi - 1.5)
    noise = random.gauss(0, 1)
    return round(base + daily + seasonal + noise, 1)

def simulate_humidity(temperature, aqi):
    """Humidity inversely related to temperature, slightly to AQI."""
    base = 70 - (temperature - 24) * 1.2 - (aqi - 100) * 0.05
    noise = random.gauss(0, 5)
    return max(10, min(100, round(base + noise, 1)))

def compute_usi(aqi, temperature, humidity):
    """
    Urban Stress Index (USI) - core metric of this project.
    Normalized weighted combination: 0 = no stress, 100 = extreme stress
    """
    aqi_norm    = min(aqi / 300, 1.0)
    temp_norm   = min(max((temperature - 15) / 25, 0), 1.0)
    humid_norm  = abs(humidity - 50) / 50  # deviation from comfort zone
    usi = (0.5 * aqi_norm + 0.3 * temp_norm + 0.2 * humid_norm) * 100
    return round(usi, 2)

def classify_risk(usi):
    if usi < 20:  return "Low"
    if usi < 40:  return "Moderate"
    if usi < 60:  return "High"
    if usi < 80:  return "Very High"
    return "Severe"

output_path = os.path.join(os.path.dirname(__file__), "historical", "env_data.csv")
os.makedirs(os.path.dirname(output_path), exist_ok=True)

start = datetime(2024, 1, 1)
rows = []

for city in CITIES:
    for hour_offset in range(365 * 24):  # 1 year of hourly data
        ts = start + timedelta(hours=hour_offset)
        aqi  = simulate_aqi(ts.hour, ts.timetuple().tm_yday, city)
        temp = simulate_temperature(ts.hour, ts.timetuple().tm_yday, city)
        hum  = simulate_humidity(temp, aqi)
        usi  = compute_usi(aqi, temp, hum)
        risk = classify_risk(usi)
        rows.append([ts.strftime("%Y-%m-%d %H:%M:%S"), city, aqi, temp, hum, usi, risk])

with open(output_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "city", "aqi", "temperature", "humidity", "usi", "risk_level"])
    writer.writerows(rows)

print(f"Generated {len(rows)} records -> {output_path}")
