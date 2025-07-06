import time
import json
import math
import random
from datetime import datetime
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

state = {
    "pm25": 8.0,
    "pm10": 15.0,
    "co2": 420.0,
    "humidity": 45.0
}

def day_night_temperature():
    """Simulate 24-hour temperature fluctuation (sinusoidal)."""
    now = datetime.utcnow()
    hour = now.hour + now.minute / 60
    # Day-night sinusoidal fluctuation
    base_temp = 22 + 5 * math.sin((2 * math.pi / 24) * (hour - 6))  # peak at 12:00 UTC
    return round(base_temp + random.uniform(-0.3, 0.3), 2)  # add micro-variation

def update_pm(pm_value):
    """Simulate slow drifting of particles like PM2.5 and PM10."""
    drift = random.uniform(-1, 1)
    pm_value += drift
    return max(0, round(pm_value, 2))

def update_co2(co2_value):
    """Simulate slow buildup of CO₂ in closed space, with occasional decay."""
    increase = random.uniform(1, 3)
    decay = random.uniform(0, 2)  # passive air leak
    co2_value += increase - decay
    return round(min(max(co2_value, 400), 2000), 2)

def update_humidity(humidity, temperature):
    """Humidity usually drops as temperature rises and vice versa."""
    temp_factor = -0.3 if temperature > 24 else 0.3
    humidity += temp_factor + random.uniform(-0.5, 0.5)
    return round(min(max(humidity, 30), 90), 2)

def generate_data():
    temperature = day_night_temperature()
    state["pm25"] = update_pm(state["pm25"])
    state["pm10"] = update_pm(state["pm10"])
    state["co2"] = update_co2(state["co2"])
    state["humidity"] = update_humidity(state["humidity"], temperature)

    return {
        "pm25": state["pm25"],
        "pm10": state["pm10"],
        "co2": state["co2"],
        "temperature": temperature,
        "humidity": state["humidity"],
        "timestamp": datetime.utcnow().isoformat()
    }

print(" Smart air quality sensor running without events...")

while True:
    data = generate_data()
    producer.send('air_quality', value=data)
    print("Sent:", data)
    time.sleep(5)
