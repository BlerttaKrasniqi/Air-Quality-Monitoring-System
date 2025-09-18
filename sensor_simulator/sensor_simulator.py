#!/usr/bin/env python3
import os, json, time, math, signal, random, logging
from datetime import datetime, timezone
from uuid import uuid4
from typing import List, Dict, Any, Optional

# Kafka
from kafka import KafkaProducer
from kafka.errors import KafkaError


BOOTSTRAP      = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC          = os.getenv("KAFKA_TOPIC", "sensor-data")
LOCATION       = os.getenv("SENSOR_LOCATION", "Pristina")
BASE_LAT       = float(os.getenv("SENSOR_LAT", 42.6629))
BASE_LON       = float(os.getenv("SENSOR_LON", 21.1655))
SEND_INTERVAL  = float(os.getenv("SEND_INTERVAL_SEC", "5"))       
GPS_JITTER_M   = float(os.getenv("GPS_JITTER_METERS", "30"))      
SEED           = os.getenv("SIM_SEED")                             
EVENT_PROB     = float(os.getenv("EVENT_PROB", "0.02"))            
EVENT_MEAN_S   = float(os.getenv("EVENT_MEAN_DURATION_SEC", "15")) 
DEVICE_COUNT   = int(os.getenv("DEVICE_COUNT", "1"))               
LOG_LEVEL      = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("simulator")

_run = True
def _graceful(*_):
    global _run
    _run = False
signal.signal(signal.SIGTERM, _graceful)
signal.signal(signal.SIGINT, _graceful)

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def meters_to_deg_latlon(d_meters):
   
    deg_lat = d_meters / 111_320.0
    deg_lon = d_meters / (111_320.0 * math.cos(math.radians(BASE_LAT)))
    return deg_lat, deg_lon

WEATHERS = ("sunny","cloudy","rainy","windy","foggy")
def choose_weather(hour_utc: int) -> str:
  
    r = random.random()
    if 0 <= hour_utc < 6:
        return "foggy" if r < 0.45 else ("cloudy" if r < 0.75 else "rainy")
    if 6 <= hour_utc < 12:
        return "sunny" if r < 0.5 else ("windy" if r < 0.7 else "cloudy")
    if 12 <= hour_utc < 18:
        return "sunny" if r < 0.45 else ("windy" if r < 0.65 else "cloudy")
    return "cloudy" if r < 0.5 else ("rainy" if r < 0.7 else "foggy")

def diurnal_base(hour_utc: int) -> float:
    
    return 0.5 + 0.5*math.sin((hour_utc - 5)/24.0 * 2*math.pi)

def simulate_sample(t: datetime, device_state: Dict[str, Any]) -> Dict[str, Any]:
    hour = t.hour
    weather = choose_weather(hour)

  
    base = diurnal_base(hour)
    temperature = 7 + 15*base + random.uniform(-1.2, 1.2)


    humidity = clamp(85 - 25*base + random.uniform(-5, 5), 30, 98)
    if weather == "rainy": humidity = clamp(humidity + 12, 40, 100)
    if weather == "foggy": humidity = clamp(humidity + 8,  40, 100)
    if weather == "sunny": humidity = clamp(humidity - 6,  25, 95)

  
    wind_speed = clamp(
        (2 if weather in ("foggy","rainy") else 5) + random.uniform(-1.5, 4.5),
        0.0, 28.0
    )
    wind_direction = int(random.uniform(0, 360))

    pm25 = 8 + 20*(1-base) + (6 if weather in ("foggy","rainy","cloudy") else -3) + random.uniform(-3, 3)
    pm10 = pm25 * random.uniform(1.6, 2.8)
    co2  = 420 + 50*(1-base) + (35 if weather in ("foggy","rainy") else -10) + random.uniform(-10, 12)

 
    if device_state.get("lat") is None:
        device_state["lat"] = BASE_LAT + meters_to_deg_latlon(random.uniform(-GPS_JITTER_M, GPS_JITTER_M))[0]
        device_state["lon"] = BASE_LON + meters_to_deg_latlon(random.uniform(-GPS_JITTER_M, GPS_JITTER_M))[1]
    else:
        dlat, dlon = meters_to_deg_latlon(random.uniform(-GPS_JITTER_M, GPS_JITTER_M))
        device_state["lat"] = clamp(device_state["lat"] + dlat, BASE_LAT - 0.02, BASE_LAT + 0.02)
        device_state["lon"] = clamp(device_state["lon"] + dlon, BASE_LON - 0.02, BASE_LON + 0.02)

   
    events: List[str] = device_state.get("events", [])
    
    if not events and random.random() < EVENT_PROB:
        events = [random.choices(["wildfire","industrial_spill","traffic_jam"], weights=[0.5,0.3,0.2])[0]]
        device_state["event_until"] = t.timestamp() + random.expovariate(1.0 / max(1.0, EVENT_MEAN_S))
        log.info("Event triggered: %s", events[0])
 
    if events and t.timestamp() >= device_state.get("event_until", 0):
        log.info("Event resolved: %s", events[0])
        events = []
        device_state["event_until"] = None
    device_state["events"] = events

    if "wildfire" in events:
        pm25 *= random.uniform(12, 24)
        pm10 *= random.uniform(16, 30)
        co2  += random.uniform(120, 220)
    elif "industrial_spill" in events:
        pm25 *= random.uniform(4, 10)
        pm10 *= random.uniform(6, 14)
        co2  += random.uniform(80, 160)
    elif "traffic_jam" in events:
        pm25 *= random.uniform(2, 3.5)
        pm10 *= random.uniform(2.2, 4.0)
        co2  += random.uniform(40, 90)

  
    pm25 = clamp(pm25, 1, 800_000)
    pm10 = clamp(pm10, 5, 8_500_000)
    co2  = clamp(co2, 350, 10_000)

    rec: Dict[str, Any] = {
        "sensor_id": str(uuid4()),
        "location": LOCATION,
        "latitude": round(device_state["lat"], 6),
        "longitude": round(device_state["lon"], 6),
        "timestamp": t.replace(tzinfo=timezone.utc).isoformat(),
        "temperature": round(temperature, 1),
        "humidity": round(humidity, 1),
        "pm25": round(pm25, 2),
        "pm10": round(pm10, 2),
        "co2": round(co2, 1),
        "weather": weather,
        "wind_speed": round(wind_speed, 1),
        "wind_direction": wind_direction,
        "events": events,
    }
    return rec


def build_producer() -> KafkaProducer:

    for attempt in range(1, 16):
        try:
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                linger_ms=100,
                retries=5,
                max_in_flight_requests_per_connection=5,
            )
            log.info("Connected to Kafka at %s", BOOTSTRAP)
            return p
        except KafkaError as e:
            log.warning("Kafka not ready (%s), retry %d/15 ...", e, attempt)
            time.sleep(2)
    raise RuntimeError("Could not connect to Kafka")


def main():
    if SEED is not None:
        random.seed(int(SEED))
        log.info("Using fixed random seed %s", SEED)

    prod = build_producer()
   
    device_states = [{"lat": None, "lon": None, "events": []} for _ in range(DEVICE_COUNT)]

    log.info("Starting simulator: topic=%s interval=%ss devices=%d location=%s",
             TOPIC, SEND_INTERVAL, DEVICE_COUNT, LOCATION)

    sent = 0
    t0 = time.time()
    while _run:
        now = datetime.utcnow()
        for d in device_states:
            record = simulate_sample(now, d)
            try:
                prod.send(TOPIC, key=record["sensor_id"], value=record)
                sent += 1
                log.info("Sent: %s", record)
            except KafkaError as e:
                log.error("Send failed: %s", e, exc_info=True)
        prod.flush(timeout=5)
        time.sleep(SEND_INTERVAL)

  
    elapsed = max(0.001, time.time() - t0)
    log.info("Simulator stopped. Sent %d messages (%.2f msg/s).", sent, sent/elapsed)

if __name__ == "__main__":
    main()
