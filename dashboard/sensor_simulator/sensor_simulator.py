#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import math
import random
import logging
from threading import Thread
from collections import deque
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer
from faker import Faker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("smart-sensor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")


LOCAL_TZ_OFFSET_HOURS = float(os.getenv("LOCAL_TZ_OFFSET_HOURS", "2"))

CALIB_TEMP_BIAS = float(os.getenv("CALIB_TEMP_BIAS", "0.0"))   
CALIB_TEMP_GAIN = float(os.getenv("CALIB_TEMP_GAIN", "1.0"))   
REAL_TEMP_HINT = os.getenv("REAL_TEMP_HINT")                   
EMA_ALPHA = float(os.getenv("CALIB_EMA_ALPHA", "0.15"))         
EMA_MAX_ABS = float(os.getenv("CALIB_EMA_MAX_ABS", "5.0"))     

class SmartAirQualitySensor:
    def __init__(self, config):
        random.seed(42)
        self.fake = Faker()
        Faker.seed(42)

        self.location = config['city']
        self.latitude = float(config['latitude'])
        self.longitude = float(config['longitude'])
        self.elevation = float(config.get('elevation', 0))
        self.industrial_zones = config.get('industrial_zones', [])

        # Weather state
        self.weather = 'sunny'
        self.wind_speed = 5.0
        self.wind_direction = 0
        self._weather_bias = 0.0

        # Monthly realistic bounds (Pristina-ish)
        self.month_ranges = {
            1: (-4, 6),   2: (-2, 8),   3: (2, 14),
            4: (7, 18),   5: (12, 24),  6: (15, 29),
            7: (17, 32),  8: (16, 31),  9: (12, 28),
            10: (8, 22),  11: (3, 14),  12: (-2, 7),
        }

    
        self.state = {"pm25": 30.0, "pm10": 60.0, "co2": 410.0, "temperature": 20.0, "humidity": 65.0}
        self.history = {k: deque(maxlen=100) for k in self.state}

       
        self._ema_bias = 0.0

      
        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=200,
            retries=6,
        )

        self.events = []
        self.running = True
        Thread(target=self._event_loop, daemon=True).start()

        log.info(f"Kafka bootstrap: {KAFKA_BOOTSTRAP}")
        log.info(f"Kafka topic    : {KAFKA_TOPIC}")
        log.info(f"Local TZ offset: {LOCAL_TZ_OFFSET_HOURS}h")
        log.info(f"Init at {self.location} ({self.latitude},{self.longitude}), elev {self.elevation} m")

    def _simulate_weather(self):
        opts = ['sunny', 'cloudy', 'rainy', 'windy', 'foggy']
        self._weather_bias += random.uniform(-0.15, 0.15)
        idx = (opts.index(self.weather) + round(self._weather_bias)) % len(opts)
        self.weather = opts[idx]
        self.wind_speed = round(max(0.0, self.wind_speed + random.uniform(-0.8, 0.8)), 1)
        self.wind_direction = int((self.wind_direction + random.randint(-12, 12)) % 360)

    def _event_loop(self):
        kinds = ['traffic_jam', 'industrial_spill', 'wildfire']
        while self.running:
            if random.random() < 0.05:
                e = random.choice(kinds)
                self.events.append(e)
                log.info(f"Event triggered: {e}")
                time.sleep(random.randint(10, 30))
                try:
                    self.events.remove(e)
                except ValueError:
                    pass
                log.info(f"Event resolved: {e}")
            time.sleep(5)

    def _apply_event_effects(self, d):
        for e in self.events:
            if e == 'traffic_jam':
                d['pm25'] *= 1.5; d['pm10'] *= 1.5; d['co2'] += 15
            elif e == 'industrial_spill':
                d['pm25'] *= 2.0; d['pm10'] *= 2.0; d['co2'] += 25
            elif e == 'wildfire':
                d['pm25'] *= 3.0; d['pm10'] *= 3.0; d['co2'] += 40
        return d

   
    def _month_bounds(self, dt):
        tmin, tmax = self.month_ranges[dt.month]
       
        lapse = -0.0065 * self.elevation
        return tmin + lapse, tmax + lapse

    def _simulate_temperature_base(self, now_utc):
       
        local = now_utc + timedelta(hours=LOCAL_TZ_OFFSET_HOURS)
        tmin, tmax = self._month_bounds(local)
        avg = (tmin + tmax) / 2.0
        amp = (tmax - tmin) / 2.0

        hour = local.hour + local.minute / 60.0
      
        daily = amp * math.sin((hour - 15) * math.pi / 12)

      
        drift = random.uniform(-0.25, 0.25)
        noise = random.uniform(-0.6, 0.6)

        temp = avg + daily + drift + noise
        return max(tmin, min(tmax, temp))

    def _calibrate_temperature(self, model_temp):
       
        temp = (model_temp + CALIB_TEMP_BIAS) * CALIB_TEMP_GAIN

    
        if REAL_TEMP_HINT:
            try:
                hint = float(REAL_TEMP_HINT)
              
                error = hint - temp
                self._ema_bias = max(-EMA_MAX_ABS, min(EMA_MAX_ABS, self._ema_bias + EMA_ALPHA * error))
                temp = temp + self._ema_bias
            except ValueError:
                pass

        return round(temp, 1)

    def _simulate_humidity(self, temperature):
        base = 70 - (temperature - 20) * 1.2
        if self.weather in ['rainy', 'foggy']:
            base += 10
        return round(max(0.0, min(100.0, base + random.uniform(-5, 5))), 1)

   
    def generate_data(self):
        self._simulate_weather()
        now = datetime.now(timezone.utc)

        model_temp = self._simulate_temperature_base(now)
        temperature = self._calibrate_temperature(model_temp)
        humidity = self._simulate_humidity(temperature)

        pm25 = self.state['pm25'] + random.uniform(-5, 5)
        pm10 = self.state['pm10'] + random.uniform(-10, 10)
        co2  = self.state['co2']  + random.uniform(-5, 10)

        data = {
            "sensor_id": self.fake.uuid4(),
            "location": self.location,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timestamp": now.isoformat(), 
            "temperature": float(temperature),
            "humidity": float(humidity),
            "pm25": round(pm25, 1),
            "pm10": round(pm10, 1),
            "co2": round(co2, 1),
            "weather": self.weather,
            "wind_speed": float(self.wind_speed),
            "wind_direction": int(self.wind_direction),
            "events": list(self.events),
        }

        data = self._apply_event_effects(data)

        for k in self.history:
            self.history[k].append(data[k])
        self.state.update({k: data[k] for k in ["pm25", "pm10", "co2", "temperature", "humidity"]})
        return data

  
    def run(self, interval=5):
        log.info(f"Running in {self.location} (TZ offset {LOCAL_TZ_OFFSET_HOURS}h) …")
        if REAL_TEMP_HINT:
            log.info(f"REAL_TEMP_HINT active = {REAL_TEMP_HINT}°C, EMAα={EMA_ALPHA}, clamp=±{EMA_MAX_ABS}°C")
        while True:
            try:
                payload = self.generate_data()
                self.producer.send(KAFKA_TOPIC, value=payload)
                log.info(f"Sent -> {payload}")
                time.sleep(interval)
            except KeyboardInterrupt:
                self.running = False
                log.info("Sensor stopped by user")
                break
            except Exception as e:
                log.error(f"Send error: {e}")
                time.sleep(interval)


if __name__ == "__main__":
    config = {
        'city': 'Pristina',
        'latitude': 42.6629,
        'longitude': 21.1655,
        'elevation': 652,
        'industrial_zones': [
            {'lat': 42.6800, 'lng': 21.1800, 'type': 'manufacturing'},
            {'lat': 42.6500, 'lng': 21.1500, 'type': 'power_plant'},
            {'lat': 42.6700, 'lng': 21.1700, 'type': 'chemical'}
        ]
    }
    SmartAirQualitySensor(config).run(interval=5)
