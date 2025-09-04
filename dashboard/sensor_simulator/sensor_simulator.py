import time
import json
import random
import math
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
from threading import Thread
from collections import deque

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SmartAirQualitySensor:
    def __init__(self, config):
        self.fake = Faker()
        Faker.seed(42)

        self.location = config['city']
        self.latitude = config['latitude']
        self.longitude = config['longitude']
        self.elevation = config['elevation']
        self.industrial_zones = config['industrial_zones']

        self.season = self._get_current_season()
        self.weather = 'sunny'
        self.wind_speed = 5.0
        self.wind_direction = 0
        self.weather_trend = 0  # gradual weather change

        # Seasonal realistic ranges for Pristina (in °C)
        self.season_ranges = {
            'winter': (-3, 7),
            'spring': (5, 18),
            'summer': (15, 30),
            'autumn': (7, 20)
        }

        self.state = {
            "pm25": 30.0,
            "pm10": 60.0,
            "co2": 410.0,
            "temperature": 20.0,
            "humidity": 65.0
        }

        self.history = {key: deque(maxlen=100) for key in self.state.keys()}

        self.producer = KafkaProducer(
            bootstrap_servers=['127.0.0.1:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.events = []
        self.running = True

        Thread(target=self._event_loop, daemon=True).start()

    def _get_current_season(self):
        month = datetime.utcnow().month
        if month in [12, 1, 2]:
            return 'winter'
        elif month in [3, 4, 5]:
            return 'spring'
        elif month in [6, 7, 8]:
            return 'summer'
        return 'autumn'

    def _simulate_weather(self):
        weather_options = ['sunny', 'cloudy', 'rainy', 'windy', 'foggy']
        # Gradual weather changes
        self.weather_trend += random.uniform(-0.2, 0.2)
        idx = int((weather_options.index(self.weather) + round(self.weather_trend)) % len(weather_options))
        self.weather = weather_options[idx]
        self.wind_speed = round(max(0, self.wind_speed + random.uniform(-1, 1)), 1)
        self.wind_direction = (self.wind_direction + random.randint(-10, 10)) % 360

    def _apply_event_effects(self, data):
        for event in self.events:
            impact_factor = 1.0
            if event == 'traffic_jam':
                impact_factor = 1.5
                data['co2'] += 15
            elif event == 'industrial_spill':
                impact_factor = 2.0
                data['co2'] += 25
            elif event == 'wildfire':
                impact_factor = 3.0
                data['co2'] += 40
            data['pm25'] *= impact_factor
            data['pm10'] *= impact_factor
        return data

    def _event_loop(self):
        event_types = ['traffic_jam', 'industrial_spill', 'wildfire']
        while self.running:
            if random.random() < 0.05:
                event = random.choice(event_types)
                self.events.append(event)
                logger.info(f"Event triggered: {event}")
                time.sleep(random.randint(10, 30))  # event duration
                self.events.remove(event)
                logger.info(f"Event resolved: {event}")
            time.sleep(5)

    def _simulate_temperature(self):
        now = datetime.utcnow()
        min_temp, max_temp = self.season_ranges[self.season]
        avg_temp = (min_temp + max_temp) / 2
        amplitude = (max_temp - min_temp) / 2

        # Daily cycle (sine) + small multi-day trend + random noise
        daily = amplitude * math.sin((now.hour / 24.0) * 2 * math.pi)
        trend = random.uniform(-0.5, 0.5)
        noise = random.uniform(-1.0, 1.0)

        temperature = avg_temp + daily + trend + noise

        # Clamp to realistic min/max
        temperature = max(min_temp, min(max_temp, temperature))

        # Round to 1 decimal place
        return round(temperature, 1)


    def _simulate_humidity(self, temperature):
        # Humidity inversely correlated with temperature and influenced by weather
        base = 70 - (temperature - 20) * 1.2
        if self.weather in ['rainy', 'foggy']:
            base += 10
        humidity = base + random.uniform(-5, 5)
        return round(max(0, min(100, humidity)), 1)

    def generate_data(self):
        self._simulate_weather()
        now = datetime.utcnow()

        temperature = self._simulate_temperature()
        humidity = self._simulate_humidity(temperature)
        pm25 = self.state['pm25'] + random.uniform(-5, 5)
        pm10 = self.state['pm10'] + random.uniform(-10, 10)
        co2 = self.state['co2'] + random.uniform(-5, 10)

        data = {
            "sensor_id": self.fake.uuid4(),
            "location": self.location,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timestamp": now.isoformat(),
            "temperature": temperature,
            "humidity": humidity,
            "pm25": round(pm25, 1),
            "pm10": round(pm10, 1),
            "co2": round(co2, 1),
            "weather": self.weather,
            "wind_speed": self.wind_speed,
            "wind_direction": self.wind_direction,
            "events": self.events[:]
        }

        data = self._apply_event_effects(data)

        for k in self.history:
            self.history[k].append(data[k])

        self.state.update({
            "pm25": data['pm25'],
            "pm10": data['pm10'],
            "co2": data['co2'],
            "temperature": data['temperature'],
            "humidity": data['humidity']
        })

        return data

    def run(self, interval=5):
        logger.info(f"Smart sensor running in {self.location} ({self.season})...")
        while True:
            try:
                data = self.generate_data()
                self.producer.send('air_quality', value=data)
                logger.info(f"Sent: {data}")
                time.sleep(interval)
            except KeyboardInterrupt:
                self.running = False
                logger.info("Sensor stopped by user")
                break
            except Exception as e:
                logger.error(f"Error: {e}")
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
    sensor = SmartAirQualitySensor(config)
    sensor.run()
