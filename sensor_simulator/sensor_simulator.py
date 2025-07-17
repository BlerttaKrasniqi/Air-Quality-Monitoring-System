import time
import json
import random
import math
import logging
from datetime import datetime
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

        self.state = {
            "pm25": 30.0,
            "pm10": 60.0,
            "co2": 410.0,
            "temperature": 20.0,
            "humidity": 65.0
        }

        self.history = {key: deque(maxlen=100) for key in self.state.keys()}

        self.producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.events = []
        self.running = True

        Thread(target=self._event_loop, daemon=True).start()

    def _get_current_season(self):
        month = datetime.utcnow().month
        if month in [12, 1, 2]: return 'winter'
        elif month in [3, 4, 5]: return 'spring'
        elif month in [6, 7, 8]: return 'summer'
        return 'autumn'

    def _simulate_weather(self):
        weather_options = ['sunny', 'cloudy', 'rainy', 'windy', 'foggy']
        self.weather = random.choice(weather_options)
        self.wind_speed = round(random.uniform(0, 25), 1)
        self.wind_direction = random.randint(0, 360)

    def _apply_event_effects(self, data):
        for event in self.events:
            if event == 'traffic_jam':
                data['pm25'] *= 1.5
                data['pm10'] *= 1.6
                data['co2'] += 15
            elif event == 'industrial_spill':
                data['pm25'] *= 1.8
                data['pm10'] *= 2.0
                data['co2'] += 25
            elif event == 'wildfire':
                data['pm25'] *= 2.5
                data['pm10'] *= 3.0
                data['co2'] += 40

        return data

    def _event_loop(self):
        event_types = ['traffic_jam', 'industrial_spill', 'wildfire']
        while self.running:
            if random.random() < 0.05:
                event = random.choice(event_types)
                self.events.append(event)
                logger.info(f" Event triggered: {event}")
                time.sleep(random.randint(10, 30))  # event duration
                self.events.remove(event)
                logger.info(f"Event resolved: {event}")
            time.sleep(5)

    def generate_data(self):
        self._simulate_weather()
        now = datetime.utcnow()

        temperature = 12 + 10 * math.sin((now.hour / 24.0) * 2 * math.pi) + random.uniform(-2, 2)
        humidity = 70 - (temperature - 20) * 1.2 + random.uniform(-5, 5)
        pm25 = self.state['pm25'] + random.uniform(-5, 5)
        pm10 = self.state['pm10'] + random.uniform(-10, 10)
        co2 = self.state['co2'] + random.uniform(-5, 10)

        data = {
            "sensor_id": self.fake.uuid4(),
            "location": self.location,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timestamp": now.isoformat(),
            "temperature": round(temperature, 1),
            "humidity": round(humidity, 1),
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
        logger.info(f" Smart sensor running in {self.location}...")
        while True:
            try:
                data = self.generate_data()
                self.producer.send('air_quality', value=data)
                logger.info(f" Sent: {data}")
                time.sleep(interval)
            except KeyboardInterrupt:
                self.running = False
                logger.info(" Sensor stopped by user")
                break
            except Exception as e:
                logger.error(f" Error: {e}")
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
