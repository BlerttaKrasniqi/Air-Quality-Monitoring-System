import time
import json
import random
import math
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
import threading
from collections import deque
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SmartAirQualitySensor:
    def __init__(self, location_config=None):
        self.fake = Faker()
        Faker.seed(12345)
        
        # Location configuration
        self.location_config = location_config or self._generate_location_config()
        self.location = self.location_config['city']
        self.latitude = self.location_config['latitude']
        self.longitude = self.location_config['longitude']
        self.elevation = self.location_config['elevation']
        self.population_density = self.location_config['population_density']
        self.industrial_zones = self.location_config['industrial_zones']
        
        # Weather and environmental state
        self.current_weather = 'sunny'
        self.weather_conditions = ['sunny', 'cloudy', 'rainy', 'windy', 'foggy', 'stormy']
        self.wind_direction = 0  # degrees
        self.wind_speed = 0  # km/h
        self.pressure = 1013.25  # hPa
        self.visibility = 10.0  # km
        
        # Traffic and industrial patterns
        self.traffic_conditions = ['low', 'moderate', 'high', 'very_high', 'extreme']
        self.industrial_activity = 0.5  # 0-1 scale
        
        # Sensor state with realistic drift
        self.sensor_drift = {
            'pm25': 0.0,
            'pm10': 0.0,
            'co2': 0.0,
            'temperature': 0.0,
            'humidity': 0.0
        }
        
        # Historical data for trend analysis
        self.history = {
            'pm25': deque(maxlen=100),
            'pm10': deque(maxlen=100),
            'co2': deque(maxlen=100),
            'temperature': deque(maxlen=100),
            'humidity': deque(maxlen=100)
        }
        

        
        # Event simulation
        self.active_events = []
        self.event_probabilities = {
            'traffic_jam': 0.05,
            'industrial_spill': 0.02,
            'wildfire': 0.01,
            'construction': 0.03,
            'weather_event': 0.08
        }
        
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Time tracking
        self.start_time = datetime.utcnow()
        self.weather_cycle = 0
        self.season = self._get_current_season()
        
        # Initialize realistic baseline values (moved after season is set)
        self.state = {
            "pm25": self._get_baseline_pm25(),
            "pm10": self._get_baseline_pm10(),
            "co2": self._get_baseline_co2(),
            "humidity": self._get_baseline_humidity(),
            "temperature": self._get_baseline_temperature()
        }
        
        # Start background event simulation
        self.event_thread = threading.Thread(target=self._event_simulation_loop, daemon=True)
        self.event_thread.start()
        
        logger.info(f"🌬️ Smart Air Quality Sensor initialized in {self.location}")
        logger.info(f"📍 Coordinates: {self.latitude}, {self.longitude}")
        logger.info(f"🏭 Industrial zones: {len(self.industrial_zones)}")
    
    def _generate_location_config(self):
        """Generate realistic location configuration for Kosovo."""
        locations = [
            {
                'city': 'Pristina',
                'latitude': 42.6629,
                'longitude': 21.1655,
                'elevation': 652,
                'population_density': 'high',
                'industrial_zones': [
                    {'lat': 42.6500, 'lng': 21.1500, 'type': 'manufacturing'},
                    {'lat': 42.6700, 'lng': 21.1800, 'type': 'power_plant'},
                    {'lat': 42.6400, 'lng': 21.1600, 'type': 'construction'}
                ]
            },
            {
                'city': 'Prizren',
                'latitude': 42.2139,
                'longitude': 20.7394,
                'elevation': 450,
                'population_density': 'moderate',
                'industrial_zones': [
                    {'lat': 42.2000, 'lng': 20.7500, 'type': 'manufacturing'},
                    {'lat': 42.2200, 'lng': 20.7300, 'type': 'textile'},
                    {'lat': 42.2100, 'lng': 20.7400, 'type': 'food_processing'}
                ]
            },
            {
                'city': 'Peja',
                'latitude': 42.6598,
                'longitude': 20.2883,
                'elevation': 550,
                'population_density': 'moderate',
                'industrial_zones': [
                    {'lat': 42.6500, 'lng': 20.3000, 'type': 'manufacturing'},
                    {'lat': 42.6700, 'lng': 20.2800, 'type': 'wood_processing'},
                    {'lat': 42.6600, 'lng': 20.2900, 'type': 'construction'}
                ]
            },
            {
                'city': 'Gjilan',
                'latitude': 42.4634,
                'longitude': 21.4694,
                'elevation': 508,
                'population_density': 'moderate',
                'industrial_zones': [
                    {'lat': 42.4600, 'lng': 21.4800, 'type': 'manufacturing'},
                    {'lat': 42.4700, 'lng': 21.4600, 'type': 'agricultural'},
                    {'lat': 42.4650, 'lng': 21.4700, 'type': 'food_processing'}
                ]
            },
            {
                'city': 'Mitrovica',
                'latitude': 42.8833,
                'longitude': 20.8667,
                'elevation': 500,
                'population_density': 'high',
                'industrial_zones': [
                    {'lat': 42.8800, 'lng': 20.8700, 'type': 'mining'},
                    {'lat': 42.8900, 'lng': 20.8600, 'type': 'manufacturing'},
                    {'lat': 42.8850, 'lng': 20.8650, 'type': 'power_plant'}
                ]
            }
        ]
        return random.choice(locations)
    
    def _get_current_season(self):
        """Get current season based on date."""
        month = datetime.utcnow().month
        if month in [12, 1, 2]:
            return 'winter'
        elif month in [3, 4, 5]:
            return 'spring'
        elif month in [6, 7, 8]:
            return 'summer'
        else:
            return 'autumn'
    
    def _get_baseline_pm25(self):
        """Get baseline PM2.5 based on location and season for Kosovo."""
        base_values = {
            'Pristina': {'winter': 35, 'spring': 28, 'summer': 22, 'autumn': 32},
            'Prizren': {'winter': 30, 'spring': 25, 'summer': 20, 'autumn': 28},
            'Peja': {'winter': 28, 'spring': 22, 'summer': 18, 'autumn': 25},
            'Gjilan': {'winter': 32, 'spring': 26, 'summer': 21, 'autumn': 30},
            'Mitrovica': {'winter': 38, 'spring': 30, 'summer': 25, 'autumn': 35}
        }
        return base_values.get(self.location, {'winter': 30, 'spring': 25, 'summer': 20, 'autumn': 28})[self.season]
    
    def _get_baseline_pm10(self):
        """Get baseline PM10 (usually 1.5-2x PM2.5)."""
        return self._get_baseline_pm25() * random.uniform(1.5, 2.0)
    
    def _get_baseline_co2(self):
        """Get baseline CO2 based on location."""
        base_co2 = {
            'New York': 420,
            'Los Angeles': 425,
            'Chicago': 418,
            'Seattle': 410
        }
        return base_co2.get(self.location, 415)
    
    def _get_baseline_humidity(self):
        """Get baseline humidity based on location and season for Kosovo."""
        base_humidity = {
            'Pristina': {'winter': 75, 'spring': 65, 'summer': 60, 'autumn': 70},
            'Prizren': {'winter': 70, 'spring': 60, 'summer': 55, 'autumn': 65},
            'Peja': {'winter': 80, 'spring': 70, 'summer': 65, 'autumn': 75},
            'Gjilan': {'winter': 72, 'spring': 62, 'summer': 58, 'autumn': 68},
            'Mitrovica': {'winter': 78, 'spring': 68, 'summer': 62, 'autumn': 72}
        }
        return base_humidity.get(self.location, {'winter': 70, 'spring': 60, 'summer': 55, 'autumn': 65})[self.season]
    
    def _get_baseline_temperature(self):
        """Get baseline temperature based on location and season for Kosovo."""
        base_temp = {
            'Pristina': {'winter': 2, 'spring': 15, 'summer': 25, 'autumn': 12},
            'Prizren': {'winter': 4, 'spring': 16, 'summer': 27, 'autumn': 14},
            'Peja': {'winter': 1, 'spring': 14, 'summer': 24, 'autumn': 11},
            'Gjilan': {'winter': 3, 'spring': 15, 'summer': 26, 'autumn': 13},
            'Mitrovica': {'winter': 0, 'spring': 13, 'summer': 23, 'autumn': 10}
        }
        return base_temp.get(self.location, {'winter': 2, 'spring': 15, 'summer': 25, 'autumn': 12})[self.season]
    
    def _update_weather_conditions(self):
        """Update weather conditions with realistic patterns."""
        now = datetime.utcnow()
        hour = now.hour
        
        # Weather changes based on time and season
        if self.weather_cycle % 50 == 0:  # Change weather every ~4 hours
            weather_weights = {
                'sunny': 0.3,
                'cloudy': 0.25,
                'rainy': 0.2,
                'windy': 0.15,
                'foggy': 0.08,
                'stormy': 0.02
            }
            
            # Adjust weights based on season
            if self.season == 'winter':
                weather_weights['rainy'] += 0.1
                weather_weights['foggy'] += 0.05
            elif self.season == 'summer':
                weather_weights['sunny'] += 0.1
                weather_weights['stormy'] += 0.05
            
            self.current_weather = random.choices(
                list(weather_weights.keys()),
                weights=list(weather_weights.values())
            )[0]
        
        # Update wind conditions
        self.wind_direction = (self.wind_direction + random.uniform(-10, 10)) % 360
        self.wind_speed = max(0, min(50, self.wind_speed + random.uniform(-2, 2)))
        
        # Update pressure and visibility
        self.pressure += random.uniform(-2, 2)
        self.visibility = max(0.1, min(20, self.visibility + random.uniform(-0.5, 0.5)))
    
    def _simulate_traffic_patterns(self, hour):
        """Simulate realistic traffic patterns."""
        # Base traffic by hour
        traffic_patterns = {
            0: 0.1, 1: 0.05, 2: 0.02, 3: 0.02, 4: 0.05, 5: 0.2,
            6: 0.4, 7: 0.8, 8: 0.9, 9: 0.7, 10: 0.5, 11: 0.6,
            12: 0.7, 13: 0.6, 14: 0.5, 15: 0.6, 16: 0.8, 17: 0.9,
            18: 0.8, 19: 0.6, 20: 0.4, 21: 0.3, 22: 0.2, 23: 0.15
        }
        
        base_traffic = traffic_patterns.get(hour, 0.5)
        
        # Adjust for day of week
        day_of_week = datetime.utcnow().weekday()
        if day_of_week >= 5:  # Weekend
            base_traffic *= 0.7
        
        # Add random variation
        traffic_factor = base_traffic + random.uniform(-0.1, 0.1)
        
        # Map to traffic conditions
        if traffic_factor < 0.2:
            return 'low'
        elif traffic_factor < 0.5:
            return 'moderate'
        elif traffic_factor < 0.8:
            return 'high'
        elif traffic_factor < 0.95:
            return 'very_high'
        else:
            return 'extreme'
    
    def _simulate_industrial_activity(self, hour):
        """Simulate industrial activity patterns."""
        # Industrial activity is higher during work hours
        if 6 <= hour < 18:
            base_activity = 0.7
        elif 18 <= hour < 22:
            base_activity = 0.4
        else:
            base_activity = 0.2
        
        # Add random variation
        self.industrial_activity = max(0, min(1, base_activity + random.uniform(-0.1, 0.1)))
        return self.industrial_activity
    
    def _calculate_pollution_contribution(self, source_type, distance, wind_factor):
        """Calculate pollution contribution from industrial sources."""
        # Inverse square law for pollution dispersion
        base_contribution = 1 / (distance ** 2)
        
        # Wind factor affects dispersion
        wind_effect = 1 + (wind_factor - 0.5) * 0.5
        
        # Source type multipliers
        source_multipliers = {
            'power_plant': 2.0,
            'refinery': 3.0,
            'chemical': 2.5,
            'steel_mill': 2.8,
            'manufacturing': 1.5,
            'shipyard': 1.8
        }
        
        multiplier = source_multipliers.get(source_type, 1.0)
        return base_contribution * wind_effect * multiplier
    
    def _simulate_temperature(self):
        """Simulate realistic temperature with seasonal and daily patterns."""
        now = datetime.utcnow()
        hour = now.hour + now.minute / 60
        
        # Seasonal base temperature
        seasonal_temp = self._get_baseline_temperature()
        
        # Daily temperature cycle (reduced for more realistic Kosovo temperatures)
        if 6 <= hour < 12:  # Morning warming
            daily_cycle = (hour - 6) * 1.0
        elif 12 <= hour < 18:  # Afternoon peak
            daily_cycle = 6 + (hour - 12) * 0.1
        elif 18 <= hour < 22:  # Evening cooling
            daily_cycle = 6 - (hour - 18) * 1.2
        else:  # Night cooling
            daily_cycle = 0 - (hour - 22) * 0.6 if hour >= 22 else 0 - (24 - hour) * 0.6
        
        # Weather effects
        weather_effects = {
            'sunny': random.uniform(2, 4),
            'cloudy': random.uniform(-1, 1),
            'rainy': random.uniform(-3, -1),
            'windy': random.uniform(-2, 2),
            'foggy': random.uniform(-1, 1),
            'stormy': random.uniform(-4, -2)
        }
        
        weather_effect = weather_effects.get(self.current_weather, 0)
        
        # Calculate final temperature
        temperature = seasonal_temp + daily_cycle + weather_effect + random.uniform(-1, 1)
        
        # Add sensor drift
        temperature += self.sensor_drift['temperature']
        
        return max(-20, min(50, round(temperature, 1)))
    
    def _simulate_pm_levels(self, pm_type="pm25"):
        """Simulate PM levels with advanced modeling."""
        now = datetime.utcnow()
        hour = now.hour
        
        # Get current conditions
        traffic = self._simulate_traffic_patterns(hour)
        industrial_activity = self._simulate_industrial_activity(hour)
        
        # Base PM values
        if pm_type == "pm25":
            base_pm = self._get_baseline_pm25()
        else:  # PM10
            base_pm = self._get_baseline_pm10()
        
        # Traffic multipliers
        traffic_multipliers = {
            'low': 0.7,
            'moderate': 1.0,
            'high': 1.5,
            'very_high': 2.2,
            'extreme': 3.0
        }
        
        # Weather effects
        weather_effects = {
            'sunny': 1.1,
            'cloudy': 1.0,
            'rainy': 0.6,  # Rain cleans the air
            'windy': 0.8,  # Wind disperses particles
            'foggy': 1.3,  # Fog traps particles
            'stormy': 0.5  # Storms clean the air
        }
        
        # Industrial contribution
        industrial_contribution = 0
        for zone in self.industrial_zones:
            # Calculate distance from sensor to industrial zone
            distance = math.sqrt(
                (self.latitude - zone['lat'])**2 + 
                (self.longitude - zone['lng'])**2
            ) * 111  # Convert to km
            
            if distance < 50:  # Only consider nearby sources
                wind_factor = self.wind_speed / 50.0  # Normalize wind speed
                contribution = self._calculate_pollution_contribution(
                    zone['type'], distance, wind_factor
                )
                industrial_contribution += contribution * industrial_activity
        
        # Calculate final PM value
        traffic_factor = traffic_multipliers.get(traffic, 1.0)
        weather_factor = weather_effects.get(self.current_weather, 1.0)
        
        final_pm = base_pm * traffic_factor * weather_factor + industrial_contribution * 10
        
        # Add random variation
        final_pm += random.uniform(-2, 2)
        
        # Add sensor drift
        final_pm += self.sensor_drift[pm_type]
        
        # Ensure realistic ranges
        if pm_type == "pm25":
            return max(5, min(150, round(final_pm, 1)))
        else:
            return max(10, min(300, round(final_pm, 1)))
    
    def _simulate_co2(self):
        """Simulate CO2 levels with advanced modeling."""
        now = datetime.utcnow()
        hour = now.hour
        
        # Base outdoor CO2
        base_co2 = self._get_baseline_co2()
        
        # Traffic contribution
        traffic = self._simulate_traffic_patterns(hour)
        traffic_contribution = {
            'low': 0,
            'moderate': 5,
            'high': 15,
            'very_high': 25,
            'extreme': 40
        }.get(traffic, 0)
        
        # Industrial contribution
        industrial_contribution = self.industrial_activity * 20
        
        # Weather effects
        weather_co2_effects = {
            'sunny': 0,
            'cloudy': 2,
            'rainy': -5,
            'windy': -3,
            'foggy': 5,
            'stormy': -8
        }
        
        weather_effect = weather_co2_effects.get(self.current_weather, 0)
        
        # Calculate final CO2
        final_co2 = base_co2 + traffic_contribution + industrial_contribution + weather_effect
        final_co2 += random.uniform(-3, 3)  # Random variation
        final_co2 += self.sensor_drift['co2']  # Sensor drift
        
        return max(400, min(600, round(final_co2, 1)))
    
    def _simulate_humidity(self, temperature):
        """Simulate humidity with advanced modeling."""
        now = datetime.utcnow()
        hour = now.hour
        
        # Base humidity (inversely related to temperature)
        base_humidity = 70 - (temperature - 20) * 2
        
        # Time-based patterns (higher at night)
        if 22 <= hour or hour < 6:
            time_factor = random.uniform(5, 15)
        else:
            time_factor = random.uniform(-10, 5)
        
        # Weather effects
        weather_humidity_effects = {
            'sunny': -5,
            'cloudy': 0,
            'rainy': 15,
            'windy': -3,
            'foggy': 10,
            'stormy': 20
        }
        
        weather_effect = weather_humidity_effects.get(self.current_weather, 0)
        
        # Calculate final humidity
        final_humidity = base_humidity + time_factor + weather_effect
        final_humidity += random.uniform(-2, 2)  # Random variation
        final_humidity += self.sensor_drift['humidity']  # Sensor drift
        
        return max(20, min(95, round(final_humidity, 1)))
    
    def _update_sensor_drift(self):
        """Simulate sensor drift over time."""
        for sensor_type in self.sensor_drift:
            # Gradual drift
            self.sensor_drift[sensor_type] += random.uniform(-0.1, 0.1)
            
            # Keep drift within reasonable bounds
            self.sensor_drift[sensor_type] = max(-5, min(5, self.sensor_drift[sensor_type]))
    
    def _event_simulation_loop(self):
        """Background thread for simulating random events."""
        while True:
            try:
                # Check for new events
                for event_type, probability in self.event_probabilities.items():
                    if random.random() < probability:
                        self._trigger_event(event_type)
                
                time.sleep(300)  # Check every 5 minutes
            except Exception as e:
                logger.error(f"Error in event simulation: {e}")
    
    def _trigger_event(self, event_type):
        """Trigger a specific air quality event."""
        event_duration = random.randint(300, 1800)  # 5-30 minutes
        event_data = {
            'type': event_type,
            'start_time': datetime.utcnow(),
            'duration': event_duration,
            'intensity': random.uniform(0.5, 1.5)
        }
        
        self.active_events.append(event_data)
        logger.info(f"🚨 Event triggered: {event_type} for {event_duration} seconds")
    
    def _apply_event_effects(self, data):
        """Apply effects from active events to sensor data."""
        current_time = datetime.utcnow()
        
        for event in self.active_events[:]:  # Copy list to avoid modification during iteration
            # Check if event has expired
            if (current_time - event['start_time']).total_seconds() > event['duration']:
                self.active_events.remove(event)
                continue
            
            # Apply event effects
            intensity = event['intensity']
            
            if event['type'] == 'traffic_jam':
                data['pm25'] *= (1 + intensity * 0.5)
                data['pm10'] *= (1 + intensity * 0.6)
                data['co2'] += intensity * 20
            elif event['type'] == 'industrial_spill':
                data['pm25'] *= (1 + intensity * 0.8)
                data['pm10'] *= (1 + intensity * 1.0)
                data['co2'] += intensity * 30
            elif event['type'] == 'wildfire':
                data['pm25'] *= (1 + intensity * 2.0)
                data['pm10'] *= (1 + intensity * 2.5)
                data['co2'] += intensity * 50
            elif event['type'] == 'construction':
                data['pm25'] *= (1 + intensity * 0.3)
                data['pm10'] *= (1 + intensity * 0.4)
            elif event['type'] == 'weather_event':
                if self.current_weather == 'stormy':
                    data['pm25'] *= 0.7
                    data['pm10'] *= 0.6
                elif self.current_weather == 'foggy':
                    data['pm25'] *= 1.2
                    data['pm10'] *= 1.3
        
        return data
    
    def _predict_trends(self):
        """Predict air quality trends using simple moving averages."""
        predictions = {}
        
        for param in self.history:
            if len(self.history[param]) >= 10:
                recent_values = list(self.history[param])[-10:]
                trend = np.polyfit(range(len(recent_values)), recent_values, 1)[0]
                predictions[param] = trend
        
        return predictions
    
    def generate_smart_data(self):
        """Generate intelligent air quality data."""
        # Update environmental conditions
        self._update_weather_conditions()
        self._update_sensor_drift()
        
        # Generate base measurements
        temperature = self._simulate_temperature()
        pm25 = self._simulate_pm_levels("pm25")
        pm10 = self._simulate_pm_levels("pm10")
        co2 = self._simulate_co2()
        humidity = self._simulate_humidity(temperature)
        
        # Create data object
        data = {
            "location": self.location,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "weather": self.current_weather,
            "wind_direction": round(self.wind_direction, 1),
            "wind_speed": round(self.wind_speed, 1),
            "pressure": round(self.pressure, 1),
            "visibility": round(self.visibility, 1),
            "pm25": pm25,
            "pm10": pm10,
            "co2": co2,
            "temperature": temperature,
            "humidity": humidity,
            "timestamp": datetime.utcnow().isoformat(),
            "sensor_id": self.fake.uuid4(),
            "air_quality_index": self._calculate_aqi(pm25, pm10),
            "season": self.season,
            "active_events": [event['type'] for event in self.active_events],
            "industrial_activity": round(self.industrial_activity, 2),
            "sensor_drift": self.sensor_drift.copy()
        }
        
        # Apply event effects
        data = self._apply_event_effects(data)
        
        # Update history
        for param in ['pm25', 'pm10', 'co2', 'temperature', 'humidity']:
            self.history[param].append(data[param])
        
        # Add trend predictions
        trends = self._predict_trends()
        data['trends'] = trends
        
        # Update state
        self.state.update({
            "pm25": data['pm25'],
            "pm10": data['pm10'],
            "co2": data['co2'],
            "temperature": data['temperature'],
            "humidity": data['humidity']
        })
        
        return data
    
    def _calculate_aqi(self, pm25, pm10):
        """Calculate comprehensive Air Quality Index."""
        # EPA AQI calculation
        if pm25 <= 12 and pm10 <= 54:
            return "Good"
        elif pm25 <= 35.4 and pm10 <= 154:
            return "Moderate"
        elif pm25 <= 55.4 and pm10 <= 254:
            return "Unhealthy for Sensitive Groups"
        elif pm25 <= 150.4 and pm10 <= 354:
            return "Unhealthy"
        elif pm25 <= 250.4 and pm10 <= 424:
            return "Very Unhealthy"
        else:
            return "Hazardous"
    
    def simulate_event(self, event_type):
        """Manually trigger an event."""
        self._trigger_event(event_type)
        return self.generate_smart_data()
    
    def get_sensor_status(self):
        """Get sensor status and health information."""
        return {
            "location": self.location,
            "uptime": (datetime.utcnow() - self.start_time).total_seconds(),
            "sensor_drift": self.sensor_drift,
            "active_events": len(self.active_events),
            "weather_cycle": self.weather_cycle,
            "data_points_generated": sum(len(history) for history in self.history.values())
        }

# Global smart sensor instance with Kosovo location
kosovo_config = {
    'city': 'Pristina',
    'latitude': 42.6629,
    'longitude': 21.1655,
    'elevation': 652,
    'population_density': 'high',
    'industrial_zones': [
        {'lat': 42.6800, 'lng': 21.1800, 'type': 'manufacturing'},
        {'lat': 42.6500, 'lng': 21.1500, 'type': 'power_plant'},
        {'lat': 42.6700, 'lng': 21.1700, 'type': 'chemical'}
    ]
}
smart_sensor = SmartAirQualitySensor(kosovo_config)

if __name__ == "__main__":
    print(f"🌬️ Smart Air Quality Sensor running in {smart_sensor.location}...")
    print(f"📍 Coordinates: {smart_sensor.latitude}, {smart_sensor.longitude}")
    print(f"🏭 Industrial zones: {len(smart_sensor.industrial_zones)}")
    print(f"🌤️ Current season: {smart_sensor.season}")
    
    while True:
        try:
            data = smart_sensor.generate_smart_data()
            smart_sensor.producer.send('air_quality', value=data)
            
            # Print formatted output
            events_str = f" | Events: {', '.join(data['active_events'])}" if data['active_events'] else ""
            print(f"📊 {data['weather'].upper()} | PM2.5: {data['pm25']} | PM10: {data['pm10']} | CO2: {data['co2']} | Temp: {data['temperature']}°C | Humidity: {data['humidity']}% | AQI: {data['air_quality_index']}{events_str}")
            
            smart_sensor.weather_cycle += 1
            time.sleep(5)
            
        except KeyboardInterrupt:
            print("\n🛑 Sensor simulation stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in sensor simulation: {e}")
            time.sleep(5) 