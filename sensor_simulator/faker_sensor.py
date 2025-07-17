import time
import json
import random
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer


fake = Faker()
Faker.seed(12345)  

class FakerAirQualitySensor:
    def __init__(self):
        self.fake = fake
        self.location = self.fake.city()
        self.weather_conditions = ['sunny', 'cloudy', 'rainy', 'windy', 'foggy']
        self.traffic_conditions = ['low', 'moderate', 'high', 'very_high']
        
        # Initialize realistic baseline values
        self.state = {
            "pm25": self.fake.random_int(min=8, max=35),
            "pm10": self.fake.random_int(min=15, max=60),
            "co2": self.fake.random_int(min=400, max=450),
            "humidity": self.fake.random_int(min=45, max=75),
            "temperature": self.fake.random_int(min=15, max=30)
        }
        
        self.producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
      
        self.start_time = datetime.utcnow()
        self.weather_cycle = 0
        
    def get_weather_condition(self):
        """Get current weather condition affecting air quality."""
        # Change weather every 2-4 hours
        if self.weather_cycle % 100 == 0:
            return self.fake.random_element(self.weather_conditions)
        return self.current_weather
    
    def get_traffic_condition(self, hour):
        """Get traffic condition based on time of day."""
        if (7 <= hour < 9) or (17 <= hour < 19):  # Rush hours
            return self.fake.random_element(['high', 'very_high'])
        elif (9 <= hour < 17):  # Daytime
            return self.fake.random_element(['moderate', 'high'])
        else:  # Night time
            return self.fake.random_element(['low', 'moderate'])
    
    def simulate_temperature(self):
        """Simulate realistic temperature with Faker."""
        now = datetime.utcnow()
        hour = now.hour
        
        
        if 6 <= hour < 12:  # Morning
            base_temp = 18 + (hour - 6) * 1.5
        elif 12 <= hour < 18:  # Afternoon
            base_temp = 25 + (hour - 12) * 0.5
        elif 18 <= hour < 22:  # Evening
            base_temp = 25 - (hour - 18) * 1.2
        else:  # Night
            base_temp = 15 - (hour - 22) * 0.5 if hour >= 22 else 15 - (24 - hour) * 0.5
        
        # Add weather-based variation
        weather_variation = {
            'sunny': random.uniform(2, 4),
            'cloudy': random.uniform(-1, 1),
            'rainy': random.uniform(-3, -1),
            'windy': random.uniform(-2, 2),
            'foggy': random.uniform(-1, 1)
        }
        
        variation = weather_variation.get(self.current_weather, 0)
        return round(base_temp + variation + self.fake.random.uniform(-1, 1), 1)
    
    def simulate_pm_levels(self, pm_type="pm25"):
        """Simulate PM levels using Faker with realistic patterns."""
        now = datetime.utcnow()
        hour = now.hour
        
        # Get current conditions
        traffic = self.get_traffic_condition(hour)
        weather = self.current_weather
        
        # Base PM values
        if pm_type == "pm25":
            base_pm = self.fake.random_int(min=10, max=40)
        else:  # PM10
            base_pm = self.fake.random_int(min=20, max=80)
        
        # Traffic multipliers
        traffic_multipliers = {
            'low': 0.7,
            'moderate': 1.0,
            'high': 1.5,
            'very_high': 2.2
        }
        
        # Weather effects
        weather_effects = {
            'sunny': 1.1,
            'cloudy': 1.0,
            'rainy': 0.6,  # Rain cleans the air
            'windy': 0.8,  # Wind disperses particles
            'foggy': 1.3   # Fog traps particles
        }
        
        # Calculate final PM value
        traffic_factor = traffic_multipliers.get(traffic, 1.0)
        weather_factor = weather_effects.get(weather, 1.0)
        
        final_pm = base_pm * traffic_factor * weather_factor
        
        # Add some random variation
        final_pm += self.fake.random.uniform(-2, 2)
        
        # Ensure realistic ranges
        if pm_type == "pm25":
            return max(5, min(100, round(final_pm, 1)))
        else:
            return max(10, min(200, round(final_pm, 1)))
    
    def simulate_co2(self):
        """Simulate CO2 levels using Faker."""
        now = datetime.utcnow()
        hour = now.hour
        
        # Base outdoor CO2 (400-420 ppm is normal)
        base_co2 = self.fake.random_int(min=400, max=420)
        
        # Time-based variations
        if (7 <= hour < 9) or (17 <= hour < 19):  # Rush hours
            variation = self.fake.random_int(min=5, max=20)
        elif 8 <= hour < 18:  # Daytime
            variation = self.fake.random_int(min=-5, max=10)
        else:  # Night time
            variation = self.fake.random_int(min=-10, max=5)
        
        # Weather effects on CO2
        weather_co2_effects = {
            'sunny': 0,
            'cloudy': 2,
            'rainy': -5,
            'windy': -3,
            'foggy': 5
        }
        
        weather_effect = weather_co2_effects.get(self.current_weather, 0)
        final_co2 = base_co2 + variation + weather_effect
        
        return max(400, min(450, round(final_co2, 1)))
    
    def simulate_humidity(self, temperature):
        """Simulate humidity using Faker."""
        now = datetime.utcnow()
        hour = now.hour
        
        # Base humidity (inversely related to temperature)
        base_humidity = 70 - (temperature - 20) * 2
        
        # Time-based patterns (higher at night)
        if 22 <= hour or hour < 6:
            time_factor = self.fake.random_int(min=5, max=15)
        else:
            time_factor = self.fake.random_int(min=-10, max=5)
        
        # Weather effects
        weather_humidity_effects = {
            'sunny': -5,
            'cloudy': 0,
            'rainy': 15,
            'windy': -3,
            'foggy': 10
        }
        
        weather_effect = weather_humidity_effects.get(self.current_weather, 0)
        final_humidity = base_humidity + time_factor + weather_effect
        
        return max(30, min(90, round(final_humidity, 1)))
    
    def generate_realistic_data(self):
        """Generate realistic air quality data using Faker."""
        # Update weather condition
        self.current_weather = self.get_weather_condition()
        
        # Generate temperature first (affects other parameters)
        temperature = self.simulate_temperature()
        
        # Generate other parameters
        pm25 = self.simulate_pm_levels("pm25")
        pm10 = self.simulate_pm_levels("pm10")
        co2 = self.simulate_co2()
        humidity = self.simulate_humidity(temperature)
        
        # Create realistic timestamp
        timestamp = self.fake.date_time_between(
            start_date='-1h',
            end_date='now'
        ).isoformat()
        
        # Update state
        self.state.update({
            "pm25": pm25,
            "pm10": pm10,
            "co2": co2,
            "temperature": temperature,
            "humidity": humidity
        })
        
        return {
            "location": self.location,
            "weather": self.current_weather,
            "pm25": pm25,
            "pm10": pm10,
            "co2": co2,
            "temperature": temperature,
            "humidity": humidity,
            "timestamp": timestamp,
            "sensor_id": self.fake.uuid4(),
            "air_quality_index": self.calculate_aqi(pm25, pm10)
        }
    
    def calculate_aqi(self, pm25, pm10):
        """Calculate Air Quality Index based on PM levels."""
        # Simple AQI calculation
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
        """Simulate specific air quality events."""
        if event_type == "high_pollution":
            self.state["pm25"] = self.fake.random_int(min=50, max=100)
            self.state["pm10"] = self.fake.random_int(min=100, max=200)
        elif event_type == "high_co2":
            self.state["co2"] = self.fake.random_int(min=800, max=1200)
        elif event_type == "temperature_spike":
            self.state["temperature"] = self.fake.random_int(min=35, max=45)
        elif event_type == "reset":
            self.state = {
                "pm25": self.fake.random_int(min=8, max=35),
                "pm10": self.fake.random_int(min=15, max=60),
                "co2": self.fake.random_int(min=400, max=450),
                "humidity": self.fake.random_int(min=45, max=75),
                "temperature": self.fake.random_int(min=15, max=30)
            }
        
        return self.generate_realistic_data()

# Global Faker sensor instance
faker_sensor = FakerAirQualitySensor()

if __name__ == "__main__":
    print(f"🌬️ Faker Air Quality Sensor running in {faker_sensor.location}...")
    print("📍 Location:", faker_sensor.location)
    
    while True:
        data = faker_sensor.generate_realistic_data()
        faker_sensor.producer.send('air_quality', value=data)
        print(f"📊 {data['weather'].upper()} | PM2.5: {data['pm25']} | PM10: {data['pm10']} | CO2: {data['co2']} | Temp: {data['temperature']}°C | Humidity: {data['humidity']}% | AQI: {data['air_quality_index']}")
        faker_sensor.weather_cycle += 1
        time.sleep(5) 