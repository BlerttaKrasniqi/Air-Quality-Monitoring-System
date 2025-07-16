import time
import json
import math
import random
from datetime import datetime
from kafka import KafkaProducer


class AirQualitySensor:
    def __init__(self):
        self.state = {
            "pm25": 25.0,  # More realistic urban PM2.5
            "pm10": 45.0,   # PM10 is usually 1.5-2x PM2.5
            "co2": 420.0,   # Normal outdoor CO2
            "humidity": 65.0 # Moderate humidity
        }
        self.producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.last_update = datetime.utcnow()
        self.weather_cycle = 0

    def day_night_temperature(self):
        """Simulate realistic 24-hour temperature fluctuation."""
        now = datetime.utcnow()
        hour = now.hour + now.minute / 60
        
       
        # More realistic temperature pattern with seasonal variation
        if 6 <= hour < 12:  # Morning warming
            base_temp = 15 + (hour - 6) * 2.0
        elif 12 <= hour < 18:  # Afternoon peak
            base_temp = 27 + (hour - 12) * 0.3
        elif 18 <= hour < 22:  # Evening cooling
            base_temp = 27 - (hour - 18) * 1.5
        else:  # Night cooling
            base_temp = 12 - (hour - 22) * 0.8 if hour >= 22 else 12 - (24 - hour) * 0.8
            
        # Add realistic variation
        variation = random.uniform(-2.0, 2.0)
        return round(base_temp + variation, 1)

    def simulate_traffic_pattern(self, hour):
        """Simulate traffic-related pollution patterns."""
       
        if (7 <= hour < 9) or (17 <= hour < 19):  
            return random.uniform(1.5, 2.5)  
        elif (9 <= hour < 17):  
            return random.uniform(1.0, 1.3)
        else:  
            return random.uniform(0.6, 0.9)

    def update_pm(self, pm_value, pm_type="pm25"):
        """Simulate realistic PM levels with traffic and weather patterns."""
        now = datetime.utcnow()
        hour = now.hour + now.minute / 60
        
        
        # Base drift (natural variation)
        base_drift = random.uniform(-1.0, 1.0)
        
        # Traffic influence
        traffic_factor = self.simulate_traffic_pattern(hour)
        
        # Weather influence (wind, rain, etc.)
        weather_factor = random.uniform(0.7, 1.3)
        
        # Calculate new value with more realistic ranges
        if pm_type == "pm25":
            new_value = pm_value + base_drift + (traffic_factor - 1) * 3
            return max(10, min(100, round(new_value, 1)))  # Realistic PM2.5 range
        else:  # PM10
            new_value = pm_value + base_drift + (traffic_factor - 1) * 5
            return max(20, min(200, round(new_value, 1)))  # PM10 is higher 

    def update_co2(self, co2_value):
        """Simulate realistic CO2 levels in outdoor environment."""
        now = datetime.utcnow()
        hour = now.hour + now.minute / 60
        
        # Outdoor CO2 is more stable, with slight variations
        # Base outdoor CO2 is around 400-420 ppm
        base_co2 = 410
        
        # Small variations based on time and traffic
        if (7 <= hour < 9) or (17 <= hour < 19):  # Rush hours
            variation = random.uniform(5, 15)
        elif 8 <= hour < 18:  # Daytime
            variation = random.uniform(-5, 10)
        else:  # Night time
            variation = random.uniform(-10, 5)
        
        new_value = base_co2 + variation
        return max(400, min(450, round(new_value, 1)))
    def update_humidity(self, humidity, temperature):
        """Simulate realistic humidity based on temperature and time."""
        now = datetime.utcnow()
        hour = now.hour + now.minute / 60
        
        
        # Humidity is inversely related to temperature
        temp_factor = -1.5 if temperature > 25 else 0.5
        
        # Time-based humidity patterns (higher at night)
        time_factor = 1.0 if 22 <= hour or hour < 6 else -0.5
        
        # Weather variation
        weather_variation = random.uniform(-2, 2)
        
        new_humidity = humidity + temp_factor + time_factor + weather_variation
        return max(40, min(80, round(new_humidity, 1)))

    def generate_data(self):
        temperature = self.day_night_temperature()
        self.state["pm25"] = self.update_pm(self.state["pm25"], "pm25")
        self.state["pm10"] = self.update_pm(self.state["pm10"], "pm10")
        self.state["co2"] = self.update_co2(self.state["co2"])
        self.state["humidity"] = self.update_humidity(self.state["humidity"], temperature)

        return {
            "pm25": self.state["pm25"],
            "pm10": self.state["pm10"],
            "co2": self.state["co2"],
            "temperature": temperature,
            "humidity": self.state["humidity"],
            "timestamp": datetime.utcnow().isoformat()
        }



sensor = AirQualitySensor()

if __name__ == "__main__":
    print(" Smart air quality sensor running without events...")
    
    while True:
        data = sensor.generate_data()
        sensor.producer.send('air_quality', value=data)
        print("Sent:", data)
        time.sleep(5)
