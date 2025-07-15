#!/usr/bin/env python3
"""
Test script to verify Kosovo sensor data is working correctly.
"""

import sys
import os

# Add sensor simulator to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'sensor_simulator'))
from smart_sensor_simulator import SmartAirQualitySensor

def test_kosovo_sensor():
    """Test the Kosovo sensor configuration."""
    
    # Create Kosovo configuration
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
    
    # Create sensor with Kosovo config
    sensor = SmartAirQualitySensor(kosovo_config)
    
    print("🌍 Kosovo Air Quality Sensor Test")
    print("=" * 50)
    print(f"📍 Location: {sensor.location}")
    print(f"🌍 Coordinates: {sensor.latitude}, {sensor.longitude}")
    print(f"🏭 Industrial zones: {len(sensor.industrial_zones)}")
    print(f"🌤️ Current season: {sensor.season}")
    print()
    
    # Generate test data
    print("📊 Generating test data...")
    for i in range(5):
        data = sensor.generate_smart_data()
        print(f"Sample {i+1}:")
        print(f"  📍 Location: {data['location']}")
        print(f"  🌡️ Temperature: {data['temperature']}°C")
        print(f"  💧 Humidity: {data['humidity']}%")
        print(f"  🌫️ PM2.5: {data['pm25']} μg/m³")
        print(f"  🌫️ PM10: {data['pm10']} μg/m³")
        print(f"  🏭 CO2: {data['co2']} ppm")
        print(f"  🌤️ Weather: {data['weather']}")
        print(f"  📊 AQI: {data['air_quality_index']}")
        print(f"  🚨 Events: {data['active_events']}")
        print()

if __name__ == "__main__":
    test_kosovo_sensor() 