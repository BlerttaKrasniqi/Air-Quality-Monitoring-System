#!/usr/bin/env python3
"""
Test script for the Smart Air Quality Sensor Simulator
"""

import sys
import os
import time
import json
from datetime import datetime

# Add sensor simulator to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'sensor_simulator'))

try:
    from smart_sensor_simulator import SmartAirQualitySensor
    print("✅ Successfully imported SmartAirQualitySensor")
except ImportError as e:
    print(f"❌ Failed to import SmartAirQualitySensor: {e}")
    sys.exit(1)

def test_sensor_initialization():
    """Test sensor initialization with different configurations."""
    print("\n🧪 Testing Sensor Initialization...")
    
    # Test default configuration
    sensor1 = SmartAirQualitySensor()
    print(f"✅ Default sensor initialized in {sensor1.location}")
    
    # Test custom configuration
    custom_config = {
        'city': 'Los Angeles',
        'latitude': 34.0522,
        'longitude': -118.2437,
        'elevation': 93,
        'population_density': 'high',
        'industrial_zones': [
            {'lat': 34.0000, 'lng': -118.2000, 'type': 'refinery'},
            {'lat': 33.9500, 'lng': -118.2500, 'type': 'manufacturing'}
        ]
    }
    
    sensor2 = SmartAirQualitySensor(custom_config)
    print(f"✅ Custom sensor initialized in {sensor2.location}")
    
    return sensor1, sensor2

def test_data_generation(sensor, num_samples=10):
    """Test data generation and validate ranges."""
    print(f"\n🧪 Testing Data Generation ({num_samples} samples)...")
    
    samples = []
    for i in range(num_samples):
        data = sensor.generate_smart_data()
        samples.append(data)
        
        # Validate data ranges
        assert 5 <= data['pm25'] <= 150, f"PM2.5 out of range: {data['pm25']}"
        assert 10 <= data['pm10'] <= 300, f"PM10 out of range: {data['pm10']}"
        assert 400 <= data['co2'] <= 600, f"CO2 out of range: {data['co2']}"
        assert -20 <= data['temperature'] <= 50, f"Temperature out of range: {data['temperature']}"
        assert 20 <= data['humidity'] <= 95, f"Humidity out of range: {data['humidity']}"
        
        print(f"  Sample {i+1}: PM2.5={data['pm25']}, PM10={data['pm10']}, CO2={data['co2']}, "
              f"Temp={data['temperature']}, Humidity={data['humidity']}, AQI={data['air_quality_index']}")
        
        time.sleep(0.1)  # Small delay between samples
    
    print(f"✅ Generated {num_samples} valid samples")
    return samples

def test_event_simulation(sensor):
    """Test event simulation functionality."""
    print("\n🧪 Testing Event Simulation...")
    
    events = ['traffic_jam', 'industrial_spill', 'wildfire', 'construction', 'weather_event']
    
    for event in events:
        print(f"  Simulating {event}...")
        data = sensor.simulate_event(event)
        
        # Verify event is active
        assert event in data.get('active_events', []), f"Event {event} not found in active events"
        print(f"    ✅ {event} event triggered successfully")
    
    print("✅ All events simulated successfully")

def test_sensor_status(sensor):
    """Test sensor status and health monitoring."""
    print("\n🧪 Testing Sensor Status...")
    
    status = sensor.get_sensor_status()
    
    required_fields = ['location', 'uptime', 'sensor_drift', 'active_events', 'weather_cycle']
    for field in required_fields:
        assert field in status, f"Missing field in status: {field}"
    
    print(f"✅ Sensor status: {status['location']}, Uptime: {status['uptime']:.1f}s")
    print(f"   Active events: {status['active_events']}")
    print(f"   Data points: {status.get('data_points_generated', 0)}")

def test_weather_patterns(sensor):
    """Test weather pattern generation."""
    print("\n🧪 Testing Weather Patterns...")
    
    weather_samples = []
    for i in range(20):
        data = sensor.generate_smart_data()
        weather_samples.append(data['weather'])
        time.sleep(0.1)
    
    unique_weather = set(weather_samples)
    print(f"✅ Weather patterns: {unique_weather}")
    print(f"   Generated {len(weather_samples)} weather samples")

def test_trend_analysis(sensor):
    """Test trend analysis functionality."""
    print("\n🧪 Testing Trend Analysis...")
    
    # Generate some data first
    for i in range(50):
        sensor.generate_smart_data()
        time.sleep(0.05)
    
    # Get trend predictions
    trends = sensor._predict_trends()
    
    if trends:
        print("✅ Trend analysis results:")
        for param, trend in trends.items():
            print(f"   {param}: {trend['trend']} (rate: {trend['change_rate']})")
    else:
        print("⚠️  No trend data available yet")

def test_sensor_drift(sensor):
    """Test sensor drift simulation."""
    print("\n🧪 Testing Sensor Drift...")
    
    initial_drift = sensor.sensor_drift.copy()
    
    # Generate data to trigger drift
    for i in range(10):
        sensor.generate_smart_data()
        time.sleep(0.1)
    
    current_drift = sensor.sensor_drift
    
    print(f"✅ Initial drift: {initial_drift}")
    print(f"   Current drift: {current_drift}")
    
    # Test calibration
    sensor.simulate_event('reset')  # This should reset drift
    print(f"   After reset: {sensor.sensor_drift}")

def run_comprehensive_test():
    """Run comprehensive test suite."""
    print("🚀 Starting Smart Air Quality Sensor Test Suite")
    print("=" * 60)
    
    try:
        # Initialize sensors
        sensor1, sensor2 = test_sensor_initialization()
        
        # Test data generation
        samples1 = test_data_generation(sensor1, 5)
        samples2 = test_data_generation(sensor2, 5)
        
        # Test event simulation
        test_event_simulation(sensor1)
        
        # Test sensor status
        test_sensor_status(sensor1)
        
        # Test weather patterns
        test_weather_patterns(sensor1)
        
        # Test trend analysis
        test_trend_analysis(sensor1)
        
        # Test sensor drift
        test_sensor_drift(sensor1)
        
        print("\n" + "=" * 60)
        print("🎉 All tests passed! Smart sensor is working correctly.")
        print("=" * 60)
        
        # Show sample data
        print("\n📊 Sample Data from Smart Sensor:")
        sample_data = sensor1.generate_smart_data()
        print(json.dumps(sample_data, indent=2, default=str))
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = run_comprehensive_test()
    sys.exit(0 if success else 1) 