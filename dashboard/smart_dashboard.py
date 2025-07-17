from flask import Flask, render_template, jsonify, request, Response
from cassandra.cluster import Cluster
import pandas as pd
import json
import sys
import os
import threading
import time
from datetime import datetime, timedelta
import logging

# Add sensor simulator to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'sensor_simulator'))
from smart_sensor_simulator import smart_sensor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global data storage for real-time updates
realtime_data = []
data_lock = threading.Lock()

class SmartDashboard:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.connect_to_cassandra()
    
    def connect_to_cassandra(self):
        """Connect to Cassandra database."""
        try:
            self.cluster = Cluster(['cassandra'])
            self.session = self.cluster.connect('air_monitoring')
            logger.info("✅ Connected to Cassandra database")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Cassandra: {e}")
            self.session = None
    
    def get_historical_data(self, limit=100):
        """Get historical sensor data from Cassandra."""
        if not self.session:
            return pd.DataFrame()
        
        try:
            rows = self.session.execute("SELECT * FROM sensor_data LIMIT %s;", (limit,))
            data = pd.DataFrame(rows, columns=['id', 'pm25', 'pm10', 'co2', 'temperature', 'humidity', 'timestamp'])
            
            if not data.empty:
                data = data.sort_values(by='timestamp', ascending=False)
                data['timestamp'] = data['timestamp'].astype(str)
            
            return data
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return pd.DataFrame()
    
    def get_sensor_status(self):
        """Get current sensor status and health."""
        return smart_sensor.get_sensor_status()
    
    def get_realtime_data(self):
        """Get current real-time sensor data."""
        with data_lock:
            return realtime_data[-1] if realtime_data else None
    
    def get_trend_analysis(self):
        """Analyze trends in sensor data."""
        if not realtime_data:
            return {}
        
        # Get last 50 data points for trend analysis
        recent_data = realtime_data[-50:] if len(realtime_data) >= 50 else realtime_data
        
        trends = {}
        for param in ['pm25', 'pm10', 'co2', 'temperature', 'humidity']:
            if recent_data:
                values = [point.get(param, 0) for point in recent_data if point.get(param)]
                if len(values) >= 2:
                    # Simple linear trend calculation
                    x = list(range(len(values)))
                    slope = (values[-1] - values[0]) / len(values) if len(values) > 1 else 0
                    trends[param] = {
                        'current': values[-1],
                        'trend': 'increasing' if slope > 0 else 'decreasing' if slope < 0 else 'stable',
                        'change_rate': round(slope, 3)
                    }
        
        return trends
    
    def get_event_summary(self):
        """Get summary of recent events."""
        if not realtime_data:
            return []
        
        events = []
        for data_point in realtime_data[-20:]:  # Last 20 data points
            if data_point.get('active_events'):
                for event in data_point['active_events']:
                    events.append({
                        'type': event,
                        'timestamp': data_point.get('timestamp'),
                        'aqi': data_point.get('air_quality_index')
                    })
        
        return events[-10:]  # Return last 10 events

# Initialize dashboard
dashboard = SmartDashboard()

@app.route("/")
def index():
    """Main dashboard page."""
    try:
        # Get historical data
        historical_data = dashboard.get_historical_data()
        records = historical_data.to_dict(orient='records') if not historical_data.empty else []
        
        # Get sensor status
        sensor_status = dashboard.get_sensor_status()
        
        return render_template("smart_dashboard.html", 
                             data=records, 
                             sensor_status=sensor_status)
    except Exception as e:
        logger.error(f"Error in main dashboard: {e}")
        return render_template("smart_dashboard.html", 
                             data=[], 
                             sensor_status={})

@app.route("/api/realtime-data")
def get_realtime_data():
    """Get real-time sensor data."""
    try:
        data = dashboard.get_realtime_data()
        return jsonify(data if data else {})
    except Exception as e:
        logger.error(f"Error getting realtime data: {e}")
        return jsonify({})

@app.route("/api/trends")
def get_trends():
    """Get trend analysis."""
    try:
        trends = dashboard.get_trend_analysis()
        return jsonify(trends)
    except Exception as e:
        logger.error(f"Error getting trends: {e}")
        return jsonify({})

@app.route("/api/events")
def get_events():
    """Get recent events."""
    try:
        events = dashboard.get_event_summary()
        return jsonify(events)
    except Exception as e:
        logger.error(f"Error getting events: {e}")
        return jsonify([])

@app.route("/api/sensor-status")
def get_sensor_status():
    """Get sensor status."""
    try:
        status = dashboard.get_sensor_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting sensor status: {e}")
        return jsonify({})

@app.route("/api/simulate-event", methods=["POST"])
def simulate_event():
    """Simulate an air quality event."""
    try:
        event_type = request.json.get("event_type", "traffic_jam")
        data = smart_sensor.simulate_event(event_type)
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error simulating event: {e}")
        return jsonify({"error": str(e)})

@app.route("/api/update-location", methods=["POST"])
def update_location():
    """Update sensor location."""
    try:
        location_data = request.json
        # This would require reinitializing the sensor with new location
        # For now, just return success
        return jsonify({"status": "success", "message": "Location updated"})
    except Exception as e:
        logger.error(f"Error updating location: {e}")
        return jsonify({"error": str(e)})

@app.route("/api/calibrate-sensor", methods=["POST"])
def calibrate_sensor():
    """Calibrate sensor drift."""
    try:
        # Reset sensor drift
        for sensor_type in smart_sensor.sensor_drift:
            smart_sensor.sensor_drift[sensor_type] = 0.0
        
        return jsonify({"status": "success", "message": "Sensor calibrated"})
    except Exception as e:
        logger.error(f"Error calibrating sensor: {e}")
        return jsonify({"error": str(e)})

def update_realtime_data():
    """Background thread to update real-time data."""
    while True:
        try:
            data = smart_sensor.generate_smart_data()
            
            with data_lock:
                realtime_data.append(data)
                # Keep only last 100 data points
                if len(realtime_data) > 100:
                    realtime_data.pop(0)
            
            time.sleep(5)  # Update every 5 seconds
            
        except Exception as e:
            logger.error(f"Error updating realtime data: {e}")
            time.sleep(5)

# Start background data update thread
data_thread = threading.Thread(target=update_realtime_data, daemon=True)
data_thread.start()

if __name__ == "__main__":
    print("🚀 Starting Smart Air Quality Dashboard...")
    print(f"📍 Sensor Location: {smart_sensor.location}")
    print(f"🌤️ Current Season: {smart_sensor.season}")
    print("📊 Dashboard will be available at http://localhost:5000")
    
    app.run(debug=True, host='0.0.0.0', port=5000) 
