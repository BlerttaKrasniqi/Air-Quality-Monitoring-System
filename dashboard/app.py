import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from flask import Flask, render_template, jsonify, request
from cassandra.cluster import Cluster
import pandas

from sensor_simulator.faker_sensor import FakerAirQualitySensor


app = Flask(__name__)
sensor = FakerAirQualitySensor()  # Instantiate once globally

@app.route("/")
def index():
    cluster = Cluster(['cassandra'])
    session = cluster.connect('air_monitoring')

    rows = session.execute("SELECT * FROM sensor_data LIMIT 100;")

    data = pandas.DataFrame(rows, columns=['id', 'pm25', 'pm10', 'co2', 'temperature', 'humidity', 'timestamp'])

    if not data.empty:
        data = data.sort_values(by='timestamp', ascending=False)
        data['timestamp'] = data['timestamp'].astype(str)

    records = data.to_dict(orient='records')

    return render_template("smart_dashboard.html", data=records)

@app.route("/simulate")
def simulate_page():
    return render_template("simulate.html")

@app.route("/api/sensor-data")
def get_sensor_data():
    """Get current sensor data"""
    return jsonify(sensor.generate_realistic_data())

@app.route("/api/simulate-event", methods=["POST"])
def simulate_event():
    """Simulate an air quality event"""
    event_type = request.json.get("event_type", "normal")
    return jsonify(sensor.simulate_event(event_type))

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
