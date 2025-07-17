from flask import Flask, render_template, jsonify, request
from cassandra.cluster import Cluster
from sensor_simulator.sensor_simulator import SmartAirQualitySensor
from datetime import datetime,timezone, timedelta

app = Flask(__name__)


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
sensor = SmartAirQualitySensor(kosovo_config)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/realtime-data")
def realtime_data():
    cluster = Cluster(['cassandra'])
    session = cluster.connect('air_monitoring')

    query = "SELECT * FROM sensor_data LIMIT 1"
    rows = session.execute(query)
    data = rows.one()

    if data:
        return jsonify({
            "pm25": data.pm25,
            "pm10": data.pm10,
            "co2": data.co2,
            "temperature": data.temperature,
            "humidity": data.humidity,
            "timestamp": (
                data.timestamp.replace(tzinfo=timezone.utc)
                            .astimezone(timezone(timedelta(hours=2)))
                            .strftime("%Y-%m-%d %H:%M:%S")),
            "sensor_id": data.sensor_id
        })
    else:
        return jsonify({"error": "No data available"}), 404

@app.route("/api/simulate-event", methods=["POST"])
def simulate_event():
    event_type = request.json.get("event_type", "normal")
    return jsonify(sensor.simulate_event(event_type))

@app.route("/api/sensor-data")
def get_sensor_data():
    return jsonify(sensor.generate_smart_data())

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
