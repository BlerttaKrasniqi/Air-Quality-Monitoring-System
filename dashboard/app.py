from flask import Flask, render_template, jsonify, request
from cassandra.cluster import Cluster
import pandas
import random
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'sensor_simulator'))
from faker_sensor import faker_sensor

app = Flask(__name__)

@app.route("/") 
def index():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('air_monitoring')

    rows = session.execute("SELECT * FROM sensor_data LIMIT 100;")

    data = pandas.DataFrame(rows, columns=['id', 'pm25', 'pm10', 'co2', 'temperature', 'humidity', 'timestamp'])

    if not data.empty:
        data=data.sort_values(by='timestamp',ascending=False)
        data['timestamp'] = data['timestamp'].astype(str)
    
    records = data.to_dict(orient='records')

  

    return render_template("index.html", data=records)  

@app.route("/simulate")
def simulate_page():
    return render_template("simulate.html")

@app.route("/api/sensor-data")
def get_sensor_data():
    """Get current sensor data"""
    data = faker_sensor.generate_realistic_data()
    return jsonify(data)

@app.route("/api/simulate-event", methods=["POST"])
def simulate_event():
    """Simulate an air quality event"""
    event_type = request.json.get("event_type", "normal")
    
    data = faker_sensor.simulate_event(event_type)
    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)
