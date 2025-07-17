from flask import Flask, render_template, jsonify, request
from cassandra.cluster import Cluster
import pandas
import random
import sys
import os
from faker_sensor import faker_sensor
from prediction_service import AirQualityPredictor
from flask import jsonify
import pandas as pd
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

predictor = AirQualityPredictor(
    model_path="models/air_quality_model.joblib", 
    scaler_path="models/air_quality_scaler.joblib"
)

@app.route('/api/predict', methods=['POST'])
def predict():
    """API endpoint to get predictions based on current sensor data"""
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        # Make prediction
        prediction = predictor.predict(data)
        
        return jsonify({
            "predicted_pm25": float(prediction),
            "timestamp": datetime.now().isoformat()
        })
    
    except Exception as e:
        app.logger.error(f"Error making prediction: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/predict_future', methods=['POST'])
def predict_future():
    """API endpoint to predict future air quality based on current data"""
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        hours_ahead = int(request.args.get('hours', 24))
        
        # Make future predictions
        predictions = predictor.predict_future(data, hours_ahead=hours_ahead)
        
        # Format for response
        formatted_predictions = []
        for pred in predictions:
            formatted_predictions.append({
                "timestamp": pred["timestamp"].isoformat(),
                "predicted_pm25": float(pred["predicted_pm25"])
            })
        
        return jsonify(formatted_predictions)
    
    except Exception as e:
        app.logger.error(f"Error making future predictions: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
