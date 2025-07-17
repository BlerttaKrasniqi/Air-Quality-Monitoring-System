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
from datetime import datetime

app = Flask(__name__)

@app.route("/") 
def index():
    cluster = Cluster(['cassandra'])
    session = cluster.connect('air_monitoring')

    rows = session.execute("SELECT * FROM sensor_data LIMIT 100;")

    data = pandas.DataFrame(rows, columns=['id', 'pm25', 'pm10', 'co2', 'temperature', 'humidity', 'timestamp'])

    if not data.empty:
        data=data.sort_values(by='timestamp',ascending=False)
        data['timestamp'] = data['timestamp'].astype(str)
    
    records = data.to_dict(orient='records')

  

    return render_template("smart_dashboard.html", data=records)  

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

predictor = AirQualityPredictor(
    model_path="models/air_quality_model.joblib", 
    scaler_path="models/air_quality_scaler.joblib"
)

# Add these routes to your Flask app
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
    app.run(host="0.0.0.0", port=8501)
