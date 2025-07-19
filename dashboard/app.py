from flask import Flask, render_template, jsonify, request
from cassandra.cluster import Cluster
import pandas as pd
import random
import sys
import os
import time
from prediction_service import AirQualityPredictor
from sensor_simulator.sensor_simulator import SmartAirQualitySensor
from datetime import datetime, timezone, timedelta
import logging
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from prometheus_metrics import (
    registry, RequestMonitoringMiddleware, TimerContextManager, 
    http_request_duration, model_prediction_counter, model_prediction_duration,
    model_training_duration, model_accuracy, db_query_counter, 
    db_query_duration, db_row_count, start_metrics_collection
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
# Add Prometheus monitoring middleware
RequestMonitoringMiddleware(app)

os.makedirs('models', exist_ok=True)
predictor = None

# Predictor initializer
def get_predictor():
    global predictor
    if predictor is None:
        try:
            predictor = AirQualityPredictor()
            if not os.path.exists(predictor.model_path) or not predictor.model:
                logger.info("No model found. Training a new model at startup...")
                
                with TimerContextManager(
                    model_training_duration, 
                    {'model_type': 'random_forest', 'data_source': 'cassandra'}
                ):
                    metrics = predictor.train(data_source='cassandra', days=1)
                
                if metrics:
                    logger.info("Model trained successfully at startup with metrics: %s", metrics)
                    # Record model accuracy metrics
                    model_accuracy.labels(model_type='random_forest', metric_name='r2_score').set(metrics['r2_score'])
                    model_accuracy.labels(model_type='random_forest', metric_name='mse').set(metrics['mse'])
                    model_accuracy.labels(model_type='random_forest', metric_name='rmse').set(metrics['rmse'])
                else:
                    logger.error("Model training failed at startup.")
        except Exception as e:
            logger.error(f"Error initializing predictor: {e}", exc_info=True)
    return predictor

# Initialize predictor at startup
with app.app_context():
    get_predictor()
    # Start metrics collection
    start_metrics_collection()

# Smart sensor configuration

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

@app.route("/metrics")
def metrics():
    """Expose Prometheus metrics"""
    return generate_latest(registry), 200, {'Content-Type': CONTENT_TYPE_LATEST}

@app.route("/api/realtime-data")
def realtime_data():
    try:
        # Use timer to measure database query duration
        with TimerContextManager(db_query_duration, {'query_type': 'sensor_data_recent'}):
            cluster = Cluster(['cassandra'])
            session = cluster.connect('air_monitoring')
            query = "SELECT * FROM sensor_data LIMIT 1"
            rows = session.execute(query)
            data = rows.one()
        
        # Record database metrics
        db_query_counter.labels(query_type='sensor_data_recent', status='success').inc()
        db_row_count.labels(query_type='sensor_data_recent').observe(1 if data else 0)

        if data:
            return jsonify({
                "pm25": data.pm25,
                "pm10": data.pm10,
                "co2": data.co2,
                "temperature": data.temperature,
                "humidity": data.humidity,
                "timestamp": data.timestamp.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=2))).strftime("%Y-%m-%d %H:%M:%S"),
                "id": data.sensor_id
            })
        else:
            # Record failure
            db_query_counter.labels(query_type='sensor_data_recent', status='no_data').inc()
            return jsonify({"error": "No data available"}), 404

    except Exception as e:
        # Record database error
        db_query_counter.labels(query_type='sensor_data_recent', status='error').inc()
        logger.error(f"Error in realtime data: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

    query = "SELECT * FROM sensor_data LIMIT 100 ALLOW FILTERING"
    rows = session.execute(query)

   
    data_list = list(rows)
    if not data_list:
        return jsonify({"error": "No data available"}), 404

    latest_data = max(data_list, key=lambda x: x.timestamp)

    return jsonify({
        "pm25": latest_data.pm25,
        "pm10": latest_data.pm10,
        "co2": latest_data.co2,
        "temperature": latest_data.temperature,
        "humidity": latest_data.humidity,
        "timestamp": latest_data.timestamp.replace(tzinfo=timezone.utc)
            .astimezone(timezone(timedelta(hours=2)))
            .strftime("%Y-%m-%d %H:%M:%S"),
        "sensor_id": latest_data.id
    })


@app.route("/api/simulate-event", methods=["POST"])
def simulate_event():
    event_type = request.json.get("event_type", "normal")
    return jsonify(sensor.simulate_event(event_type))

@app.route("/api/sensor-data")
def get_sensor_data():
    return jsonify(sensor.generate_smart_data())

@app.route('/api/predict', methods=['POST'])
def predict():
    try:
        predictor = get_predictor()
        if predictor is None or predictor.model is None or predictor.scaler is None:
            model_prediction_counter.labels(model_type='random_forest', status='unavailable').inc()
            logger.error("Predictor or necessary components are not loaded.")
            return jsonify({"error": "Model or scaler not available."}), 500

        data = request.json
        logger.info(f"Received data for prediction: {data}")

        if not data:
            model_prediction_counter.labels(model_type='random_forest', status='invalid_input').inc()
            return jsonify({"error": "No data provided."}), 400

        required_fields = ['temperature', 'humidity', 'co2']
        for field in required_fields:
            if field not in data:
                model_prediction_counter.labels(model_type='random_forest', status='missing_field').inc()
                logger.error(f"Missing required field: {field}")
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Time the prediction
        with TimerContextManager(model_prediction_duration, {'model_type': 'random_forest'}):
            prediction = predictor.predict(data)
        
        # Record successful prediction
        model_prediction_counter.labels(model_type='random_forest', status='success').inc()
        
        return jsonify({
            "predicted_pm25": float(prediction),
            "timestamp": datetime.now().isoformat()
        })

    except Exception as e:
        # Record failed prediction
        model_prediction_counter.labels(model_type='random_forest', status='error').inc()
        logger.error(f"Error making prediction: {e}", exc_info=True)
        return jsonify({"error": "Internal server error during prediction."}), 500

@app.route('/api/predict_future', methods=['POST'])
def predict_future():
    try:
        predictor = get_predictor()
        if predictor is None or predictor.model is None or predictor.scaler is None:
            model_prediction_counter.labels(model_type='random_forest', status='unavailable').inc()
            return jsonify({"error": "Model or scaler not available."}), 500

        data = request.json
        logger.info(f"Received data for future prediction: {data}")

        if not data:
            model_prediction_counter.labels(model_type='random_forest', status='invalid_input').inc()
            return jsonify({"error": "No data provided."}), 400

        required_fields = ['temperature', 'humidity', 'co2']
        for field in required_fields:
            if field not in data:
                model_prediction_counter.labels(model_type='random_forest', status='missing_field').inc()
                logger.error(f"Missing required field: {field}")
                return jsonify({"error": f"Missing required field: {field}"}), 400

        hours_ahead = int(request.args.get('hours', 24))
        
        # Time the future prediction
        with TimerContextManager(model_prediction_duration, {'model_type': 'random_forest'}):
            predictions = predictor.predict_future(data, hours_ahead=hours_ahead)

        # Record successful predictions
        model_prediction_counter.labels(model_type='random_forest', status='success').inc(hours_ahead)
        
        formatted_predictions = [{
            "timestamp": pred["timestamp"].isoformat(),
            "predicted_pm25": float(pred["predicted_pm25"])
        } for pred in predictions]

        return jsonify(formatted_predictions)

    except Exception as e:
        # Record failed prediction
        model_prediction_counter.labels(model_type='random_forest', status='error').inc()
        logger.error(f"Error predicting future values: {e}", exc_info=True)
        return jsonify({"error": "Internal server error during future prediction."}), 500

@app.route('/api/model-info', methods=['GET'])
def model_info():
    try:
        predictor = get_predictor()
        if predictor:
            info = predictor.get_model_info()
            return jsonify({"status": "success", **info})
        return jsonify({"status": "error", "message": "Predictor not initialized."}), 500

    except Exception as e:
        logger.error(f"Error retrieving model info: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/train-model', methods=['POST'])
def train_model():
    try:
        predictor = get_predictor()
        data = request.json or {}
        data_source = data.get('data_source', 'cassandra')
        days = data.get('days', 30)

        # Time the training process
        with TimerContextManager(
            model_training_duration, 
            {'model_type': 'random_forest', 'data_source': data_source}
        ):
            metrics = predictor.train(data_source=data_source, days=days)
        
        if metrics:
            # Record model accuracy metrics
            model_accuracy.labels(model_type='random_forest', metric_name='r2_score').set(metrics['r2_score'])
            model_accuracy.labels(model_type='random_forest', metric_name='mse').set(metrics['mse'])
            model_accuracy.labels(model_type='random_forest', metric_name='rmse').set(metrics['rmse'])
            
            return jsonify({
                "status": "success",
                "message": "Model trained successfully",
                "metrics": metrics
            })
        return jsonify({"status": "error", "message": "Model training failed."}), 500

    except Exception as e:
        logger.error(f"Error training model: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
