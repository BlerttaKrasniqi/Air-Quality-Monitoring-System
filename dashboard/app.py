import os
import time
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template, request

from prediction_service import AirQualityPredictor
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from prometheus_metrics import (
    registry, RequestMonitoringMiddleware, TimerContextManager, 
    http_request_duration, model_prediction_counter, model_prediction_duration,
    model_training_duration, model_accuracy, db_query_counter, 
    db_query_duration, db_row_count, start_metrics_collection
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Optional CORS (auto-enable if installed)
try:
    from flask_cors import CORS  # type: ignore
    _HAS_CORS = True
except Exception:  # pragma: no cover
    _HAS_CORS = False

# Optional Prometheus metrics (auto-enable if installed)
try:
    from prometheus_client import (  # type: ignore
        generate_latest, Counter, Histogram, CONTENT_TYPE_LATEST
    )
    _HAS_METRICS = True
except Exception:  # pragma: no cover
    _HAS_METRICS = False

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_monitoring")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "sensor_data")
PORT = int(os.getenv("PORT", "5000"))

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("frontend")

# -----------------------------------------------------------------------------
# Flask
# -----------------------------------------------------------------------------
app = Flask(__name__, static_folder="static", static_url_path="/static", template_folder="templates")
if _HAS_CORS:
    CORS(app)

# -----------------------------------------------------------------------------
# Prometheus metrics (no-op if lib not installed)
# -----------------------------------------------------------------------------
if _HAS_METRICS:
    REQUEST_COUNTER = Counter(
        "frontend_http_requests_total", "Total HTTP requests", ["endpoint", "method", "status"]
    )
    REQUEST_LATENCY = Histogram(
        "frontend_http_request_seconds", "Latency of HTTP requests", ["endpoint", "method"]
    )

def record_metrics(endpoint: str, method: str, status: str, latency_s: float) -> None:
    if not _HAS_METRICS:
        return
    REQUEST_COUNTER.labels(endpoint=endpoint, method=method, status=status).inc()
    REQUEST_LATENCY.labels(endpoint=endpoint, method=method).observe(latency_s)

# -----------------------------------------------------------------------------
# Cassandra connection with retries
# -----------------------------------------------------------------------------
_cluster = None
_session = None

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

def connect_cassandra(retries: int = 20, delay_s: float = 3.0) -> None:
    """Connect to Cassandra with optional username/password env vars."""
    global _cluster, _session
    for attempt in range(1, retries + 1):
        try:
            username = os.getenv("CASSANDRA_USER")
            password = os.getenv("CASSANDRA_PASSWORD")
            if username and password:
                auth = PlainTextAuthProvider(username=username, password=password)
                _cluster = Cluster([CASSANDRA_HOST], auth_provider=auth)
            else:
                _cluster = Cluster([CASSANDRA_HOST])
            _session = _cluster.connect(CASSANDRA_KEYSPACE)
            log.info("Connected to Cassandra keyspace '%s' at '%s'", CASSANDRA_KEYSPACE, CASSANDRA_HOST)
            return
        except Exception as e:
            log.warning("Cassandra not ready (%s), retry %d/%d...", e, attempt, retries)
            time.sleep(delay_s)
    raise RuntimeError("Could not connect to Cassandra after retries")

def get_session():
    if _session is None:
        connect_cassandra()
    return _session

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def row_to_dict(row) -> Dict[str, Any]:
    """Tolerate either 'sensor_id' or 'id' as the PK; pass through the rest."""
    sid = getattr(row, "sensor_id", getattr(row, "id", None))
    ts = getattr(row, "timestamp", None)
    # Convert timestamp to ISO for JS charts if it's a datetime
    if ts is not None:
        try:
            ts = ts.isoformat()
        except Exception:
            pass
    return {
        "sensor_id": sid,
        "pm25": getattr(row, "pm25", None),
        "pm10": getattr(row, "pm10", None),
        "co2": getattr(row, "co2", None),
        "temperature": getattr(row, "temperature", None),
        "humidity": getattr(row, "humidity", None),
        "timestamp": ts,
    }

def _template_exists(name: str) -> bool:
    return os.path.exists(os.path.join(app.template_folder or "templates", name))

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/health")
def health():
    return "ok", 200

@app.route("/")
def home():
    # Prefer the real template if present; otherwise show a simple fallback page.
    if _template_exists("index.html"):
        return render_template("index.html")
    return (
        """
        <html>
          <head><title>Air Quality Monitoring</title></head>
          <body style="font-family:system-ui, sans-serif">
            <h1>Air Quality Monitoring</h1>
            <p>Try the API: <a href="/api/realtime-data" target="_blank">/api/realtime-data</a></p>
            <p>Metrics: <a href="/metrics" target="_blank">/metrics</a></p>
          </body>
        </html>
        """,
        200,
    )

@app.route("/api/realtime-data")
def realtime_data():
    t0 = time.time()
    status = 200
    try:
        session = get_session()
        # CQL can't ORDER BY arbitrary columns without partition key, so fetch and sort in Python.
        stmt = SimpleStatement(
            f"SELECT id, co2, humidity, pm10, pm25, temperature,timestamp "
            f"FROM {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} LIMIT 100;"
        )
        rows = session.execute(stmt)
        payload: List[Dict[str, Any]] = [row_to_dict(r) for r in rows]
        payload.sort(key=lambda r: r.get("timestamp") or "", reverse=True)
        return jsonify(payload), 200
    except Exception as e:
        log.error("Error in /api/realtime-data: %s", e, exc_info=True)
        status = 500
        return jsonify({"error": str(e)}), 500
    finally:
        record_metrics("/api/realtime-data", "GET", str(status), time.time() - t0)

@app.route("/metrics")
def metrics():
    if not _HAS_METRICS:
        return "prometheus_client not installed", 404
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

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

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
