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

from prometheus_metrics import pm25_gauge, co2_gauge

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
        stmt = SimpleStatement(
            f"SELECT id, co2, humidity, pm10, pm25, temperature, timestamp "
            f"FROM {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} LIMIT 100;"
        )
        rows = session.execute(stmt)
        payload: List[Dict[str, Any]] = [row_to_dict(r) for r in rows]
        payload.sort(key=lambda r: r.get("timestamp") or "", reverse=True)

        # 🔑 Update Prometheus gauges for *all* rows
        for row in payload:
            sensor_id = str(row.get("id", "unknown"))

            pm25_val = row.get("pm25")
            co2_val = row.get("co2")

            pm25_gauge.labels(sensor_id=sensor_id).set(float(pm25_val) if pm25_val is not None else 0.0)
            co2_gauge.labels(sensor_id=sensor_id).set(float(co2_val) if co2_val is not None else 0.0)

        return jsonify(payload), 200

    except Exception as e:
        log.error("Error in /api/realtime-data: %s", e, exc_info=True)
        status = 500
        return jsonify({"error": str(e)}), 500
    finally:
        record_metrics("/api/realtime-data", "GET", str(status), time.time() - t0)

@app.route("/api/daily-averages")
def get_daily_averages():
    """Return structured daily averages for today and yesterday plus optional last_n_days.

    Response shape:
      {
        "today": {"pm25": .., "pm10": .., "co2": .., "temperature": .., "humidity": ..},
        "yesterday": { ... },
        "last_n_days": [{"day": "YYYY-MM-DD", "pm25": .., ...}, ...]
      }
    """
    try:
        session = get_session()

        # default range: yesterday..today
        today_dt = datetime.utcnow().date()
        yesterday_dt = today_dt - timedelta(days=1)

        start = datetime.combine(yesterday_dt, datetime.min.time())
        end = datetime.combine(today_dt + timedelta(days=1), datetime.min.time())

        query = SimpleStatement(f"""
            SELECT toDate(toTimestamp(window_start)) AS day,
                   avg(avg_pm25) AS pm25,
                   avg(avg_pm10) AS pm10,
                   avg(avg_co2) AS co2,
                   avg(avg_temp) AS temperature,
                   avg(avg_humidity) AS humidity
            FROM {CASSANDRA_KEYSPACE}.sensor_aggregates
            WHERE window_start >= minTimeuuid(%s) AND window_start < maxTimeuuid(%s)
            GROUP BY day
            ALLOW FILTERING;
        """)

        try:
            rows = session.execute(query, (start, end))

            # Map rows to dict keyed by ISO date
            day_map: Dict[str, Dict[str, Any]] = {}
            for r in rows:
                try:
                    day_key = r.day.isoformat() if hasattr(r, 'day') and r.day is not None else str(r.day)
                except Exception:
                    day_key = str(r.day)

                day_map[day_key] = {
                    'pm25': float(r.pm25) if getattr(r, 'pm25', None) is not None else None,
                    'pm10': float(r.pm10) if getattr(r, 'pm10', None) is not None else None,
                    'co2': float(r.co2) if getattr(r, 'co2', None) is not None else None,
                    'temperature': float(r.temperature) if getattr(r, 'temperature', None) is not None else None,
                    'humidity': float(r.humidity) if getattr(r, 'humidity', None) is not None else None,
                }

        except Exception as e:
            # Handle type mismatch when window_start is a timestamp rather than timeuuid
            log.warning('Primary daily-averages query failed (%s), trying timestamp-based fallback', e)

            # Fallback: select raw rows in the range and aggregate per-day in Python
            fallback_query = SimpleStatement(f"""
                SELECT window_start,
                       avg_pm25,
                       avg_pm10,
                       avg_co2,
                       avg_temp,
                       avg_humidity
                FROM {CASSANDRA_KEYSPACE}.sensor_aggregates
                WHERE window_start >= %s AND window_start < %s
                ALLOW FILTERING;
            """)

            rows = session.execute(fallback_query, (start, end))

            # Aggregate per day: sum/count -> average for each metric
            accum: Dict[str, Dict[str, Any]] = {}
            for r in rows:
                ws = getattr(r, 'window_start', None)
                if ws is None:
                    day_key = 'unknown'
                else:
                    try:
                        day_key = ws.date().isoformat()
                    except Exception:
                        day_key = str(ws)

                if day_key not in accum:
                    accum[day_key] = {
                        'pm25_sum': 0.0, 'pm25_count': 0,
                        'pm10_sum': 0.0, 'pm10_count': 0,
                        'co2_sum': 0.0, 'co2_count': 0,
                        'temp_sum': 0.0, 'temp_count': 0,
                        'hum_sum': 0.0, 'hum_count': 0,
                    }

                if getattr(r, 'avg_pm25', None) is not None:
                    accum[day_key]['pm25_sum'] += float(r.avg_pm25)
                    accum[day_key]['pm25_count'] += 1
                if getattr(r, 'avg_pm10', None) is not None:
                    accum[day_key]['pm10_sum'] += float(r.avg_pm10)
                    accum[day_key]['pm10_count'] += 1
                if getattr(r, 'avg_co2', None) is not None:
                    accum[day_key]['co2_sum'] += float(r.avg_co2)
                    accum[day_key]['co2_count'] += 1
                if getattr(r, 'avg_temp', None) is not None:
                    accum[day_key]['temp_sum'] += float(r.avg_temp)
                    accum[day_key]['temp_count'] += 1
                if getattr(r, 'avg_humidity', None) is not None:
                    accum[day_key]['hum_sum'] += float(r.avg_humidity)
                    accum[day_key]['hum_count'] += 1

            # Build day_map with computed averages
            day_map = {}
            for day_key, v in accum.items():
                def avg(sum_key, count_key):
                    return (v[sum_key] / v[count_key]) if v[count_key] > 0 else None

                day_map[day_key] = {
                    'pm25': avg('pm25_sum', 'pm25_count'),
                    'pm10': avg('pm10_sum', 'pm10_count'),
                    'co2': avg('co2_sum', 'co2_count'),
                    'temperature': avg('temp_sum', 'temp_count'),
                    'humidity': avg('hum_sum', 'hum_count'),
                }

        today_key = today_dt.isoformat()
        yesterday_key = yesterday_dt.isoformat()

        response = {
            'today': day_map.get(today_key, {'pm25': None, 'pm10': None, 'co2': None, 'temperature': None, 'humidity': None}),
            'yesterday': day_map.get(yesterday_key, {'pm25': None, 'pm10': None, 'co2': None, 'temperature': None, 'humidity': None}),
            'last_n_days': [
                {'day': d, **day_map[d]} for d in sorted(day_map.keys())
            ]
        }

        return jsonify(response)
    except Exception as e:
        log.exception('Error in /api/daily-averages: %s', e)
        return jsonify({'error': str(e)}), 500

@app.route("/metrics")
def metrics():
    if not _HAS_METRICS:
        return "prometheus_client not installed", 404
    return generate_latest(registry), 200, {"Content-Type": CONTENT_TYPE_LATEST}


@app.route('/api/export-csv')
def export_csv():
    """Export sensor readings as CSV for a given time range.

    Query params:
      start: ISO timestamp or date (inclusive)
      end: ISO timestamp or date (exclusive)
      sensor_id: optional sensor id to filter
    """
    try:
        start_str = request.args.get('start')
        end_str = request.args.get('end')
        sensor_id = request.args.get('sensor_id')

        if not start_str or not end_str:
            return jsonify({'error': 'start and end parameters required (ISO format)'}), 400

        try:
            start = datetime.fromisoformat(start_str)
        except Exception:
            start = datetime.strptime(start_str, '%Y-%m-%d')

        try:
            end = datetime.fromisoformat(end_str)
        except Exception:
            end = datetime.strptime(end_str, '%Y-%m-%d') + timedelta(days=1)

        session = get_session()

        # Try to query using timestamp bounds first (handles timestamp column)
        try:
            q = f"SELECT sensor_id, pm25, pm10, co2, temperature, humidity, timestamp FROM {CASSANDRA_KEYSPACE}.sensor_data WHERE timestamp >= %s AND timestamp < %s"
            params = [start, end]
            if sensor_id:
                q += " AND sensor_id = %s"
                params.append(sensor_id)
            q += " LIMIT 10000;"
            stmt = SimpleStatement(q)
            rows = session.execute(stmt, tuple(params))
        except Exception:
            # Fall back to scanning a table with ALLOW FILTERING if necessary
            q = f"SELECT sensor_id, pm25, pm10, co2, temperature, humidity, timestamp FROM {CASSANDRA_KEYSPACE}.sensor_data WHERE timestamp >= %s AND timestamp < %s ALLOW FILTERING"
            params = (start, end)
            if sensor_id:
                q += " AND sensor_id = %s"
                params = (start, end, sensor_id)
            stmt = SimpleStatement(q)
            rows = session.execute(stmt, params)

        # Build CSV lines
        import io, csv
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(['sensor_id', 'timestamp', 'pm25', 'pm10', 'co2', 'temperature', 'humidity'])
        for r in rows:
            ts = getattr(r, 'timestamp', None)
            if hasattr(ts, 'isoformat'):
                ts = ts.isoformat()
            writer.writerow([
                getattr(r, 'sensor_id', None) or '',
                ts or '',
                getattr(r, 'pm25', ''),
                getattr(r, 'pm10', ''),
                getattr(r, 'co2', ''),
                getattr(r, 'temperature', ''),
                getattr(r, 'humidity', ''),
            ])

        csv_data = buf.getvalue()
        buf.close()

        from flask import Response
        resp = Response(csv_data, mimetype='text/csv')
        resp.headers['Content-Disposition'] = f'attachment; filename=air_quality_{start.date().isoformat()}_to_{end.date().isoformat()}.csv'
        return resp
    except Exception as e:
        log.exception('Error exporting CSV: %s', e)
        return jsonify({'error': str(e)}), 500

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
        
        # Time the future prediction. Some deployments may have an older predictor
        # implementation that doesn't expose `predict_future`. Add a safe fallback
        # which iteratively calls predictor.predict to produce hour-by-hour results.
        with TimerContextManager(model_prediction_duration, {'model_type': 'random_forest'}):
            if hasattr(predictor, 'predict_future') and callable(getattr(predictor, 'predict_future')):
                predictions = predictor.predict_future(data, hours_ahead=hours_ahead)
            else:
                # Fallback iterative predictor
                try:
                    base = data.copy() if isinstance(data, dict) else dict(data)
                except Exception:
                    base = dict(data)

                # ensure timestamp exists and is a datetime
                ts = base.get('timestamp')
                if ts is None:
                    base_time = datetime.utcnow()
                else:
                    try:
                        base_time = datetime.fromisoformat(ts) if isinstance(ts, str) else ts
                    except Exception:
                        base_time = datetime.utcnow()

                fallback_preds = []
                for i in range(1, int(hours_ahead) + 1):
                    future_time = base_time + timedelta(hours=i)
                    pd_input = base.copy()
                    pd_input['timestamp'] = future_time
                    # update time-based features if model expects them
                    pd_input['hour'] = future_time.hour
                    pd_input['day'] = future_time.day
                    pd_input['month'] = future_time.month
                    pd_input['day_of_week'] = future_time.weekday()

                    # call predictor.predict (may return array or scalar)
                    raw = predictor.predict(pd_input)
                    try:
                        val = float(raw)
                    except Exception:
                        import numpy as _np
                        val = float(_np.asarray(raw).ravel()[0])

                    val = max(0.0, val)
                    fallback_preds.append({
                        'timestamp': future_time,
                        'predicted_pm25': val
                    })

                    # update base with last prediction for simple lag handling
                    try:
                        base['pm25'] = val
                        if 'pm10' in base:
                            base['pm10'] = val * 1.5
                        base['timestamp'] = future_time
                    except Exception:
                        pass

                predictions = fallback_preds

        # Record successful predictions
        model_prediction_counter.labels(model_type='random_forest', status='success').inc(hours_ahead)

        formatted_predictions = [
            {
                "timestamp": pred["timestamp"].isoformat(),
                "predicted_pm25": float(pred["predicted_pm25"])
            }
            for pred in predictions
        ]

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
