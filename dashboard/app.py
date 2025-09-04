import os
import time
import logging
from typing import List, Dict, Any

from flask import Flask, jsonify, render_template

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
            f"SELECT id, pm25, pm10, co2, temperature, humidity, timestamp "
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

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
