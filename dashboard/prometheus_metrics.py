from prometheus_client import Counter, Histogram, Gauge, Summary, CollectorRegistry
import time
import threading
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create a registry
registry = CollectorRegistry()

# API request metrics
http_request_counter = Counter(
    'air_quality_http_requests_total',
    'Total number of HTTP requests',
    ['method', 'endpoint', 'status'],
    registry=registry
)

http_request_duration = Histogram(
    'air_quality_http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint'],
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30],
    registry=registry
)

# Model metrics
model_prediction_counter = Counter(
    'air_quality_model_predictions_total',
    'Total number of model predictions',
    ['model_type', 'status'],
    registry=registry
)

model_prediction_duration = Histogram(
    'air_quality_model_prediction_duration_seconds',
    'Model prediction duration in seconds',
    ['model_type'],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2.5, 5],
    registry=registry
)

model_training_duration = Histogram(
    'air_quality_model_training_duration_seconds',
    'Model training duration in seconds',
    ['model_type', 'data_source'],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800],
    registry=registry
)

model_accuracy = Gauge(
    'air_quality_model_accuracy',
    'Model accuracy metrics',
    ['model_type', 'metric_name'],
    registry=registry
)

# Air quality sensor values
pm25_gauge = Gauge(
    'air_quality_pm25',
    'Latest PM2.5 value in µg/m³',
    ['sensor_id'],
    registry=registry
)

co2_gauge = Gauge(
    'air_quality_co2',
    'Latest CO₂ concentration in ppm',
    ['sensor_id'],
    registry=registry
)

# Database metrics
db_query_counter = Counter(
    'air_quality_db_queries_total',
    'Total number of database queries',
    ['query_type', 'status'],
    registry=registry
)

db_query_duration = Histogram(
    'air_quality_db_query_duration_seconds',
    'Database query duration in seconds',
    ['query_type'],
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30],
    registry=registry
)

db_row_count = Histogram(
    'air_quality_db_row_count',
    'Number of rows returned by database queries',
    ['query_type'],
    buckets=[0, 1, 10, 100, 1000, 10000],
    registry=registry
)

# System metrics
memory_usage = Gauge(
    'air_quality_memory_usage_bytes',
    'Memory usage in bytes',
    ['process_type'],
    registry=registry
)

cpu_usage = Gauge(
    'air_quality_cpu_usage_percent',
    'CPU usage percentage',
    ['process_type'],
    registry=registry
)

# Helper class for timing operations
class TimerContextManager:
    """Context manager for timing code blocks and recording to Prometheus"""
    
    def __init__(self, histogram, labels=None):
        self.histogram = histogram
        self.labels = labels or {}
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        if isinstance(self.histogram, Histogram):
            self.histogram.labels(**self.labels).observe(duration)
        return False  # Don't suppress exceptions

# Function to collect system metrics
def collect_system_metrics(interval=15):
    """Collect system metrics in a background thread"""
    try:
        import psutil
        
        def _collect_metrics():
            while True:
                try:
                    # Get process info
                    process = psutil.Process()
                    
                    # Memory usage
                    memory_info = process.memory_info()
                    memory_usage.labels(process_type='flask_app').set(memory_info.rss)
                    
                    # CPU usage
                    cpu_percent = process.cpu_percent(interval=1)
                    cpu_usage.labels(process_type='flask_app').set(cpu_percent)
                    
                    # System-wide metrics
                    memory_usage.labels(process_type='system').set(psutil.virtual_memory().used)
                    cpu_usage.labels(process_type='system').set(psutil.cpu_percent())
                    
                    # Wait for next collection
                    time.sleep(interval)
                except Exception as e:
                    logger.error(f"Error collecting system metrics: {e}")
                    time.sleep(interval)
        
        # Start collection in a daemon thread
        metrics_thread = threading.Thread(target=_collect_metrics, daemon=True)
        metrics_thread.start()
        
        logger.info("System metrics collection started")
        
    except ImportError:
        logger.warning("psutil not installed. System metrics will not be collected.")
        logger.warning("Install with: pip install psutil")

# Middleware for Flask request monitoring
class RequestMonitoringMiddleware:
    """Middleware to monitor Flask requests"""
    
    def __init__(self, app):
        self.app = app
        # Store request start time in Flask g object
        self.app.before_request(self.before_request)
        self.app.after_request(self.after_request)
    
    def before_request(self):
        from flask import g
        g.start_time = time.time()
    
    def after_request(self, response):
        from flask import request, g
        
        # Measure request duration
        duration = time.time() - getattr(g, 'start_time', time.time())
        
        # Record metrics
        endpoint = request.endpoint or 'unknown'
        status_code = str(response.status_code)
        
        http_request_counter.labels(
            method=request.method,
            endpoint=endpoint,
            status=status_code
        ).inc()
        
        http_request_duration.labels(
            method=request.method,
            endpoint=endpoint
        ).observe(duration)
        
        return response

def start_metrics_collection():
    """Start all metrics collection processes"""
    collect_system_metrics()
    logger.info("Started metrics collection")
