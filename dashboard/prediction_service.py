import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
import joblib
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging
import os
import json
import argparse
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cassandra connection parameters
CASSANDRA_HOST = ['cassandra']
CASSANDRA_PORT = 9042
CASSANDRA_USERNAME = '' 
CASSANDRA_PASSWORD = ''  
CASSANDRA_KEYSPACE = 'air_quality_monitoring'
CASSANDRA_TABLE = 'sensor_data'

class AirQualityPredictor:
    """Unified class for air quality prediction model training and inference"""
    
    def __init__(self, model_path="models/air_quality_model.joblib", 
                 scaler_path="models/air_quality_scaler.joblib"):
        """Initialize the AI system with model and scaler"""
        self.model_path = model_path
        self.scaler_path = scaler_path
        self.model = None
        self.scaler = None
        self.metrics = None
        self.training_timestamp = None
        
        self.load_model()
    
    def load_model(self):
        """Load the trained model and scaler"""
        try:
            if os.path.exists(self.model_path) and os.path.exists(self.scaler_path):
                self.model = joblib.load(self.model_path)
                self.scaler = joblib.load(self.scaler_path)
                self.training_timestamp = datetime.fromtimestamp(
                    os.path.getmtime(self.model_path)
                ).isoformat()
                
                # Try to load metrics if available
                metrics_path = os.path.join(os.path.dirname(self.model_path), 'training_metrics.json')
                if os.path.exists(metrics_path):
                    with open(metrics_path, 'r') as f:
                        self.metrics = json.load(f)
                
                logger.info(f"Model loaded successfully from {self.model_path}")
                return True
            else:
                logger.warning("No existing model found. Model needs to be trained.")
                return False
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            return False
    
    def connect_to_cassandra(self):
        """Establish connection to Cassandra database"""
        try:
            if CASSANDRA_USERNAME and CASSANDRA_PASSWORD:
                auth_provider = PlainTextAuthProvider(username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD)
                cluster = Cluster(CASSANDRA_HOST, port=CASSANDRA_PORT, auth_provider=auth_provider)
            else:
                cluster = Cluster(CASSANDRA_HOST, port=CASSANDRA_PORT)
            
            session = cluster.connect(CASSANDRA_KEYSPACE)
            logger.info("Connected to Cassandra database")
            return session
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise
    
    def fetch_data(self, session, start_date=None, end_date=None):
        """Fetch air quality data from Cassandra"""
        try:
            # Adjust query based on your table structure
            query = f"SELECT sensor_id, timestamp, temperature, humidity, pm25, pm10, co2, tvoc FROM {CASSANDRA_TABLE}"
            
            # Add date range if specified
            if start_date and end_date:
                query += f" WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'"
            
            rows = session.execute(query)
            
            # Convert to pandas DataFrame
            data = []
            for row in rows:
                data.append({
                    'sensor_id': row.sensor_id,
                    'timestamp': row.timestamp,
                    'temperature': row.temperature,
                    'humidity': row.humidity,
                    'pm25': row.pm25,
                    'pm10': row.pm10,
                    'co2': row.co2,
                    'tvoc': row.tvoc
                })
            
            df = pd.DataFrame(data)
            logger.info(f"Retrieved {len(df)} records from Cassandra")
            return df
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise
    
    def create_dummy_data(self, n_samples=1000):
        """Create synthetic data for testing when Cassandra is not available"""
        logger.info("Creating dummy data for training")
        
        np.random.seed(42)
        
        # Generate timestamps for the last 30 days, every hour
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        timestamps = [start_date + timedelta(hours=i) for i in range(30*24)][:n_samples]
        
        # Generate synthetic features
        temperature = np.random.normal(25, 5, n_samples)  # mean 25°C, std 5°C
        humidity = np.random.normal(60, 15, n_samples)    # mean 60%, std 15%
        co2 = np.random.normal(450, 100, n_samples)       # mean 450ppm, std 100ppm
        tvoc = np.random.normal(200, 50, n_samples)       # mean 200ppb, std 50ppb
        
        # Create target variable (PM2.5) with some relationship to the features
        pm25 = (
            0.5 * temperature + 
            -0.3 * humidity + 
            0.2 * co2/100 + 
            0.1 * tvoc/10 + 
            np.random.normal(0, 5, n_samples)  # Add some noise
        )
        # Ensure PM2.5 is positive
        pm25 = np.maximum(pm25, 0)
        
        # Create PM10 (usually higher than PM2.5)
        pm10 = pm25 * 1.5 + np.random.normal(0, 3, n_samples)
        pm10 = np.maximum(pm10, 0)
        
        # Create a DataFrame
        df = pd.DataFrame({
            'sensor_id': ['dummy_sensor'] * n_samples,
            'timestamp': timestamps,
            'temperature': temperature,
            'humidity': humidity,
            'co2': co2,
            'tvoc': tvoc,
            'pm25': pm25,
            'pm10': pm10
        })
        
        logger.info(f"Created {len(df)} dummy records")
        return df
    
    def preprocess_data(self, df):
        """Preprocess the data for model training"""
        try:
            # Handle missing values
            df = df.dropna()
            
            # Convert timestamp to datetime features
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['timestamp'].dt.hour
            df['day'] = df['timestamp'].dt.day
            df['month'] = df['timestamp'].dt.month
            df['day_of_week'] = df['timestamp'].dt.dayofweek
            
            # Create feature matrix X and target variable y
            # Assuming we want to predict pm25 levels based on other factors
            X = df[['temperature', 'humidity', 'co2', 'tvoc', 'hour', 'day', 'month', 'day_of_week']]
            y = df['pm25']  # Target variable
            
            # Convert sensor_id to numeric if needed
            if 'sensor_id' in X.columns and X['sensor_id'].dtype == 'object':
                X['sensor_id'] = pd.factorize(X['sensor_id'])[0]
                
            # Split the data into training and testing sets
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Feature scaling
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            logger.info("Data preprocessing completed")
            return X_train_scaled, X_test_scaled, y_train, y_test, scaler
        except Exception as e:
            logger.error(f"Error preprocessing data: {e}")
            raise
    
    def train_model(self, X_train, y_train):
        """Train a machine learning model"""
        try:
            # Create and train the model
            model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
            
            logger.info("Training model...")
            model.fit(X_train, y_train)
            logger.info("Model training completed")
            return model
        except Exception as e:
            logger.error(f"Error training model: {e}")
            raise
    
    def evaluate_model(self, model, X_test, y_test):
        """Evaluate the trained model"""
        try:
            # Make predictions
            y_pred = model.predict(X_test)
            
            # Calculate metrics
            mse = mean_squared_error(y_test, y_pred)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_test, y_pred)
            
            logger.info(f"Model Evaluation Metrics:")
            logger.info(f"Mean Squared Error: {mse:.4f}")
            logger.info(f"Root Mean Squared Error: {rmse:.4f}")
            logger.info(f"R² Score: {r2:.4f}")
            
            return mse, rmse, r2
        except Exception as e:
            logger.error(f"Error evaluating model: {e}")
            raise
    
    def save_model(self, model, scaler, metrics=None):
        """Save the trained model and scaler"""
        try:
            # Create a directory for models if it doesn't exist
            os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
            
            # Save the model and scaler (using specified paths)
            joblib.dump(model, self.model_path)
            joblib.dump(scaler, self.scaler_path)
            
            # Save training timestamp
            self.training_timestamp = datetime.now().isoformat()
            
            # Save metrics if provided
            if metrics:
                self.metrics = metrics
                metrics_path = os.path.join(os.path.dirname(self.model_path), 'training_metrics.json')
                with open(metrics_path, 'w') as f:
                    json.dump(metrics, f)
            
            logger.info(f"Model saved to {self.model_path}")
            logger.info(f"Scaler saved to {self.scaler_path}")
            
            return self.model_path, self.scaler_path
        except Exception as e:
            logger.error(f"Error saving model: {e}")
            raise
    
    def train(self, data_source='cassandra', days=30):
        """Main function to orchestrate the training process"""
        try:
            # Get data based on source
            if data_source == 'cassandra':
                # Connect to Cassandra
                session = self.connect_to_cassandra()
                
                # Fetch data for the specified number of days
                end_date = datetime.now()
                start_date = end_date - timedelta(days=int(days))
                df = self.fetch_data(session, start_date, end_date)
                
                # Close Cassandra connection
                session.cluster.shutdown()
            else:
                # Use dummy data for testing
                df = self.create_dummy_data()
            
            if len(df) == 0:
                logger.warning("No data retrieved. Aborting training.")
                return None
            
            # Preprocess data
            X_train, X_test, y_train, y_test, scaler = self.preprocess_data(df)
            
            # Train model
            model = self.train_model(X_train, y_train)
            
            # Evaluate model
            mse, rmse, r2 = self.evaluate_model(model, X_test, y_test)
            
            # Save metrics
            metrics = {
                "mse": float(mse),
                "rmse": float(rmse),
                "r2_score": float(r2)
            }
            
            # Save model
            self.save_model(model, scaler, metrics)
            
            # Update instance variables
            self.model = model
            self.scaler = scaler
            
            logger.info("Model training pipeline completed successfully")
            return metrics
        
        except Exception as e:
            logger.error(f"Error in training pipeline: {e}")
            return None
    
    def preprocess_input(self, data):
        """Preprocess input data for prediction"""
        try:
            # Convert input to DataFrame if it's a dictionary
            if isinstance(data, dict):
                data = pd.DataFrame([data])
            
            # Extract datetime features if timestamp exists
            if 'timestamp' in data.columns:
                data['timestamp'] = pd.to_datetime(data['timestamp'])
                data['hour'] = data['timestamp'].dt.hour
                data['day'] = data['timestamp'].dt.day
                data['month'] = data['timestamp'].dt.month
                data['day_of_week'] = data['timestamp'].dt.dayofweek
            else:
                # Use current time if no timestamp provided
                now = datetime.now()
                data['hour'] = now.hour
                data['day'] = now.day
                data['month'] = now.month
                data['day_of_week'] = now.weekday()
            
            # Select only the features the model was trained on
            features = ['temperature', 'humidity', 'co2', 'tvoc', 
                       'hour', 'day', 'month', 'day_of_week']
            
            # Make sure all required features exist
            for feature in features:
                if feature not in data.columns:
                    if feature not in ['hour', 'day', 'month', 'day_of_week']:
                        data[feature] = 0  # Default value for missing features
            
            # Extract features used by the model
            X = data[features]
            
            # Scale features
            X_scaled = self.scaler.transform(X)
            
            return X_scaled
        
        except Exception as e:
            logger.error(f"Error preprocessing data: {e}")
            raise
    
    def predict(self, data):
        """Make predictions using the trained model"""
        try:
            if self.model is None:
                raise ValueError("Model not loaded. Please train or load a model first.")
                
            # Preprocess the input data
            X_scaled = self.preprocess_input(data)
            
            # Make prediction
            predictions = self.model.predict(X_scaled)
            
            return predictions[0] if len(predictions) == 1 else predictions
        
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            raise
    
    def predict_future(self, current_data, hours_ahead=24):
        """Predict air quality for future hours based on current data"""
        try:
            if self.model is None:
                raise ValueError("Model not loaded. Please train or load a model first.")
                
            # Create a copy of the current data
            if isinstance(current_data, dict):
                base_data = current_data.copy()
                if 'timestamp' not in base_data:
                    base_data['timestamp'] = datetime.now()
            else:
                base_data = current_data.iloc[0].to_dict()
                if 'timestamp' not in base_data:
                    base_data['timestamp'] = datetime.now()
            
            # Convert timestamp to datetime if it's a string
            if isinstance(base_data['timestamp'], str):
                base_data['timestamp'] = pd.to_datetime(base_data['timestamp'])
            
            # Generate predictions for future hours
            future_predictions = []
            current_time = base_data['timestamp']
            
            for i in range(hours_ahead):
                future_time = current_time + pd.Timedelta(hours=i)
                prediction_data = base_data.copy()
                prediction_data['timestamp'] = future_time
                
                # Make prediction
                predicted_value = self.predict(prediction_data)
                
                future_predictions.append({
                    'timestamp': future_time,
                    'predicted_pm25': predicted_value
                })
            
            return future_predictions
        
        except Exception as e:
            logger.error(f"Error predicting future values: {e}")
            raise
    
    def get_model_info(self):
        """Get information about the current model"""
        if self.model is None:
            return {
                "status": "not_loaded",
                "timestamp": None,
                "accuracy": None,
                "metrics": None
            }
        
        return {
            "status": "loaded",
            "timestamp": self.training_timestamp,
            "accuracy": self.metrics.get("r2_score") if self.metrics else None,
            "metrics": self.metrics
        }


# For command-line usage
def parse_args():
    parser = argparse.ArgumentParser(description='Air Quality AI System')
    parser.add_argument('--mode', type=str, choices=['train', 'predict'], 
                        default='train', help='Mode to run (train or predict)')
    parser.add_argument('--data_source', type=str, default='cassandra', 
                        help='Data source (cassandra or dummy)')
    parser.add_argument('--days', type=int, default=30, 
                        help='Number of days of data to use for training')
    parser.add_argument('--model_path', type=str, default='models/air_quality_model.joblib',
                        help='Path to save/load model')
    parser.add_argument('--scaler_path', type=str, default='models/air_quality_scaler.joblib',
                        help='Path to save/load scaler')
    return parser.parse_args()


# Example usage
if __name__ == "__main__":
    args = parse_args()
    
    # Create the AI system
    air_quality_ai = AirQualityAI(
        model_path=args.model_path,
        scaler_path=args.scaler_path
    )
    
    if args.mode == 'train':
        # Train the model
        metrics = air_quality_ai.train(data_source=args.data_source, days=args.days)
        if metrics:
            print(f"Training completed successfully. R² Score: {metrics['r2_score']:.4f}")
        else:
            print("Training failed.")
    
    elif args.mode == 'predict':
        # Make a prediction with sample data
        sample_data = {
            'temperature': 25.0,
            'humidity': 60.0,
            'co2': 450.0,
            'tvoc': 200.0,
            'timestamp': datetime.now()
        }
        
        try:
            # Make a prediction
            prediction = air_quality_ai.predict(sample_data)
            print(f"Predicted PM2.5: {prediction:.2f}")
            
            # Predict for the next 24 hours
            future_predictions = air_quality_ai.predict_future(sample_data, hours_ahead=24)
            for pred in future_predictions:
                print(f"Time: {pred['timestamp']}, Predicted PM2.5: {pred['predicted_pm25']:.2f}")
        
        except Exception as e:
            print(f"Error making prediction: {e}")
