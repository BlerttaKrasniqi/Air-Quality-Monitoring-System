import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from sklearn.preprocessing import StandardScaler, RobustScaler, PolynomialFeatures
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.base import BaseEstimator, TransformerMixin
import joblib
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging
import os
import json
import argparse
from datetime import datetime, timedelta
from prometheus_metrics import (
    db_query_counter, db_query_duration, db_row_count,
    model_training_duration, model_accuracy, model_prediction_duration,
    TimerContextManager
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cassandra connection parameters
CASSANDRA_HOST = ['cassandra']
CASSANDRA_PORT = 9042
CASSANDRA_USERNAME = ''
CASSANDRA_PASSWORD = ''  
CASSANDRA_KEYSPACE = 'air_monitoring'
CASSANDRA_TABLE = 'sensor_data'

# Custom transformer for time-based features
class TimeFeatureExtractor(BaseEstimator, TransformerMixin):
    """Extract advanced time-based features from timestamp"""
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        X_copy = X.copy()
        
        # Ensure timestamp is in datetime format
        if 'timestamp' in X_copy.columns:
            X_copy['timestamp'] = pd.to_datetime(X_copy['timestamp'])
            
            # Basic time features
            X_copy['hour'] = X_copy['timestamp'].dt.hour
            X_copy['day'] = X_copy['timestamp'].dt.day
            X_copy['month'] = X_copy['timestamp'].dt.month
            X_copy['day_of_week'] = X_copy['timestamp'].dt.dayofweek
            
            # Advanced time features
            X_copy['is_weekend'] = X_copy['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)
            X_copy['is_night'] = X_copy['hour'].apply(lambda x: 1 if (x >= 22 or x <= 6) else 0)
            X_copy['hour_sin'] = np.sin(2 * np.pi * X_copy['hour'] / 24)
            X_copy['hour_cos'] = np.cos(2 * np.pi * X_copy['hour'] / 24)
            X_copy['month_sin'] = np.sin(2 * np.pi * X_copy['month'] / 12)
            X_copy['month_cos'] = np.cos(2 * np.pi * X_copy['month'] / 12)
            
            # Drop the original timestamp as it's not useful for the model
            X_copy.drop('timestamp', axis=1, inplace=True)
            
        return X_copy

# Custom transformer for weather-based features
class WeatherFeatureExtractor(BaseEstimator, TransformerMixin):
    """Extract advanced weather-based features"""
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        X_copy = X.copy()
        
        # Combine temperature and humidity for heat index
        if 'temperature' in X_copy.columns and 'humidity' in X_copy.columns:
            # Simplified heat index formula
            X_copy['heat_index'] = X_copy['temperature'] + 0.05 * X_copy['humidity']
            
            # Dew point approximation
            X_copy['dew_point'] = X_copy['temperature'] - ((100 - X_copy['humidity']) / 5)
            
            # Temperature-humidity ratio
            X_copy['temp_humidity_ratio'] = X_copy['temperature'] / (X_copy['humidity'] + 1)  # +1 to avoid division by zero
            
        # CO2 interaction features
        if 'co2' in X_copy.columns:
            X_copy['co2_norm'] = X_copy['co2'] / 400  # Normalize by baseline atmospheric CO2
            
            if 'temperature' in X_copy.columns:
                X_copy['co2_temp_interaction'] = X_copy['co2'] * X_copy['temperature'] / 1000  # Scale down
                
        return X_copy

# Custom transformer for detecting and handling outliers
class OutlierRemover(BaseEstimator, TransformerMixin):
    """Detect and handle outliers in the data"""

    def __init__(self, threshold=3.0):
        self.threshold = threshold
        self.means = None
        self.stds = None
        self.numeric_cols = None  # store numeric columns only

    def fit(self, X, y=None):
        # Identify numeric columns
        self.numeric_cols = X.select_dtypes(include=[np.number]).columns
        # Calculate means and standard deviations for numeric columns only
        self.means = X[self.numeric_cols].mean()
        self.stds = X[self.numeric_cols].std()
        return self

    def transform(self, X):
        X_copy = X.copy()

        logger.info(f"Columns selected for outlier removal: {self.numeric_cols.tolist()}")

        for col in self.numeric_cols:
            if self.stds[col] > 0:
                z_scores = np.abs((X_copy[col] - self.means[col]) / self.stds[col])
                outliers = z_scores > self.threshold

                if outliers.any():
                    logger.info(f"Clipping {outliers.sum()} outliers in column '{col}'")

                # Instead of dropping rows, just clip the values
                X_copy.loc[outliers & (X_copy[col] > self.means[col]), col] = (
                    self.means[col] + self.threshold * self.stds[col]
                )
                X_copy.loc[outliers & (X_copy[col] < self.means[col]), col] = (
                    self.means[col] - self.threshold * self.stds[col]
                )

        return X_copy


class AirQualityPredictor:
    """Unified class for air quality prediction model training and inference"""
   
    def __init__(self, model_path="models/air_quality_model.joblib",
                 scaler_path="models/air_quality_scaler.joblib",
                 preprocessor_path="models/air_quality_preprocessor.joblib",
                 feature_importance_path="models/feature_importance.png"):
        """Initialize the AI system with model and scaler"""
        self.model_path = model_path
        self.scaler_path = scaler_path
        self.preprocessor_path = preprocessor_path
        self.feature_importance_path = feature_importance_path
        self.model = None
        self.scaler = None
        self.preprocessor = None
        self.feature_names = None
        self.metrics = None
        self.training_timestamp = None
        self.test_predictions = None
        self.test_actuals = None
       
        self.load_model()
   
    def load_model(self):
        """Force load the trained model and log loudly if it fails."""
        try:
            models_dir = "/app/models"
            self.model_path = os.path.join(models_dir, "air_quality_model.joblib")
            self.scaler_path = os.path.join(models_dir, "air_quality_scaler.joblib")
            self.preprocessor_path = os.path.join(models_dir, "air_quality_preprocessor.joblib")

            logger.warning(f"🔎 Attempting to load model from: {self.model_path}")

            # Check all files exist
            for f in [self.model_path, self.scaler_path, self.preprocessor_path]:
                if not os.path.exists(f):
                    raise FileNotFoundError(f"Required file missing: {f}")

            # Load with joblib
            self.model = joblib.load(self.model_path)
            logger.warning(f"✅ Model object type: {type(self.model)}")

            self.scaler = joblib.load(self.scaler_path)
            logger.warning(f"✅ Scaler object type: {type(self.scaler)}")

            self.preprocessor = joblib.load(self.preprocessor_path)
            logger.warning(f"✅ Preprocessor object type: {type(self.preprocessor)}")

            self.training_timestamp = datetime.fromtimestamp(
                os.path.getmtime(self.model_path)
            ).isoformat()

            logger.warning("🎉 Model + Scaler + Preprocessor loaded successfully")

            return True

        except Exception as e:
            logger.error(f"❌ Critical error loading model: {e}", exc_info=True)
            self.model = None
            self.scaler = None
            self.preprocessor = None
            return False
   
    def connect_to_cassandra(self):
        """Establish connection to Cassandra database"""
        try:
            with TimerContextManager(db_query_duration, {'query_type': 'connect'}):
                if CASSANDRA_USERNAME and CASSANDRA_PASSWORD:
                    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD)
                    cluster = Cluster(CASSANDRA_HOST, port=CASSANDRA_PORT, auth_provider=auth_provider)
                else:
                    cluster = Cluster(CASSANDRA_HOST, port=CASSANDRA_PORT)
               
                session = cluster.connect(CASSANDRA_KEYSPACE)
           
            db_query_counter.labels(query_type='connect', status='success').inc()
            logger.info("Connected to Cassandra database")
            return session
        except Exception as e:
            db_query_counter.labels(query_type='connect', status='error').inc()
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise
   
    def fetch_data(self, session, start_date=None, end_date=None):
        """Fetch air quality data from Cassandra"""
        try:
            query = f"SELECT id, timestamp, temperature, humidity, pm25, pm10, co2 FROM {CASSANDRA_TABLE}"
           
            # Add date range if specified
            if start_date and end_date:
                query += f" WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}' ALLOW FILTERING"
           
            with TimerContextManager(db_query_duration, {'query_type': 'fetch_sensor_data'}):
                rows = session.execute(query)
               
                # Convert to pandas DataFrame
                data = []
                for row in rows:
                    data.append({
                        'id': row.id,
                        'timestamp': row.timestamp,
                        'temperature': row.temperature,
                        'humidity': row.humidity,
                        'pm25': row.pm25,
                        'pm10': row.pm10,
                        'co2': row.co2
                    })
               
                df = pd.DataFrame(data)
           
            # Record metrics
            db_query_counter.labels(query_type='fetch_sensor_data', status='success').inc()
            db_row_count.labels(query_type='fetch_sensor_data').observe(len(df))
           
            logger.info(f"Retrieved {len(df)} records from Cassandra")
            return df
        except Exception as e:
            db_query_counter.labels(query_type='fetch_sensor_data', status='error').inc()
            logger.error(f"Error fetching data: {e}")
            raise
   
    def create_dummy_data(self, n_samples=1000):
        """Create synthetic data for testing when Cassandra is not available"""
        logger.info("Creating dummy data for training")
       
        np.random.seed(42)
       
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        timestamps = [start_date + timedelta(hours=i) for i in range(n_samples)]
       
        # Generate synthetic features with more realistic patterns
        # Time-based patterns for temperature
        hour_of_day = [(timestamp.hour / 24) * 2 * np.pi for timestamp in timestamps]
        base_temp = 20 + 10 * np.sin(hour_of_day)  # Daily cycle: cooler at night, warmer in day
        day_of_year = [(timestamp.timetuple().tm_yday / 365) * 2 * np.pi for timestamp in timestamps]
        seasonal_effect = 5 * np.sin(day_of_year)  # Seasonal cycle
        temperature = base_temp + seasonal_effect + np.random.normal(0, 2, n_samples)
        
        # Humidity has inverse relationship with temperature + random variation
        humidity = 80 - 0.5 * (temperature - 20) + np.random.normal(0, 5, n_samples)
        humidity = np.clip(humidity, 30, 95)
        
        # CO2 with daily patterns and relationship to temperature
        co2_base = 400 + 50 * np.sin(hour_of_day)  # Higher CO2 during active hours
        co2 = co2_base + (temperature - 20) * 2 + np.random.normal(0, 20, n_samples)
        co2 = np.clip(co2, 350, 800)
        
        # Create PM2.5 with complex relationship to other variables
        # Higher PM2.5 when:
        # - Higher CO2 (activity/combustion)
        # - Higher temperature (reactions)
        # - Lower humidity (particles stay airborne)
        # - Time of day effects (traffic patterns)
        # - Weekly patterns (weekday vs weekend)
        day_of_week = [timestamp.weekday() for timestamp in timestamps]
        weekday_effect = [1.2 if day < 5 else 0.8 for day in day_of_week]  # Higher on weekdays
        
        pm25_base = (
            (co2 - 400) * 0.03 +  # CO2 effect
            (temperature - 20) * 0.2 +  # Temperature effect
            (70 - humidity) * 0.05 +  # Humidity effect (inverse)
            5 * np.sin(hour_of_day + np.pi/2)  # Time of day (peak during commute hours)
        )
        
        pm25 = pm25_base * weekday_effect + np.random.normal(0, 2, n_samples)
        pm25 = np.maximum(pm25, 0)  # Ensure non-negative
        
        # PM10 is correlated with PM2.5 but with additional factors
        pm10 = pm25 * 1.5 + np.random.normal(0, 3, n_samples)
        pm10 = np.maximum(pm10, 0)
       
        # Create a DataFrame
        df = pd.DataFrame({
            'sensor_id': ['dummy_sensor'] * n_samples,
            'timestamp': timestamps,
            'temperature': temperature,
            'humidity': humidity,
            'co2': co2,
            'pm25': pm25,
            'pm10': pm10
        })
       
        logger.info(f"Created {len(df)} dummy records with realistic patterns")
        return df
   
    def build_preprocessing_pipeline(self):
        """Build a preprocessing pipeline for feature engineering"""
        
        # Define the preprocessing steps
        time_features = TimeFeatureExtractor()
        weather_features = WeatherFeatureExtractor()
        outlier_remover = OutlierRemover(threshold=3.0)
        
        # Create a preprocessing pipeline
        preprocessing_pipeline = Pipeline([
            ('outlier_removal', outlier_remover),
            ('time_features', time_features),
            ('weather_features', weather_features)
        ])
        
        return preprocessing_pipeline
    
    def preprocess_data(self, df):
        """Preprocess the data for model training with advanced feature engineering"""
        try:
            # Handle missing values
            # Handle missing values
            for col in ['temperature', 'humidity', 'co2', 'pm25', 'pm10']:
                if col in df.columns:
                    if df[col].isnull().any():
                        if col in ['humidity', 'co2']:
                            # fill with median (robust to outliers)
                            df[col].fillna(df[col].median(), inplace=True)
                        else:
                            # temperature, pm25, pm10 → fill with mean
                            df[col].fillna(df[col].mean(), inplace=True)

            logger.info(f"Data shape after imputing NAs: {df.shape}")


            # Drop identifier columns not useful for training
            if 'id' in df.columns:
                df = df.drop(columns=['id'])
                logger.info("Dropped 'id' column before preprocessing")

            # Remove extreme outliers (5 std)
            for col in ['temperature', 'humidity', 'co2', 'pm25', 'pm10']:
                if col in df.columns:
                    mean = df[col].mean()
                    std = df[col].std()
                    lower, upper = mean - 5 * std, mean + 5 * std
                    df[col] = df[col].clip(lower, upper)

            logger.info(f"Data shape after clipping extreme outliers: {df.shape}")

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            y = df['pm25'].copy()

            preprocessor = self.build_preprocessing_pipeline()
            X_transformed = preprocessor.fit_transform(df)

            feature_names = X_transformed.columns.tolist()

            X_train, X_test, y_train, y_test = train_test_split(
                X_transformed, y, test_size=0.2, random_state=42
            )

            scaler = RobustScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)

            X_train_scaled_df = pd.DataFrame(X_train_scaled, columns=X_train.columns)
            X_test_scaled_df = pd.DataFrame(X_test_scaled, columns=X_test.columns)

            logger.info("Advanced data preprocessing completed")
            logger.info(f"Final feature set: {X_train.columns.tolist()}")

            return X_train_scaled_df, X_test_scaled_df, y_train, y_test, scaler, preprocessor, feature_names

        except Exception as e:
            logger.error(f"Error preprocessing data: {e}")
            raise

   
    def train_model(self, X_train, y_train, X_test, y_test):
        """Train a machine learning model with hyperparameter tuning"""
        try:
            logger.info("Starting model training with hyperparameter tuning...")
            
            # Define multiple model candidates
            model_candidates = {
                'random_forest': RandomForestRegressor(
                    random_state=42, 
                    n_jobs=-1
                )
            }
            
            # Define hyperparameter grids for each model
            param_grids = {
                'random_forest': {
                    'n_estimators': [100, 200],
                    'max_depth': [None, 20, 30],
                    'min_samples_split': [2, 5],
                    'min_samples_leaf': [1, 2]
                }
            }
            
            # Dictionary to store results
            model_results = {}
            
            # Train and evaluate each model
            for model_name, model in model_candidates.items():
                with TimerContextManager(model_training_duration, 
                                        {'model_type': model_name, 'data_source': 'processed'}):
                    logger.info(f"Tuning {model_name}...")
                    
                    # Create grid search
                    grid = GridSearchCV(
                        estimator=model,
                        param_grid=param_grids[model_name],
                        cv=5,
                        scoring='neg_mean_squared_error',
                        n_jobs=-1,
                        verbose=1
                    )
                    
                    # Fit grid search
                    grid.fit(X_train, y_train)
                    
                    # Get best model
                    best_model = grid.best_estimator_
                    
                    # Evaluate on test set
                    y_pred = best_model.predict(X_test)
                    mse = mean_squared_error(y_test, y_pred)
                    rmse = np.sqrt(mse)
                    r2 = r2_score(y_test, y_pred)
                    mae = mean_absolute_error(y_test, y_pred)
                    
                    # Store results
                    model_results[model_name] = {
                        'model': best_model,
                        'best_params': grid.best_params_,
                        'mse': mse,
                        'rmse': rmse,
                        'r2': r2,
                        'mae': mae,
                        'test_predictions': y_pred,
                        'test_actuals': y_test
                    }
                    
                    logger.info(f"{model_name} - Best Parameters: {grid.best_params_}")
                    logger.info(f"{model_name} - Test RMSE: {rmse:.4f}, R²: {r2:.4f}, MAE: {mae:.4f}")
            
            # Find best model based on test R²
            best_model_name = max(model_results, key=lambda x: model_results[x]['r2'])
            best_result = model_results[best_model_name]
            
            logger.info(f"Best model: {best_model_name} with R² = {best_result['r2']:.4f}")
            
            # Save test predictions for later visualization
            self.test_predictions = best_result['test_predictions']
            self.test_actuals = best_result['test_actuals']
            
            # Record metrics in Prometheus
            model_accuracy.labels(model_type=best_model_name, metric_name='mse').set(best_result['mse'])
            model_accuracy.labels(model_type=best_model_name, metric_name='rmse').set(best_result['rmse'])
            model_accuracy.labels(model_type=best_model_name, metric_name='r2_score').set(best_result['r2'])
            model_accuracy.labels(model_type=best_model_name, metric_name='mae').set(best_result['mae'])
            
            return best_result['model'], best_result
        except Exception as e:
            logger.error(f"Error training model: {e}")
            raise
   
    def evaluate_model(self, model, X_test, y_test):
        """Evaluate the trained model with comprehensive metrics"""
        try:
            # Make predictions
            y_pred = model.predict(X_test)
            
            # Calculate various metrics
            mse = mean_squared_error(y_test, y_pred)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_test, y_pred)
            mae = mean_absolute_error(y_test, y_pred)
            
            # Calculate relative errors
            absolute_errors = np.abs(y_test - y_pred)
            relative_errors = absolute_errors / (y_test + 1e-10)  # Avoid division by zero
            mape = np.mean(relative_errors) * 100  # Mean Absolute Percentage Error
            
            # Calculate quantiles of errors
            q90_error = np.percentile(absolute_errors, 90)
            q95_error = np.percentile(absolute_errors, 95)
            q99_error = np.percentile(absolute_errors, 99)
            
            # Log metrics
            logger.info(f"Model Evaluation Metrics:")
            logger.info(f"Mean Squared Error: {mse:.4f}")
            logger.info(f"Root Mean Squared Error: {rmse:.4f}")
            logger.info(f"Mean Absolute Error: {mae:.4f}")
            logger.info(f"Mean Absolute Percentage Error: {mape:.2f}%")
            logger.info(f"R² Score: {r2:.4f}")
            logger.info(f"90th percentile of absolute error: {q90_error:.4f}")
            logger.info(f"95th percentile of absolute error: {q95_error:.4f}")
            logger.info(f"99th percentile of absolute error: {q99_error:.4f}")
            
            # Create comprehensive metrics dictionary
            metrics = {
                "mse": float(mse),
                "rmse": float(rmse),
                "r2_score": float(r2),
                "mae": float(mae),
                "mape": float(mape),
                "q90_error": float(q90_error),
                "q95_error": float(q95_error),
                "q99_error": float(q99_error)
            }
            
            return metrics
        except Exception as e:
            logger.error(f"Error evaluating model: {e}")
            raise
   
    def save_model(self, model, scaler, preprocessor, feature_names, metrics=None):
        """Save the trained model, scaler, preprocessor and metrics"""
        try:
            # Create a directory for models if it doesn't exist
            os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
            
            # Save the model, scaler and preprocessor
            joblib.dump(model, self.model_path)
            joblib.dump(scaler, self.scaler_path)
            joblib.dump(preprocessor, self.preprocessor_path)
            
            # Save feature names
            feature_names_path = os.path.join(os.path.dirname(self.model_path), 'feature_names.json')
            with open(feature_names_path, 'w') as f:
                json.dump(feature_names, f)
            
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
            logger.info(f"Preprocessor saved to {self.preprocessor_path}")
            
            return self.model_path
        except Exception as e:
            logger.error(f"Error saving model: {e}")
            raise
   
    def train(self, data_source='cassandra', days=2):
        """Main function to orchestrate the training process with advanced techniques"""
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
                df = self.create_dummy_data(n_samples=max(1000, days * 24))  # At least 1000 samples
            
            if len(df) == 0:
                logger.warning("No data retrieved. Aborting training.")
                return None
            
            # Preprocess data with advanced feature engineering
            X_train, X_test, y_train, y_test, scaler, preprocessor, feature_names = self.preprocess_data(df)
            
            # Train model with hyperparameter tuning
            model, model_results = self.train_model(X_train, y_train, X_test, y_test)
            
            # Evaluate model with comprehensive metrics
            metrics = self.evaluate_model(model, X_test, y_test)
            
            # Plot and save feature importance
            
            # Save model and related artifacts
            self.save_model(model, scaler, preprocessor, feature_names, metrics)
            
            # Update instance variables
            self.model = model
            self.scaler = scaler
            self.preprocessor = preprocessor
            self.feature_names = feature_names
            
            logger.info("Advanced model training pipeline completed successfully")
            return metrics
        
        except Exception as e:
            logger.error(f"Error in training pipeline: {e}", exc_info=True)
            return None
   
    def preprocess_input(self, data):
        """Preprocess input data for prediction using the saved preprocessor."""
        try:
            # Convert dict → DataFrame
            if isinstance(data, dict):
                data = pd.DataFrame([data])

            # Ensure timestamp exists
            if 'timestamp' not in data.columns:
                data['timestamp'] = datetime.now()

            # Apply preprocessing if available
            if self.preprocessor is not None:
                data_processed = self.preprocessor.transform(data)
            else:
                data_processed = data.copy()

            # Reindex to training features if available
            if self.feature_names is not None:
                data_processed = pd.DataFrame(data_processed, columns=self.feature_names)
                data_processed = data_processed.reindex(columns=self.feature_names, fill_value=0)

            # Convert to numpy
            X_array = data_processed.to_numpy()

            # Scale without feature name checks
            if self.scaler is not None:
                X_scaled = self.scaler.transform(X_array)
            else:
                X_scaled = X_array

            return X_scaled

        except Exception as e:
            logger.error(f"Error preprocessing input data: {e}", exc_info=True)
            raise

   
    def predict(self, data):
        """Make predictions using the trained model"""
        try:
            if self.model is None:
                raise ValueError("Model not loaded. Please train or load a model first.")
                
            # Preprocess the input data
            X_scaled = self.preprocess_input(data)
            
            # Make prediction with timing
            with TimerContextManager(model_prediction_duration, {'model_type': 'advanced_model'}):
                predictions = self.model.predict(X_scaled)
            
            # Ensure predictions are non-negative
            predictions = np.maximum(predictions, 0)
            
            return predictions[0] if len(predictions) == 1 else predictions
        
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            raise
   
    def predict_future(self, current_data, hours_ahead=24):
        """Predict air quality for future hours with uncertainty estimates"""
        try:
            if self.model is None:
                raise ValueError("Model not loaded. Please train or load a model first.")
            
            logger.info(f"Model type during predict_future: {type(self.model)}")

            # Check if the model supports uncertainty estimation
            provides_uncertainty = hasattr(self.model, 'estimators_') and isinstance(self.model, RandomForestRegressor)

            # Prepare base data
            if isinstance(current_data, dict):
                base_data = current_data.copy()
                if 'timestamp' not in base_data:
                    base_data['timestamp'] = datetime.now()
            else:
                base_data = current_data.iloc[0].to_dict()
                if 'timestamp' not in base_data:
                    base_data['timestamp'] = datetime.now()

            # Ensure timestamp is a datetime object
            if isinstance(base_data['timestamp'], str):
                base_data['timestamp'] = pd.to_datetime(base_data['timestamp'])

            future_predictions = []
            current_time = base_data['timestamp']

            for i in range(hours_ahead):
                future_time = current_time + pd.Timedelta(hours=i)
                prediction_data = base_data.copy()
                prediction_data['timestamp'] = future_time

                # Update time-based features if available
                prediction_data['hour'] = future_time.hour
                prediction_data['day'] = future_time.day
                prediction_data['month'] = future_time.month
                prediction_data['day_of_week'] = future_time.weekday()

                # Make prediction
                predicted_value = self.predict(prediction_data)
                prediction_entry = {
                    'timestamp': future_time,
                    'predicted_pm25': predicted_value
                }

                # If model supports uncertainty, estimate it
                if provides_uncertainty:
                    X_scaled = self.preprocess_input(prediction_data)
                    
                    tree_predictions = []
                    for estimator in self.model.estimators_:
                        if hasattr(estimator, 'predict'):
                            pred = estimator.predict(X_scaled)[0]
                            tree_predictions.append(pred)
                        else:
                            logger.warning("Estimator in model is not a tree with predict method.")

                    if tree_predictions:
                        std_dev = np.std(tree_predictions)
                        prediction_entry['lower_bound'] = max(0, predicted_value - 1.96 * std_dev)
                        prediction_entry['upper_bound'] = predicted_value + 1.96 * std_dev
                        prediction_entry['uncertainty'] = std_dev
                    else:
                        logger.warning("No valid tree predictions for uncertainty estimation.")

                future_predictions.append(prediction_entry)

            return future_predictions

        except Exception as e:
            logger.error(f"Error predicting future values: {e}", exc_info=True)
            raise

   
    def get_model_info(self):
        """Get detailed information about the current model"""
        if self.model is None:
            return {
                "status": "not_loaded",
                "timestamp": None,
                "accuracy": None,
                "metrics": None,
                "model_type": None,
                "features": None
            }
        
        # Get model type
        model_type = type(self.model).__name__
        
        # Get feature importance if available
        feature_importance = None
        if hasattr(self.model, 'feature_importances_') and self.feature_names is not None:
            importance_dict = dict(zip(self.feature_names, self.model.feature_importances_))
            # Sort by importance
            feature_importance = dict(sorted(importance_dict.items(), key=lambda x: x[1], reverse=True))
        
        return {
            "status": "loaded",
            "timestamp": self.training_timestamp,
            "accuracy": self.metrics.get("r2_score") if self.metrics else None,
            "metrics": self.metrics,
            "model_type": model_type,
            "features": self.feature_names,
            "feature_importance": feature_importance
        }


# For command-line usage
def parse_args():
    parser = argparse.ArgumentParser(description='Advanced Air Quality AI System')
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
    parser.add_argument('--preprocessor_path', type=str, default='models/air_quality_preprocessor.joblib',
                        help='Path to save/load preprocessor')
    return parser.parse_args()


# Example usage
if __name__ == "__main__":
    args = parse_args()
    
    # Create the AI system
    air_quality_ai = AirQualityPredictor(
        model_path=args.model_path,
        scaler_path=args.scaler_path,
        preprocessor_path=args.preprocessor_path
    )
    
    if args.mode == 'train':
        # Train the model
        metrics = air_quality_ai.train(data_source=args.data_source, days=args.days)
        if metrics:
            print(f"Training completed successfully. R² Score: {metrics['r2_score']:.4f}")
            print(f"RMSE: {metrics['rmse']:.4f}, MAE: {metrics['mae']:.4f}")
        else:
            print("Training failed.")
    
    elif args.mode == 'predict':
        sample_data = {
            'temperature': 25.0,
            'humidity': 60.0,
            'co2': 450.0,
            'timestamp': datetime.now()
        }
        
        try:
            # Make a prediction
            prediction = air_quality_ai.predict(sample_data)
            print(f"Predicted PM2.5: {prediction:.2f}")
            
            # Predict for the next 24 hours
            future_predictions = air_quality_ai.predict_future(sample_data, hours_ahead=24)
            for pred in future_predictions:
                if 'uncertainty' in pred:
                    print(f"Time: {pred['timestamp']}, Predicted PM2.5: {pred['predicted_pm25']:.2f} " + 
                          f"(95% CI: {pred['lower_bound']:.2f} - {pred['upper_bound']:.2f})")
                else:
                    print(f"Time: {pred['timestamp']}, Predicted PM2.5: {pred['predicted_pm25']:.2f}")
        
        except Exception as e:
            print(f"Error making prediction: {e}")
