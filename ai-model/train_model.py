import pandas as pd
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from data_loader import load_data

data = load_data() # get the data from cassandra

data['hour'] = data['timestamp'].dt.hour
data['dayofweek'] = data['timestamp'].dt.dayofweek
data['pm25_ma'] = data['pm25'].rolling(window=3).mean().fillna(method='bfill')

data['pm25_next'] = data['pm25'].shift(-1)
data = data.dropna()

features = ['pm25', 'co2', 'temperature', 'humidity', 'hour', 'dayofweek', 'pm25_ma']
X = data[features]
y = data['pm25_next']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
print(f"Model trained. MSE: {mse:.2f}")

joblib.dump(model, 'ai_model/pm25_predictor.pkl')
print("Model saved to ai_model/pm25_predictor.pkl")
