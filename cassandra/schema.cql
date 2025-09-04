CREATE KEYSPACE IF NOT EXISTS air_monitoring
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- FIX: table must be in the *same* keyspace name "air_monitoring"
CREATE TABLE IF NOT EXISTS air_monitoring.sensor_data (
  id uuid PRIMARY KEY,
  pm25 float,
  pm10 float,
  co2 float,
  temperature float,
  humidity float,
  timestamp timestamp
);
