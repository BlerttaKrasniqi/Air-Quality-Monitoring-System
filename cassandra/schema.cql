CREATE KEYSPACE IF NOT EXISTS air_monitoring
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Raw sensor records (one table, unified definition)
CREATE TABLE IF NOT EXISTS air_monitoring.sensor_data (
  id uuid PRIMARY KEY,
  sensor_id text,
  location text,
  pm25 float,
  pm10 float,
  co2 float,
  temperature float,
  humidity float,
  timestamp timestamp,
  latitude float,
  longitude float,
  weather text,
  wind_speed float,
  wind_direction float
);

-- Aggregations (windowed results)
CREATE TABLE IF NOT EXISTS air_monitoring.sensor_aggregates (
  id uuid PRIMARY KEY,
  window_start timestamp,
  window_end timestamp,
  avg_pm25 float,
  avg_pm10 float,
  avg_co2 float,
  avg_temp float,
  avg_humidity float
);
