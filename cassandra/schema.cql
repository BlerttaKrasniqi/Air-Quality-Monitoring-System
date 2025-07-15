CREATE KEYSPACE IF NOT EXISTS air_monitoring
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS airmonitoring.sensor_data (
    id UUID PRIMARY KEY,
    pm25 float,
    pm10 float,
    co2 float,
    temperature float,
    humidity float,
    timestamp timestamp
);
