from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, avg
from pyspark.sql.types import StructType, FloatType, StringType, TimestampType

# Initialize Spark session with Cassandra connector
spark = SparkSession.builder \
    .appName("AirQualityStreamProcessor") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema of incoming JSON data
schema = StructType() \
    .add("pm25", FloatType()) \
    .add("pm10", FloatType()) \
    .add("co2", FloatType()) \
    .add("temperature", FloatType()) \
    .add("humidity", FloatType()) \
    .add("timestamp", StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "air_quality") \
    .load()

# Parse Kafka JSON messages
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Filter invalid data
valid_df = json_df.filter(
    (col("pm25").isNotNull()) &
    (col("temperature") > 0) &
    (col("humidity") > 0)
)

# Convert timestamp string to timestamp type
valid_df = valid_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Add UUID for Cassandra
final_df = valid_df.withColumn("id", expr("uuid()"))

# Write valid records to Cassandra
query_raw = final_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "./checkpoint_raw") \
    .option("keyspace", "air_monitoring") \
    .option("table", "sensor_data") \
    .outputMode("append") \
    .start()

# Sliding window aggregation (e.g., every 30 sec, sliding every 10 sec)
aggregated_df = valid_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "30 seconds", "10 seconds")) \
    .agg(
        avg("pm25").alias("avg_pm25"),
        avg("pm10").alias("avg_pm10"),
        avg("co2").alias("avg_co2"),
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_humidity")
    )

# Write aggregation results to console (for monitoring/debugging)
query_agg = aggregated_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query_raw.awaitTermination()
query_agg.awaitTermination()
