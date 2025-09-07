# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, expr, window, avg
# from pyspark.sql.types import StructType, FloatType, StringType, TimestampType

# # Initialize Spark session with Cassandra connector
# spark = SparkSession.builder \
#     .appName("AirQualityStreamProcessor") \
#     .config("spark.cassandra.connection.host", "cassandra") \
#     .config("spark.cassandra.connection.port", "9042") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Define the schema of incoming JSON data
# schema = StructType() \
#     .add("pm25", FloatType()) \
#     .add("pm10", FloatType()) \
#     .add("co2", FloatType()) \
#     .add("temperature", FloatType()) \
#     .add("humidity", FloatType()) \
#     .add("timestamp", StringType())

# # Read from Kafka
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "sensor-data") \
#     .load()

# # Parse Kafka JSON messages
# json_df = df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")

# # Cast timestamp and add UUID id column
# processed_df = json_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
# final_df = processed_df.withColumn("sensor_id", expr("uuid()"))
# # Filter invalid data
# valid_df = json_df.filter(
#     (col("pm25").isNotNull()) &
#     (col("temperature") > 0) &
#     (col("humidity") > 0)
# )
# # Convert timestamp string to timestamp type
# valid_df = valid_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# # Add UUID for Cassandra
# final_df = valid_df.withColumn("id", expr("uuid()"))

# # Write valid records to Cassandra
# query_raw = final_df.writeStream \
#     .format("org.apache.spark.sql.cassandra") \
#     .option("checkpointLocation", "./checkpoint_raw") \
#     .option("keyspace", "air_monitoring") \
#     .option("table", "sensor_data") \
#     .outputMode("append") \
#     .start()

# # Sliding window aggregation (e.g., every 30 sec, sliding every 10 sec)
# aggregated_df = valid_df \
#     .withWatermark("timestamp", "1 minute") \
#     .groupBy(window(col("timestamp"), "30 seconds", "10 seconds")) \
#     .agg(
#         avg("pm25").alias("avg_pm25"),
#         avg("pm10").alias("avg_pm10"),
#         avg("co2").alias("avg_co2"),
#         avg("temperature").alias("avg_temp"),
#         avg("humidity").alias("avg_humidity")
#     )

# # Write aggregation results to console (for monitoring/debugging)
# query_agg = aggregated_df.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# query_raw.awaitTermination()


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, avg, window
from pyspark.sql.types import StructType, FloatType, StringType, TimestampType

spark = (SparkSession.builder
    .appName("AirQualityStreamProcessor")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
    # Optional hardening:
    .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = (StructType()
    .add("pm25", FloatType())
    .add("pm10", FloatType())
    .add("co2", FloatType())
    .add("temperature", FloatType())
    .add("humidity", FloatType())
    .add("timestamp", StringType())
)

# ---- Kafka source (hardened) ----
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sensor-data")       # <— must match your simulator
    .option("startingOffsets", "latest")      # don't try to read old, possibly gone offsets
    .option("failOnDataLoss", "false")        # don't crash if partitions/offsets moved
    .option("maxOffsetsPerTrigger", "3000")   # optional: throttle
    .load()
)

json_df = (raw.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

valid_df = (json_df
    .filter(
        col("pm25").isNotNull() &
        (col("temperature") > 0) &
        (col("humidity") > 0)
    )
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))
    .withColumn("id", expr("uuid()"))
)

# ---- Write to Cassandra (stable checkpoint path) ----
query_raw = (valid_df.writeStream
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", "air_monitoring")
    .option("table", "sensor_data")
    .option("checkpointLocation", "/opt/spark/checkpoints/raw")  # <— stable, absolute
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

# (Optional) windowed debug aggregation — give it its own checkpoint
# aggregated_df = (valid_df
#     .withWatermark("timestamp", "1 minute")
#     .groupBy(window(col("timestamp"), "30 seconds", "10 seconds"))
#     .agg(
#         avg("pm25").alias("avg_pm25"),
#         avg("pm10").alias("avg_pm10"),
#         avg("co2").alias("avg_co2"),
#         avg("temperature").alias("avg_temp"),
#         avg("humidity").alias("avg_humidity"),
#     )
# )
# query_agg = (aggregated_df.writeStream
#     .outputMode("update")
#     .format("console")
#     .option("truncate", "false")
#     .option("checkpointLocation", "/opt/spark/checkpoints/agg")
#     .trigger(processingTime="10 seconds")
#     .start()
# )

# Wait for all active streams (safer if you re-enable query_agg later)
for q in spark.streams.active:
    q.awaitTermination()
