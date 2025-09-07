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


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg,to_timestamp,coalesce,expr,lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, IntegerType, TimestampType

# ---------- Spark session ----------
spark = (
    SparkSession.builder
    .appName("AirQualityStream")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    # Cassandra connector
    .config("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST", "cassandra"))
    .config("spark.cassandra.connection.port", os.getenv("CASSANDRA_PORT", "9042"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("INFO")

# ---------- Kafka source ----------
kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "sensor-data")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")  # make sure we consume existing messages
    .load()
)

# ---------- Parse JSON ----------
schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("location", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("timestamp", StringType()),  # will parse to timestamp below
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pm25", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("co2", DoubleType()),
    StructField("weather", StringType()),
    StructField("wind_speed", DoubleType()),
    StructField("wind_direction", IntegerType()),
    StructField("events", ArrayType(StringType()))
])

parsed = (
    raw.select(from_json(col("value").cast("string"), schema).alias("j"))
       .select(
           col("j.timestamp").alias("ts_str"),
           col("j.pm25").alias("pm25"),
           col("j.temperature").alias("temperature")
       )
       # ISO-8601 with microseconds and timezone, e.g. 2025-09-07T22:03:16.467687+00:00
       # Try 6-digit and 3-digit fractional seconds just in case your simulator varies.
       .withColumn(
           "event_time",
           coalesce(
               to_timestamp(col("ts_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
               to_timestamp(col("ts_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
           )
       )
       .drop("ts_str")
       .filter(col("event_time").isNotNull())
)

# ---------- Windowed aggregates (e.g., 1 minute) ----------
windowed = (
    parsed
    .withWatermark("event_time", "2 minutes")
    .groupBy(window(col("event_time"), "1 minute").alias("w"))
    .agg(
        avg("pm25").alias("avg_pm25"),
        avg("temperature").alias("avg_temp"),
        # if you want to fill the other avg_* later, add them here too
        # avg("pm10").alias("avg_pm10"),
        # avg("co2").alias("avg_co2"),
        # avg("humidity").alias("avg_humidity"),
    )
    .select(
        expr("uuid()").alias("id"),
        col("w.start").alias("window_start"),
        col("w.end").alias("window_end"),
        col("avg_pm25").cast("float").alias("avg_pm25"),
        lit(None).cast("float").alias("avg_pm10"),
        lit(None).cast("float").alias("avg_co2"),
        col("avg_temp").cast("float").alias("avg_temp"),
        lit(None).cast("float").alias("avg_humidity"),
    )
)

def write_aggregates_to_cassandra(batch_df, batch_id: int):
    (batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="sensor_aggregates", keyspace="air_monitoring")
        .save())
CHECKPOINT_RAW = "/tmp/spark_checkpoints/raw"
CHECKPOINT_AGG =  "/tmp/spark_checkpoints/agg"

# Optional: also keep a raw mirror of a few fields into Cassandra (if you want to confirm ingest)
def write_raw_to_cassandra(batch_df, batch_id: int):
    out = (batch_df
           .select(
               expr("uuid()").alias("id"),
               col("event_time").alias("timestamp"),
               col("pm25").cast("float").alias("pm25"),
               col("temperature").cast("float").alias("temperature")
           ))
    (out.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="sensor_data", keyspace="air_monitoring")
        .save())



agg_query = (windowed.writeStream
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_AGG)
    .foreachBatch(write_aggregates_to_cassandra)
    .start())

raw_query = (
    parsed.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_RAW)
    .foreachBatch(write_raw_to_cassandra)
    .start()
)

debug_q = (
    parsed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", "5")
    .start()
)


agg_query.awaitTermination()
raw_query.awaitTermination()
