from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, FloatType, StringType, TimestampType

# Initialize Spark session with Cassandra connector
spark = SparkSession.builder \
    .appName("AirQualityStreamProcessor") \
    .config("spark.cassandra.connection.host", "localhost") \
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
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "air_quality") \
    .load()

# Parse Kafka JSON messages
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Cast timestamp and add UUID id column
processed_df = json_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
final_df = processed_df.withColumn("id", expr("uuid()"))

# Write to Cassandra
query = final_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "./checkpoint") \
    .option("keyspace", "air_monitoring") \
    .option("table", "sensor_data") \
    .outputMode("append") \
    .start()

query.awaitTermination()
