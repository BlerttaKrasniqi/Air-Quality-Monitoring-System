from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import StructType, FloatType, StringType, TimestampType


spark = SparkSession.builder \
    .appName("AirQualityStreamProcessor") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("pm25", FloatType()) \
    .add("pm10", FloatType()) \
    .add("co2", FloatType()) \
    .add("temperature", FloatType()) \
    .add("humidity", FloatType()) \
    .add("timestamp", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "air_quality") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

processed_df = json_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

query = processed_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "./checkpoint") \
    .option("keyspace", "air_monitoring") \
    .option("table", "sensor_data") \
    .outputMode("append") \
    .start()

query.awaitTermination()