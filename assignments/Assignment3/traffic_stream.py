from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, avg, count
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
# from pyspark.sql.streaming import Trigger


# Initialize Spark Session
spark = SparkSession.builder.appName("TrafficAnalysis").getOrCreate()

# Define Schema for Kafka Data
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", FloatType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", FloatType(), True),
    StructField("congestion_level", StringType(), True)
])

# Read Data from Kafka
raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_data") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON messages
df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp to structured window
df = df.withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

# Congestion Analysis (1-minute window)
congestion_agg = df.groupBy(window(col("event_time"), "1 minute"), col("sensor_id")) \
    .agg(count("congestion_level").alias("total_readings"),
         sum((col("congestion_level") == "HIGH").cast("int")).alias("high_congestion_count"))

# Traffic Volume Analysis (5-minute window)
volume_agg = df.groupBy(window(col("event_time"), "5 minutes"), col("sensor_id")) \
    .agg(sum("vehicle_count").alias("total_vehicles"))

# Speed Analysis (10-minute window)
speed_agg = df.groupBy(window(col("event_time"), "10 minutes"), col("sensor_id")) \
    .agg(avg("average_speed").alias("avg_speed"))

# Busiest Sensors (30-minute window)
busiest_sensors = df.groupBy(window(col("event_time"), "30 minutes"), col("sensor_id")) \
    .agg(sum("vehicle_count").alias("total_vehicles"))

# Write Streams to Console (Can be changed to database or file)
congestion_query = congestion_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

volume_query = volume_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

speed_query = speed_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

busiest_query = busiest_sensors.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
