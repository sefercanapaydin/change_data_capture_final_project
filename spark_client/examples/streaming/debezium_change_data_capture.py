from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Stream From Kafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Schema definition for the Kafka JSON payload
customerFields = [
    StructField("customerId", StringType()),
    StructField("customerFName", StringType()),
    StructField("customerLName", StringType()),
    StructField("customerEmail", StringType()),
    StructField("customerPassword", StringType()),
    StructField("customerStreet", StringType()),
    StructField("customerCity", StringType()),
    StructField("customerState", StringType()),
    StructField("customerZipcode", StringType())
]

schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType(customerFields)),
        StructField("after", StructType(customerFields)),
        StructField("ts_ms", StringType()),
        StructField("op", StringType())
    ]))
])

# Read from Kafka
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver4.public.customers1") \
    .load()

# Select and cast the value field to string
lines2 = lines.selectExpr("CAST(value AS STRING)")

# Parse the JSON data
parsedData = lines2.select(from_json(col("value"), schema).alias("data"))

# Flatten the data and select fields
flattenedData = parsedData.select(
    col("data.payload.before.*"),
    col("data.payload.after.*"),
    col("data.payload.ts_ms"),
    col("data.payload.op")
)

# Define checkpoint location
checkpoint_dir = "file:///tmp/streaming/read_from_kafka"

# Write the streaming output to console
streamingQuery = flattenedData \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="1 second") \
    .option("checkpointLocation", checkpoint_dir) \
    .option("numRows", 10) \
    .option("truncate", False) \
    .start()

# Start streaming
streamingQuery.awaitTermination()
