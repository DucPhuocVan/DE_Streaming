from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.streaming import StreamingContext
from pykafka import KafkaClient
import time
import csv
# Apply Schema to JSON value column and expand the value
from pyspark.sql.functions import from_json
from pyspark.sql.functions import explode, col

# Create the Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

spark.conf.set("spark.sql.adaptive.enabled", "false")

# Create the streaming_df to read from kafka
streaming_df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Devops") \
    .option("startingOffsets", "earliest") \
    .load()

# JSON Schema
json_schema = StructType(
    [StructField('User', StringType(), True), \
    StructField('Card', StringType(), True), \
    StructField('Year', StringType(), True), \
    StructField('Month', StringType(), True), \
    StructField('Day', StringType(), True), \
    StructField('Time', StringType(), True), \
    StructField('Amount', StringType(), True), \
    StructField('Use chip', StringType(), True), \
    StructField('Merchant Name', StringType(), True), \
    StructField('Merchant City', StringType(), True), \
    StructField('Merchant State', StringType(), True), \
    StructField('Zip', StringType(), True), \
    StructField('MCC', StringType(), True), \
    StructField('Errors?', StringType(), True), \
    StructField('Is Fraud?', StringType(), True)])

# Parse value from binay to string
json_df = streaming_df.selectExpr("cast(value as string) as value")

json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*") 


exploded_df = json_expanded_df \
    .select("Year", "Month", "Amount", "Merchant Name", "Merchant City", "Errors?")

# Write the output to console sink to check the output
exploded_df = exploded_df.coalesce(1)

writing_df = exploded_df.writeStream \
    .format("csv") \
    .option("checkpointLocation", "/home/phuoc/Documents/project/checkpoint_dir") \
    .option("path", "/home/phuoc/Documents/project/output") \
    .outputMode("append")\
    .option("header", "true")\
    .start()

writing_df.awaitTermination()