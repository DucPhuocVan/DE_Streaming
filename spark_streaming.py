from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws, lit, expr
from datetime import datetime


# Create the Spark Session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("KafkaStreamingExample") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .config("spark.sql.shuffle.partitions", 4) \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.sql.adaptive.enabled", "false")


# Define schema
schema = StructType([
    StructField("User", StringType(), True),
    StructField("Card", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Day", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Amount", StringType(), True),
    StructField("Use_Chip", StringType(), True),
    StructField("Merchant_Name", StringType(), True),
    StructField("Merchant_City", StringType(), True),
    StructField("Merchant_State", StringType(), True),
    StructField("Zip", StringType(), True),
    StructField("MCC", StringType(), True),
    StructField("Errors", StringType(), True),
    StructField("Is_Fraud?", StringType(), True),
])


# Create the streaming_df to read from kafka
streaming_df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Devops") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false")\
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Transform data in a streaming-friendly way
df_transformed = streaming_df \
    .withColumn("Transaction_Date", concat_ws("/", col("Day"), col("Month"), col("Year"))) \
    .withColumn("Time", concat_ws(":", col("Time"), lit("00"))) \
    .withColumn("Amount", expr("substring(Amount, 2)").cast("float") * 24000) \
    .withColumn("created_date", lit(datetime.now())) \
    .filter((col("Is_Fraud?") == "No") | (col("Is_Fraud?") == lit(None).cast("string"))) \
    .select("Transaction_Date", "Year", "Month", "Time", "Amount", "Merchant_City", "Merchant_State", "Is_Fraud?")

# Coalesce to reduce the number of partitions
# df_transformed = df_transformed.coalesce(1)

writing_df = df_transformed.writeStream \
    .trigger(processingTime="5 seconds") \
    .outputMode("append")\
    .format("csv") \
    .option("path", "hdfs://127.0.0.1:9000/user/output/") \
    .option("checkpointLocation", "hdfs://127.0.0.1:9000/user/checkpoint_dir") \
    .option("format", "append")\
    .option("header", "true") \
    .start()

# Wait for the real-time processing to finish
writing_df.awaitTermination()

#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_streaming.py

    # .option("path", "hdfs://127.0.0.1:9000/user/output/") \
    # .option("checkpointLocation", "hdfs://127.0.0.1:9000/user/checkpoint_dir") \

    # .option("path", "/home/phuoc/Documents/project/output") \
    # .option("checkpointLocation", "/home/phuoc/Documents/project/output/checkpoint_dir") \

    # .partitionBy("Year", "Month") \