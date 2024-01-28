from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType

# Tạo Spark Session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("KafkaStreamingExample") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.2.24') \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.jars", "/usr/share/java/postgresql-42.2.10.jar") \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.sql.adaptive.enabled", "false")

# Đọc dữ liệu từ tệp Parquet trên HDFS
hdfs_path = "hdfs://127.0.0.1:9000/user/output/"

# Thiết lập kết nối JDBC cho PostgreSQL
host = "localhost"
pwd2 = "123456"
uid2 = "postgres"
jdbc_url = f"jdbc:postgresql://{host}:5432/postgres"
properties = {
    "user": uid2,
    "password": pwd2,
    "driver": "org.postgresql.Driver"
}

# Ghi dữ liệu vào bảng PostgreSQL
table_name = "report"

# Define schema
schema = StructType([
    StructField("Transaction_Date", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Amount", StringType(), True),
    StructField("Merchant_City", StringType(), True),
    StructField("Merchant_State", StringType(), True),
    StructField("Is_Fraud?", StringType(), True),
])

def load_to_database(hdfs_path, table_name, jdbc_url, properties,year = None, month = None):
    condition = (col("year") == year) if year is not None else (col("year") != "-1") 
    condition = condition & (col("month") == month) if month is not None else condition
    df = spark.read.csv(hdfs_path, header=True, schema=schema).filter(condition)
    df.show()

    df.printSchema()
    df.write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, table=table_name, properties=properties)

load_to_database(hdfs_path, table_name, jdbc_url, properties)
