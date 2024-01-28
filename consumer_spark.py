from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.streaming import StreamingContext
from pykafka import KafkaClient
import time
import csv

def process_message(message):
    # Lấy giá trị của tin nhắn
    value = message[1]
    
    # Phân tích dữ liệu từ tin nhắn (giả sử dữ liệu là CSV)
    fields = value.split(",")
    User = fields[0]
    Card = fields[1]
    Year = fields[2]
    Month = fields[3]
    Day = fields[4]
    Time = fields[5]
    Amount = float(fields[6])
    Use_Chip = fields[7]
    Merchant_Name = fields[8]
    Merchant_City = fields[9]
    Merchant_State = fields[10]
    Zip = fields[11]
    MCC = fields[12]
    Error = fields[13]
    Is_Fraud = fields[14]
    
    if Is_Fraud == "Yes":
        return
    
    with open("result.csv", mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([User, Card, Year, Month, Day, Time, Amount, Use_Chip, Merchant_Name, Merchant_City, Merchant_State, Zip, MCC, Error])

    print("Processed transaction:", User, Card, Year, Month, Day, Time, Amount, Use_Chip, Merchant_Name, Merchant_City, Merchant_State, Zip, MCC, Error)

# Đọc dữ liệu từ Kafka topic
def read_from_kafka():
    for message in consumer:
        process_message(message)

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("KafkaStreamingExample") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
        .config("spark.sql.shuffle.partitions", 4) \
        .getOrCreate()

    # Khởi tạo StreamingContext với batch interval là 1 giây
    ssc = StreamingContext(spark.sparkContext, 1)

    # Định nghĩa thông tin Kafka
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "Devops"

    # Tạo một Kafka Consumer
    client = KafkaClient(hosts=kafka_bootstrap_servers)
    topic = client.topics[kafka_topic]
    consumer = topic.get_simple_consumer()


    # Đọc dữ liệu từ Kafka topic
    def read_from_kafka():
        for message in consumer:
            process_message(message)

    # Tạo một DStream để đọc dữ liệu từ Kafka
    kafkaStream = ssc.queueStream([], default=True)

    # Áp dụng hàm đọc dữ liệu từ Kafka topic cho mỗi RDD trong DStream
    kafkaStream.foreachRDD(lambda rdd: rdd.foreachPartition(lambda _: read_from_kafka()))

    # Khởi động Spark Streaming job
    ssc.start()
    ssc.awaitTermination()


    # df = spark.readStream.format("kafka") \
    #     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    #     .option("subscribe", kafka_topic) \
    #     .option("startingOffsets", "latest") \
    #     .load()

    # df = df.selectExpr("CAST(value AS STRING)")

    # df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    # processed_df = df.filter(col('IsFraud') == 'Yes')

    # query = processed_df.writeStream \
    #     .format("csv") \
    #     .option("checkpointLocation", "/home/phuoc/Documents/project/checkpoint") \
    #     .option("path", "/home/phuoc/Documents/project/output.csv") \
    #     .outputMode("append") \
    #     .trigger(processingTime="1 seconds") \
    #     .start()

    # # Periodically consolidate output into a single CSV file
    # while True:
    #     # Coalesce the output files into a single CSV file
    #     consolidated_df = spark.read.csv(
    #         "/home/phuoc/Documents/project/output.csv",
    #         schema="IsFraud STRING, TransactionYear STRING, TransactionMonth STRING, TransactionDate STRING, TransactionTime STRING, MerchantName STRING, MerchantCity STRING, Amount DOUBLE"
    #     ).coalesce(1)

    #     # Write consolidated data into a single CSV file
    #     consolidated_df.write.mode("overwrite").csv("/home/phuoc/Documents/project/output.csv")

    #     # Sleep for a specified time before consolidating again
    #     time.sleep(300)

    # query.awaitTermination()