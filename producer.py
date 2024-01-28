from confluent_kafka import Producer
import csv
import random
import time
import json

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message: {msg.value()}: {err}")
    else:
        print(f"Send message: {msg.value().decode('utf-8')}")

def send_to_kafka(producer, topic, csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            value = json.dumps(row).encode('utf-8')  # Chuyển đổi từ điển thành bytes
            producer.produce(topic, key=None, value=value, callback=delivery_report)
            producer.poll(0)
            time.sleep(random.uniform(1, 3))

if __name__ == '__main__':
    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(conf)
    topic = 'Devops'
    csv_file = '/home/phuoc/Downloads/User0_credit_card_transactions.csv'
    send_to_kafka(producer, topic, csv_file)