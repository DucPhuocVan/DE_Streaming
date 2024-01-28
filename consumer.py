from confluent_kafka import Consumer
import json
import pandas as pd 

def consume_from_kafka(consumer, topic):
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Chuyển đổi giá trị của tin nhắn từ JSON thành DataFrame
        # else:
            # json_data = json.loads(msg.value()) 
            # print(f"Received message: {msg.value().decode('utf-8')}")

        else:
            message = msg.value().decode("utf-8") + '\n'
            with open('/home/phuoc/Downloads/status_from_producer.txt', 'a') as file:
                file.write(message)
            print(f"Received message: {msg.value().decode('utf-8')}")

    consumer.close()

if __name__ == '__main__':
    conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'my-consumer-group'}
    consumer = Consumer(conf)
    topic = 'Devops'
    consume_from_kafka(consumer, topic)