import csv
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8')
)

file_path = '/home/nhn/Downloads/log_action.csv'
topic = 'vdt2024'

with open(file_path, mode='r', encoding='utf-8') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        message = ','.join(row)
        producer.send(topic, value=message)
        print(f"Sent: {message}")
        time.sleep(1)

producer.flush()
print("All messages sent successfully.")

