import json
from kafka import KafkaConsumer
from datetime import datetime
from time import sleep

def consumer():
    consumer = KafkaConsumer(
        'test',
        bootstrap_servers=['kafka-1:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        print(f'[{datetime.now()}]: Partition: {message.partition}, Offset: {message.offset}, Key: {message.key}, Message: {message.value}')

if __name__ == '__main__':
    consumer()