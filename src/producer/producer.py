from time import sleep
import json
from kafka import KafkaProducer
from datetime import datetime

def producer():
    producer = KafkaProducer(bootstrap_servers=['kafka-1:9092'],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))

    for e in range(1000):
        print(f'Sent a producer msg... {e}')
        data = {'number': e}
        producer.send('test', value=data)
        sleep(5)

if __name__ == '__main__':
    producer()