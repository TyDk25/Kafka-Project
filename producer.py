from kafka import KafkaProducer, KafkaConsumer
import threading
import time
import json
import pandas as pd

topic = 'test-topic'

# Producer sends a message
def produce(v):
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    return producer.send(topic, v)


df = pd.read_csv('indexProcessed.csv')

if __name__ == "__main__":
    # Example usage of the producer
    #produce(k='hello',v='world')
    while True:
            get_data = df.sample(1).to_dict(orient='records')[0]
            produce(v=get_data)


