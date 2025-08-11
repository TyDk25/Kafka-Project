from kafka import KafkaProducer, KafkaConsumer
import threading
import time
import boto3
import json
import io 
from s3fs import S3FileSystem


topic = 'test-topic'
s3 = boto3.client('s3')
s3fs = S3FileSystem()



consumer = KafkaConsumer(topic,
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for k,v in enumerate(consumer):
    buffer = io.BytesIO()
    buffer.write(json.dumps(v.value).encode('utf-8'))
    buffer.seek(0)
    s3.upload_fileobj(buffer, 'financedata-tdk', f'finance_data_{k}.json')