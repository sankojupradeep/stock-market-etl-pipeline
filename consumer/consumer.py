import time
import json
from kafka import KafkaConsumer
import boto3

# MinIO S3 client
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9002',  # ✅ fixed
    aws_access_key_id='admin',
    aws_secret_access_key='password123',
)

bucket_name = 'row-data'

# Kafka Consumer
consumer = KafkaConsumer(
    'stock_data',   # ✅ match producer topic
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='raw-data-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🚀 Consumer is running...")

# Process messages
for message in consumer:
    record = message.value
    symbol = record['symbol']
    timestamp = record['fetched_at']
    filename = f"{symbol}/{timestamp}.json"

    # Upload to MinIO S3
    s3.put_object(
        Bucket=bucket_name,
        Key=filename,
        Body=json.dumps(record)
    )
    print(f"✅ Uploaded {filename} to S3")
