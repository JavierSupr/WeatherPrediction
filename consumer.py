from kafka import KafkaConsumer
import json

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'weather-forecast'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Kafka Consumer running...")
for message in consumer:
    print(f"Received data: {message.value}")
