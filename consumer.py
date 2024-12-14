from confluent_kafka import Consumer, KafkaException, KafkaError
import logging
import json
import csv
import os

class BMKGWeatherDataConsumer:
    def __init__(self, 
                 kafka_bootstrap_servers='localhost:9092', 
                 kafka_topic='bmkg_weather_data',
                 group_id='bmkg-weather-consumer',
                 output_csv_file='weather_data.csv'):
        """
        Initialize the BMKG Weather Data Consumer
        """
        # Configure logging
        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.kafka_topic = kafka_topic
        self.output_csv_file = output_csv_file
        
        # Kafka Consumer Configuration
        kafka_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # Start from the earliest available message
            'enable.auto.commit': False  # Disable auto commit, we'll handle it manually
        }

        # Initialize Kafka Consumer
        try:
            self.consumer = Consumer(kafka_config)
            self.consumer.subscribe([self.kafka_topic])
            self.logger.info(f"Kafka Consumer initialized successfully and subscribed to topic {self.kafka_topic}")
        except KafkaException as e:
            self.logger.error(f"Failed to initialize Kafka Consumer: {e}")
            raise

        # Initialize CSV file (if it doesn't exist)
        if not os.path.isfile(self.output_csv_file):
            with open(self.output_csv_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(["timestamp", "temperature", "total cloud cover","titik embun", "kelembapan", "jarak pandang"])  # Modify this based on actual data structure

    def consume_messages(self):
        """
        Consume messages from Kafka topic and process them, then export to CSV
        """
        try:
            while True:
                # Poll for new messages
                msg = self.consumer.poll(timeout=1.0)  # Timeout is 1 second
                
                if msg is None:
                    # No new message, continue
                    continue
                elif msg.error():
                    # Handle Kafka error
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.info(f"End of partition reached: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                    else:
                        self.logger.error(f"Error while consuming message: {msg.error()}")
                else:
                    # Process the received message
                    message_value = msg.value().decode('utf-8')
                    self.logger.info(f"Received message: {message_value}")
                    
                    # Optional: Convert the JSON string back to a Python dictionary for further processing
                    try:
                        weather_data = json.loads(message_value)
                        self.logger.info(f"Processed weather data: {weather_data}")
                        
                        # Assuming weather_data is a dictionary with keys: timestamp, temperature, humidity, wind_speed
                        with open(self.output_csv_file, mode='a', newline='', encoding='utf-8') as file:
                            writer = csv.writer(file)
                            # Modify this line based on actual weather_data structure
                            writer.writerow([weather_data.get('timestamp'), weather_data.get('t'), 
                                             weather_data.get('tcc'), weather_data.get('tp'), weather_data.get('hu'), weather_data.get('vs')])
                            self.logger.info(f"Weather data exported to CSV: {weather_data}")
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Failed to decode JSON: {e}")
                
                # Manually commit the message offset to ensure it's only processed once
                self.consumer.commit(msg)

        except KeyboardInterrupt:
            self.logger.info("Consumer stopped by user")
        except Exception as e:
            self.logger.error(f"Error while consuming messages: {e}")
        finally:
            self.consumer.close()

def main():
    # Create consumer instance
    try:
        consumer = BMKGWeatherDataConsumer(
            kafka_bootstrap_servers='localhost:9092',
            kafka_topic='bmkg_weather_data'
        )
        
        # Start consuming messages
        consumer.consume_messages()
    
    except Exception as e:
        print(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
