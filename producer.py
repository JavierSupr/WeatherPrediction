import requests
import json
from confluent_kafka import Producer
import logging
import time
from datetime import datetime

class BMKGWeatherDataStreamer:
    def __init__(self, 
                 api_url='https://api.bmkg.go.id/publik/prakiraan-cuaca',
                 kafka_bootstrap_servers='localhost:9092', 
                 kafka_topic='bmkg_weather_data'):
        """
        Initialize the BMKG Weather Data Streamer
        """
        # Configure logging
        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.api_url = api_url
        self.kafka_topic = kafka_topic
        
        # Kafka Producer Configuration
        kafka_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'client.id': 'bmkg-weather-producer'
        }
        
        # Initialize Kafka Producer
        try:
            self.producer = Producer(kafka_config)
            self.logger.info("Kafka Producer initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka Producer: {e}")
            raise

    def delivery_report(self, err, msg):
        """
        Callback for message delivery reports
        """
        if err is not None:
            self.logger.error(f'Message delivery failed: {err}')
        else:
            self.logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
            
    def extract_weather_data(self, raw_data):
        """
        Extract specific weather data fields without location info

        :param raw_data: Raw API response
        :return: List of extracted weather data
        """
        extracted_data = []

        # Check if 'data' and nested 'cuaca' exist
        if raw_data.get('data') and raw_data['data'][0].get('cuaca'):
            # Iterate through all entries in 'cuaca', which is a list of lists
            for weather_group in raw_data['data'][0]['cuaca']:
                for weather_entry in weather_group:  # Iterate through each timestamp in the group
                    extracted_entry = {
                        'timestamp': weather_entry.get('datetime'),
                        't': weather_entry.get('t'),  # Temperature
                        'tcc': weather_entry.get('tcc'),  # Total Cloud Cover
                        'tp': weather_entry.get('tp'),  # Precipitation
                        'weather_desc': weather_entry.get('weather_desc'),  # Weather Description
                        'weather_desc_en': weather_entry.get('weather_desc_en'),  # English Weather Description
                        'wd_deg': weather_entry.get('wd_deg'),  # Wind Direction (degree)
                        'wd': weather_entry.get('wd'),  # Wind Direction
                        'ws': weather_entry.get('ws'),  # Wind Speed
                        'hu': weather_entry.get('hu'),  # Humidity
                        'vs': weather_entry.get('vs'),  # Visibility
                        'vs_text': weather_entry.get('vs_text')  # Visibility description
                    }
                    extracted_data.append(extracted_entry)

        return extracted_data


    def fetch_weather_data(self, region_code='35.15.17.2015'):
        """
        Fetch weather data from BMKG API for a specific region
        
        :param region_code: Region code for weather data
        :return: Processed weather data
        """
        try:
            params = {'adm4': region_code}
            response = requests.get(self.api_url, params=params)
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            # print(data)
            
            # Extract specific weather data
            processed_data = self.extract_weather_data(data)
            
            return processed_data
        
        except requests.RequestException as e:
            self.logger.error(f"Error fetching weather data: {e}")
            return None

    def stream_to_kafka(self, data_list):
        """
        Stream weather data to Kafka topic

        :param data_list: List of weather data dictionaries
        """
        if not data_list:
            self.logger.warning("No data to stream.")
            return

        try:
            for data in data_list:
                # Convert data to JSON string
                json_data = json.dumps(data).encode('utf-8')

                # Produce message to Kafka
                self.producer.produce(
                    self.kafka_topic, 
                    value=json_data,
                    callback=self.delivery_report
                )

            # Trigger message delivery and handle events
            self.producer.poll(0)  # Non-blocking poll to handle the delivery report
            self.producer.flush()  # Ensure all messages are sent

        except Exception as e:
            self.logger.error(f"Error streaming to Kafka: {e}")

    def start_streaming(self, interval=900):  # Default 15 minutes
        """
        Start continuous streaming of weather data
        
        :param interval: Time between data fetches in seconds
        """
        try:
            while True:
                weather_data = self.fetch_weather_data()
                if weather_data:
                    self.stream_to_kafka(weather_data)
                    print("sampai jj")
                
                # Wait before next fetch
                time.sleep(interval)
        
        except KeyboardInterrupt:
            self.logger.info("Streaming stopped by user")
        except Exception as e:
            self.logger.error(f"Streaming error: {e}")
        finally:
            # Flush any remaining messages
            self.producer.flush()

def main():
    # Create streamer instance
    try:
        streamer = BMKGWeatherDataStreamer(
            kafka_bootstrap_servers='localhost:9092',
            kafka_topic='bmkg_weather_data'
        )
        
        # Start streaming
        streamer.start_streaming()
    
    except Exception as e:
        print(f"Fatal error: {e}")

if __name__ == "__main__":
    main()