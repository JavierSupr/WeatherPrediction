from kafka import KafkaProducer
import requests
import json
import time
import schedule

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'weather-forecast'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather_data():
    url = "https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4={kode_wilayah_tingkat_iv}"
    try:
        response = requests.get(url)
        response.raise_for_status() 
        data = response.json()  

        producer.send(TOPIC_NAME, data)
        print(f"Data sent to Kafka: {data}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")

schedule.every(10).minutes.do(fetch_weather_data)

print("Kafka Producer running...")
while True:
    schedule.run_pending()
    time.sleep(1)
