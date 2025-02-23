import time
import json
import os
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

# OpenSky Network API configuration
API_URL = 'https://opensky-network.org/api/states/all'


# Configuración de Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=10485760,  # 10MB
    buffer_memory=10485760,     # 10MB
    compression_type='gzip'      # Enable compression
)

def fetch_airplanes_data():
    load_dotenv()

    OPENSKY_USERNAME = os.getenv('OPENSKY_USERNAME')
    OPENSKY_PASSWORD = os.getenv('OPENSKY_PASSWORD')

    try:
        if OPENSKY_USERNAME != "" and OPENSKY_PASSWORD != "":
        # Add authentication and parameters
            response = requests.get(
                API_URL,
                auth=(OPENSKY_USERNAME, OPENSKY_PASSWORD),
            )
        else:
            response = requests.get(API_URL)
        
        if response.status_code == 200:
            data = response.json()
            states = []

            for item in data['states']:
                state = {
                    'icao24': item[0],
                    'callsign': item[1],
                    'origin_country': item[2],
                    'time_position': item[3],
                    'last_contact': item[4],
                    'longitude': item[5],
                    'latitude': item[6],
                    'baro_altitude': item[7],
                    'on_ground': item[8],
                    'velocity': item[9],
                    'vertical_rate': item[10],
                    'sensors': item[11],
                    'geo_altitude': item[12],
                    'squawk': item[13],
                    'spi': item[14],
                    'position_source': item[15],
                    'category': item[16]
                }
                states.append(state)
            return {
                'timestamp': data['time'],
                'states': states
            }
        else:
            print("Error al obtener datos de la API")
            print(response.json())
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None

print("Starting Kafka producer...")

while True:
    print("Fetching airplane data...")
    message = fetch_airplanes_data()
    if message:
        print(f"Got data with {len(message['states'])} airplane states")
        future = producer.send('airplanes_data', value=message)
        try:
            record_metadata = future.get(timeout=10)
            print(f"Message sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
            producer.flush()
        except Exception as e:
            print(f"Error sending message to Kafka: {e}")
    else:
        print("No data received from API")
    time.sleep(10)  # Esperar 10 segundos antes de la siguiente captura