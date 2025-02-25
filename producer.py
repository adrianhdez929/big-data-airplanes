import time
import datetime
import json
import os
import math
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

# OpenSky Network API configuration
STATES_API_URL = 'https://opensky-network.org/api/states/all'
FLIGHTS_API_URL = 'https://opensky-network.org/api/flights/all'


# Configuración de Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=10485760,  # 10MB
    buffer_memory=10485760,     # 10MB
    compression_type='gzip'      # Enable compression
)

def fetch_flights_data():
    load_dotenv()

    OPENSKY_USERNAME = os.getenv('OPENSKY_USERNAME')
    OPENSKY_PASSWORD = os.getenv('OPENSKY_PASSWORD')

    timenow = math.floor(datetime.datetime.now().timestamp())

    api_url = f"{FLIGHTS_API_URL}?begin={timenow - 1000}&end={timenow}"

    try:
        if OPENSKY_USERNAME != "" and OPENSKY_PASSWORD != "":
        # Add authentication and parameters
            response = requests.get(
                api_url,
                auth=(OPENSKY_USERNAME, OPENSKY_PASSWORD),
            )
        else:
            response = requests.get(api_url)
        
        if response.status_code == 200:
            flights = []
            data = response.json()

            for flight in data:
                if flight['estArrivalAirport'] != "":
                    flights.append(flight)
            return {
                'flights': flights
            }
        else:
            print("Error al obtener datos de la API flights")
            print(response.json())
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def fetch_states_data():
    load_dotenv()

    OPENSKY_USERNAME = os.getenv('OPENSKY_USERNAME')
    OPENSKY_PASSWORD = os.getenv('OPENSKY_PASSWORD')

    try:
        if OPENSKY_USERNAME != "" and OPENSKY_PASSWORD != "":
        # Add authentication and parameters
            response = requests.get(
                STATES_API_URL,
                auth=(OPENSKY_USERNAME, OPENSKY_PASSWORD),
            )
        else:
            response = requests.get(STATES_API_URL)
        
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
            print("Error al obtener datos de la API states")
            print(response.json())
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def calculate_delays(flight):
    delays = {
        'flight_icao24': flight['icao24'],
        'callsign': flight.get('callsign', 'N/A').strip(),
        'departure_airport': flight['estDepartureAirport'],
        'arrival_airport': flight['estArrivalAirport'],
    }
    
    # Calculate estimated flight time based on first and last seen times
    actual_flight_time = flight['lastSeen'] - flight['firstSeen']  # in seconds
    
    # Calculate estimated arrival delay
    if flight['firstSeen'] and flight['lastSeen']:
        # Add flight duration
        delays['flight_duration_minutes'] = round(actual_flight_time / 60, 2)
        
        # Calculate horizontal distance to arrival airport
        arrival_distance = flight['estArrivalAirportHorizDistance']  # in meters
        departure_distance = flight.get('estDepartureAirportHorizDistance', 0)  # in meters
        
        if arrival_distance is not None:
            delays['distance_to_arrival'] = arrival_distance
            
            # Calculate expected flight time based on total distance
            total_distance = departure_distance + arrival_distance
            # Assuming average speed of 800 km/h = 222 m/s for commercial flights
            expected_flight_time = total_distance / 222  # seconds
            
            # Calculate delay magnitude
            time_difference = actual_flight_time - expected_flight_time
            delay_minutes = round(time_difference / 60, 2)
            
            # Determine delay status and magnitude
            if delay_minutes <= 0:
                delays['status'] = 'on_time'
                delays['delay_magnitude'] = 0
                delays['estimated_arrival_delay'] = 0
            else:
                # Calculate delay magnitude (1-5 scale)
                if arrival_distance > 5000:  # If still far from destination
                    remaining_time = arrival_distance / 222  # seconds
                    total_estimated_delay = delay_minutes + (remaining_time / 60)
                    
                    if total_estimated_delay < 15:
                        delays['status'] = 'slight_delay'
                        delays['delay_magnitude'] = 1
                    elif total_estimated_delay < 30:
                        delays['status'] = 'minor_delay'
                        delays['delay_magnitude'] = 2
                    elif total_estimated_delay < 60:
                        delays['status'] = 'moderate_delay'
                        delays['delay_magnitude'] = 3
                    elif total_estimated_delay < 120:
                        delays['status'] = 'significant_delay'
                        delays['delay_magnitude'] = 4
                    else:
                        delays['status'] = 'severe_delay'
                        delays['delay_magnitude'] = 5
                    
                    delays['estimated_arrival_delay'] = round(total_estimated_delay, 2)
                else:
                    # Flight is close to destination
                    if delay_minutes < 15:
                        delays['status'] = 'approaching_slight_delay'
                        delays['delay_magnitude'] = 1
                    elif delay_minutes < 30:
                        delays['status'] = 'approaching_minor_delay'
                        delays['delay_magnitude'] = 2
                    else:
                        delays['status'] = 'approaching_with_delay'
                        delays['delay_magnitude'] = 3
                    
                    delays['estimated_arrival_delay'] = delay_minutes
    
    return delays

def process_flight_data(flight):
    # Process each flight and add delay information
    flight_info = calculate_delays(flight)
    
    # Add additional context
    flight_info['timestamp'] = datetime.datetime.now().timestamp()
    flight_info['first_seen'] = datetime.datetime.fromtimestamp(flight['firstSeen']).strftime('%Y-%m-%d %H:%M:%S')
    flight_info['last_seen'] = datetime.datetime.fromtimestamp(flight['lastSeen']).strftime('%Y-%m-%d %H:%M:%S')
    
    return flight_info

def fetch_airplanes_data():
    states = fetch_states_data()
    flights = fetch_flights_data()
    
    if flights and 'flights' in flights:
        # Process each flight and add delay information
        processed_flights = [process_flight_data(flight) for flight in flights['flights']]
        states['flight_delays'] = processed_flights
    return states

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