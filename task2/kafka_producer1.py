import json
import uuid
import time
import random
from datetime import datetime, UTC
from kafka import KafkaProducer
from kafka.errors import KafkaError

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: json.dumps(k).encode('utf-8')
        )
        print("Kafka producer connected successfully.")
        return producer
    except KafkaError as e:
        print(f"Failed to connect to Kafka: {e}")
        return None

def generate_parcel_data(tracking_number, status, created_date, arrival_date=None):
    current_time = datetime.now(UTC).isoformat() + ".000Z"
    cities = ["Kyiv", "Lviv", "Odesa", "Kharkiv", "Dnipro"]
    origin = random.choice(cities)
    destination = random.choice([city for city in cities if city != origin])
    return {
        "tracking_number": tracking_number,
        "created_date": created_date,
        "arrival_date": arrival_date,
        "status": status,
        "origin": origin,
        "destination": destination,
        "last_updated": current_time,
        "event_type": status
    }

def generate_kafka_key(event_type):
    timestamp = datetime.now(UTC).isoformat() + ".000Z"
    unique_id = str(uuid.uuid4())
    service_name = "logistics_service"
    return {
        "generation_timestamp": timestamp,
        "uuid": unique_id,
        "service_name": service_name,
        "event_type": event_type
    }

def publish_event(producer, tracking_number, status, created_date, arrival_date=None):
    if producer is None:
        print("Kafka producer not initialized.")
        return

    value = generate_parcel_data(tracking_number, status, created_date, arrival_date)
    key = generate_kafka_key(status)

    try:
        producer.send('user-requests', key=key, value=value, partition=0)
        print(f"Published to Kafka: Key={key}, Value={value}")
    except KafkaError as e:
        print(f"Error sending to Kafka: {e}")

def simulate_parcel_lifecycle(producer):
    if producer is None:
        print("Cannot start simulation: Kafka not initialized.")
        return

    while True:
        tracking_number = str(uuid.uuid4())
        created_time = datetime.now(UTC).isoformat() + ".000Z"

        publish_event(producer, tracking_number, "Sent", created_time)
        time.sleep(5)

        arrival_time = datetime.now(UTC).isoformat() + ".000Z"
        publish_event(producer, tracking_number, "Arrived", created_time, arrival_time)
        time.sleep(5)

        publish_event(producer, tracking_number, "Delivered", created_time, arrival_time)
        time.sleep(5)

if __name__ == "__main__":
    print("Starting Kafka producer...")
    producer = create_producer()
    simulate_parcel_lifecycle(producer)