import json
import uuid
import time
import random
import schedule
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


def job_sent(producer, tracking_number, created_time):
    publish_event(producer, tracking_number, "Sent", created_time)


def job_arrived(producer, tracking_number, created_time):
    arrival_time = datetime.now(UTC).isoformat() + ".000Z"
    publish_event(producer, tracking_number, "Arrived", created_time, arrival_time)


def job_delivered(producer, tracking_number, created_time):
    arrival_time = datetime.now(UTC).isoformat() + ".000Z"
    publish_event(producer, tracking_number, "Delivered", created_time, arrival_time)


def schedule_jobs(producer):
    tracking_number = str(uuid.uuid4())
    created_time = datetime.now(UTC).isoformat() + ".000Z"

    schedule.every(5).seconds.do(job_sent, producer, tracking_number, created_time)
    schedule.every(10).seconds.do(job_arrived, producer, tracking_number, created_time)
    schedule.every(15).seconds.do(job_delivered, producer, tracking_number, created_time)


if __name__ == "__main__":
    print("Starting Kafka producer with scheduled jobs...")
    producer = create_producer()
    if producer:
        schedule_jobs(producer)
        while True:
            schedule.run_pending()
            time.sleep(1)