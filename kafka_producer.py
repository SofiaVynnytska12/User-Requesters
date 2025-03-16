import json
import uuid
import time
import random
import mysql.connector
from datetime import datetime
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

def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="logistics_db",
            port="3306"
        )
        print("MySQL connected successfully.")
        return connection
    except mysql.connector.Error as e:
        print(f"Failed to connect to MySQL: {e}")
        return None

def create_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS parcels (
                tracking_number VARCHAR(36) PRIMARY KEY,
                created_date DATETIME,
                arrival_date DATETIME DEFAULT NULL,
                status ENUM('Sent', 'Arrived', 'Delivered') NOT NULL,
                origin VARCHAR(100) NOT NULL,
                destination VARCHAR(100) NOT NULL,
                last_updated DATETIME NOT NULL
            )
        """)
        connection.commit()
        cursor.close()
        print("Table 'parcels' created or already exists.")
    except mysql.connector.Error as e:
        print(f"Error creating table: {e}")

def save_to_mysql(connection, data):
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO parcels (tracking_number, created_date, arrival_date, status, origin, destination, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                arrival_date = VALUES(arrival_date),
                status = VALUES(status),
                last_updated = VALUES(last_updated)
        """
        values = (
            data["tracking_number"],
            data["created_date"].replace("T", " ").replace(".000Z", ""),
            data["arrival_date"].replace("T", " ").replace(".000Z", "") if data["arrival_date"] else None,
            data["status"],
            data["origin"],
            data["destination"],
            data["last_updated"].replace("T", " ").replace(".000Z", "")
        )
        cursor.execute(query, values)
        connection.commit()
        cursor.close()
        print(f"Data saved to MySQL: {data['tracking_number']} - {data['status']}")
    except mysql.connector.Error as e:
        print(f"Error saving to MySQL: {e}")

def generate_parcel_data(tracking_number, status, created_date, arrival_date=None):
    current_time = datetime.utcnow().isoformat() + ".000Z"
    cities = [
        "Kyiv", "Lviv", "Odesa", "Kharkiv", "Dnipro",
        "Zaporizhzhia", "Mykolaiv", "Vinnytsia", "Kherson", "Poltava"
    ]
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
    timestamp = datetime.utcnow().isoformat() + ".000Z"
    unique_id = str(uuid.uuid4())
    service_name = "logistics_service"
    return {
        "generation_timestamp": timestamp,
        "uuid": unique_id,
        "service_name": service_name,
        "event_type": event_type
    }

def publish_event(producer, mysql_connection, tracking_number, status, created_date, arrival_date=None):
    if producer is None or mysql_connection is None:
        print("Kafka producer or MySQL not initialized.")
        return

    value = generate_parcel_data(tracking_number, status, created_date, arrival_date)
    key = generate_kafka_key(status)

    try:
        producer.send('user-requests', key=key, value=value, partition=0)
        save_to_mysql(mysql_connection, value)
        print(f"Published to Kafka: Key={key}, Value={value}")
    except KafkaError as e:
        print(f"Error sending to Kafka: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def simulate_parcel_lifecycle(producer, mysql_connection):
    if producer is None or mysql_connection is None:
        print("Cannot start simulation: Kafka or MySQL not initialized.")
        return

    while True:
        tracking_number = str(uuid.uuid4())
        created_time = datetime.utcnow().isoformat() + ".000Z"

        publish_event(producer, mysql_connection, tracking_number, "Sent", created_time)
        time.sleep(10)

        arrival_time = datetime.utcnow().isoformat() + ".000Z"
        publish_event(producer, mysql_connection, tracking_number, "Arrived", created_time, arrival_time)
        time.sleep(10)

        publish_event(producer, mysql_connection, tracking_number, "Delivered", created_time, arrival_time)
        time.sleep(10)


if __name__ == "__main__":
    print("Starting Kafka producer and MySQL...")
    producer = create_producer()
    mysql_connection = connect_to_mysql()
    if mysql_connection:
        create_table(mysql_connection)
    simulate_parcel_lifecycle(producer, mysql_connection)