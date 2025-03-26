# User-Requesters
This project simulates logistics data by generating mock parcel events and sending them to Kafka.
## Technologies
- **Programming Language**: Python 3.12
- **Message Broker**: Apache Kafka (KRaft mode)
- **Containerization**: Docker
- **IDE**: PyCharm

## Setup and Running
**Clone the repository**:

   ```
   git clone https://github.com/SofiaVynnytska12/User-Requesters.git
   cd User-Requesters
   ```

2. **Install Prerequisites**:

PyCharm: Download and install from jetbrains.com/pycharm/download/.
Docker: Download and install Docker Desktop from docker.com/products/docker-desktop/, then launch it.
Python: Ensure Python 3.13.2 is installed (download from python.org).

3. **Install Liquibase and MySQL Driver**:
Download Liquibase from liquibase.com/download (e.g., liquibase-4.27.0.zip) and extract it to a folder of your choice.
Download the MySQL JDBC driver from dev.mysql.com/downloads/connector/j/ (select "Platform Independent", e.g., mysql-connector-j-8.0.33.zip), extract, and copy mysql-connector-j-8.0.33.jar to the project:

```
mkdir lib
copy path\to\mysql-connector-j-8.0.33.jar lib\
```

4. **Set Up Kafka and MySQL in Docker**:

Open Docker Desktop and ensure it’s running (check for the green icon in the system tray).
Start the services using Docker Compose:
```
docker-compose up -d
```

5**Apply Liquibase Migrations:**:

Set up the database schema:
path\to\liquibase\liquibase.bat --classpath=lib/mysql-connector-j-8.0.33.jar update


6. **Test Connections**:

Kafka: Verify Kafka is running and check topics:
```
docker exec -it user-requesters-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

MySQL: Connect to MySQL and verify the database:
```
docker exec -it user-requesters-mysql-1 mysql -uroot -proot -e "SHOW DATABASES;"
```

7. **Run the Producer**:

Install dependencies:

```
pip install kafka-python mysql-connector-python
```

Start the Python script to generate and send data to Kafka and MySQL:
```
python kafka_producer.py
```

8. **Consume Messages**:
Check messages in the user-requests topic:
```
docker exec -it user-requesters-kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user-requests --from-beginning
```

9. **View Data in MySQL**:
Open PyCharm Database Tool, connect to localhost:3306 (user: root, password: root, database: logistics_db), and query:
```
SELECT * FROM parcels;
```

## Data Schema
The logistics data is generated in JSON format with ISO timestamps and stored simultaneously in Kafka and MySQL. Each message consists of a key and a value.


### Kafka Key Structure

```
{
  "generation_timestamp": "2025-03-16T10:00:00.000Z",
  "uuid": "550e8400-e29b-41d4-a716-446655440000",
  "service_name": "logistics_service",
  "event_type": "Sent"
}
```
- **Fields**:
  - `generation_timestamp`: ISO timestamp when the event is generated.
  - `uuid`: Unique identifier for the event (UUID).
  - `service_name`: Fixed service identifier ("logistics_service").
  - `event_type`: Type of event ("Sent", "Arrived", "Delivered").

### Kafka Value Structure
```
{
  "tracking_number": "550e8400-e29b-41d4-a716-446655440000",
  "created_date": "2025-03-16T10:00:00.000Z",
  "arrival_date": "2025-03-16T10:00:05.000Z",
  "status": "Arrived",
  "origin": "Kyiv",
  "destination": "Lviv",
  "last_updated": "2025-03-16T10:00:05.000Z",
  "event_type": "Arrived"
}
```
- **Constant Fields**:
  - `tracking_number`: Unique identifier (UUID) generated once per parcel.
  - `created_date`: ISO timestamp of parcel registration.
  - `origin`: Departure location (e.g., "Kyiv").
  - `destination`: Destination location (e.g., "Lviv").
- **Variable Fields**:
  - `arrival_date`: ISO timestamp updated upon arrival at the warehouse (null if not yet arrived).
  - `status`: Current status ("Sent", "Arrived", "Delivered").
  - `last_updated`: ISO timestamp of the last update.
  - `event_type`: Matches `status` and indicates the event type.

#### Data Flow
- **Parcel Registration**: A new parcel is registered with "Sent" status, generating a unique `tracking_number`, and the event is sent to Kafka and saved to MySQL.
- **Status Updates**: As the parcel progresses ("Arrived" after 5 seconds, "Delivered" after another 5 seconds), updated messages with new keys and values are sent to Kafka and MySQL.
- **Storage**: Data is written to Kafka (`user-requests` topic) for real-time messaging and MySQL (`parcels` table) for persistent storage.

#### Notes
- Kafka topic `user-requests` uses 1 partition and 1 replication factor (configurable for future scaling).
- Each event is generated with a 5-second delay between statuses to simulate a realistic logistics timeline.
- Error handling ensures the script continues running even if Kafka or MySQL encounters issues.

## Data Generation
- The `kafka_producer.py` script simulates the lifecycle of parcels:
  1. **"Sent"**: Initial registration of a new parcel with a unique `tracking_number`.
  2. **"Arrived"**: Parcel arrives at the warehouse after 5 seconds, updating `arrival_date`.
  3. **"Delivered"**: Parcel is delivered to the destination after another 5 seconds.
- Each parcel lifecycle takes 15 seconds (5s per event), after which a new parcel is generated.
- Events are sent to Kafka and MySQL concurrently, ensuring data consistency.
- Cities are randomly selected from a list of Ukrainian cities in English (e.g., "Kyiv", "Lviv", "Odesa", "Kharkiv", "Dnipro", etc.).
```
