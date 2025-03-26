Tools and Settings

Python Script**: We use kafka-python to generate JSON and send to Kafka, and mysql-connector-python to store in MySQL.

Kafka: Messages are sent to localhost:9092 in the user-requests topic (single partition, KRaft mode without Zookeeper).

MySQL: Data is stored in the parcels table in the logistics_db database.

Docker: Containers for Kafka and MySQL are configured via docker-compose.yml.

Frequency: A new event is generated every 5 seconds to simulate real-time.

Mock Data Creation Strategy

Parcel Lifecycle:
Event 1: "Sent" — starts with registering a new parcel.
Event 2: "Arrived" — arrival at the warehouse in 5 seconds with arrival_date updated.
Event 3: "Delivered" — delivery to the destination in 5 more seconds.

Uniqueness:
tracking_number in the value is generated as a UUID for each parcel.
The Kafka key includes a unique uuid and generation_timestamp for each event.

Time: We use ISO 8601 format (e.g., "2025-03-16T10:00:00.000Z") with UTC.

Data flow
Generate JSON with key and value for each event.
Send user-requests topic to Kafka.
Save parcels table in MySQL with update by tracking_number.

Implementation steps

Generate initial data: Create JSON for Sent with new key and initial values.

Update events: Change JSON for "Arrived" and "Delivered" with new keys and timestamps.

Write to Kafka: Use Kafka Producer to send each event with a unique key.

Write to MySQL: Insert or update data in parcels table.

Logistics data description:

The mock data shows a three-step logistics process for each parcel, broken down into key and value:

Key:
generation_timestamp: Event generation timestamp (variable for each event).
uuid: Unique event identifier (UUID, generated for each event).
service_name: Constant field, always "logistics_service".
event_type: Event type ("Sent", "Arrived", "Delivered"), corresponding to status.

Value:
Tracking_number: A unique identifier (UUID) that is generated when a package is created and does not change.
Created_date: A fixed timestamp set at "Sent".
Arrival_date: A variable field, updated at "Arrived", null before that.
Status: A variable field with three values:
"Sent"
"Arrived"
"Delivered"
Origin: A constant field, a random city (e.g. "Kyiv").
Destination: A constant field, a random city (e.g. "Lviv").
Last_updated: A variable field, updated at every event.
Event type (event_type): Corresponds to the status, duplicates event_type from the key.

The data is generated as JSON, sent to Kafka with a key and value, and simultaneously stored in MySQL.

JSON Structure
Event 1: Registration (Sent)
Key:

{
"generation_timestamp": "2025-03-16T10:00:00.000Z",
"uuid": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
"service_name": "logistics_service",
"event_type": "Sent"
}

Value:

{
"tracking_number": "550e8400-e29b-41d4-a716-446655440000",
"created_date": "2025-03-16T10:00:00.000Z",
"arrival_date": null,
"status": "Sent",
"origin": "Kyiv",
"destination": "Lviv",
"last_updated": "2025-03-16T10:00:00.000Z",
"event_type": "Sent"
}

Event 2: Arrival at warehouse (Arrived)
Key:

{
"generation_timestamp": "2025-03-16T10:00:05.000Z",
"uuid": "b2c3d4e5-f6g7-8901-2345-678901bcdefg",
"service_name": "logistics_service",
"event_type": "Arrived"
}

Value:

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

Event 3: Delivered
Key:

{
"generation_timestamp": "2025-03-16T10:00:10.000Z",
"uuid": "c3d4e5f6-g7h8-9012-3456-789012cdefgh",
"service_name": "logistics_service",
"event_type": "Delivered"
}

Value:

{
"tracking_number": "550e8400-e29b-41d4-a716-446655440000",
"created_date": "2025-03-16T10:00:00.000Z",
"arrival_date": "2025-03-16T10:00:05.000Z",
"status": "Delivered",
"origin": "Kyiv",
"destination": "Lviv",
"last_updated": "2025-03-16T10:00:10.000Z",
"event_type": "Delivered"
}

Data transfer scheme

Launch

The mock data generator is launched as a Python script (kafka_producer.py), which connects to Kafka on localhost:9092 and MySQL on localhost:3306.

Sending
Every 10 seconds a new event is created for one parcel:
The "Sent" event is generated first with a new key.
After 10 seconds, "Arrived" with a new key.
After another 10 seconds, "Delivered" with a new key.

Each JSON (key + value):
Sent to the Kafka topic user-requests.
Written to the MySQL table parcels.

Consumption

Other systems can read data from the user-requests topic for analysis or display:

docker exec -it user-requesters-kafka-1 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user-requests --from-beginning

Potential Challenges
Time Synchronization: ISO 8601 with UTC unifies time, but additional coordination may be required for distributed systems.
Scaling: For large numbers of parcels, you need to optimize latency (reduce time.sleep) or add partitions to Kafka.

Implementation Steps in Code

Step 1: Register a Parcel (Sent)
Key Generation: New generation_timestamp, uuid, service_name="logistics_service", event_type="Sent".
Value generation: New tracking_number (UUID), created_date and last_updated are the current time, arrival_date are null, status and event_type are "Sent", random origin and destination.
Save to MySQL: Data is inserted into parcels table.
Send to Kafka: JSON with key and value is sent to user-requests.

Step 2: Arrive at warehouse (Arrived)
Key generation: New generation_timestamp, uuid, event_type="Arrived".
Value update**: arrival_date is the current time, status and event_type are "Arrived", last_updated is updated.
Save to MySQL: Data is updated in parcels with tracking_number.
Send to Kafka: Updated JSON with new key is sent to user-requests.

Step 3: Delivery (Delivered)
Key generation: New generation_timestamp, uuid, event_type="Delivered".
Value update: status and event_type are "Delivered", last_updated is updated.
Save to MySQL: Data is updated to parcels.
Send to Kafka: Final JSON with new key is sent to user-requests.

How it works:

Docker containers
mysql-container: MySQL 8.0 for logistics_db database with parcels table.
kafka-container: Kafka in KRaft mode for message broker.

Registration
The script generates a new parcel with a unique key and value for "Sent", writes to MySQL and sends to Kafka.

Update
After 10 seconds, an "Arrived" is generated with a new key, written to MySQL and Kafka.
After another 10 seconds, a "Delivered" with a new key, with the same actions.

Synchronization
Kafka provides a real-time data stream with unique keys for each event.
MySQL stores the latest status of each package by tracking_number.