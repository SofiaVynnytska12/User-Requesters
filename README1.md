# User-Requesters
This project is designed to handle logistics data efficiently, including shipment, receipt, status change of parcels, unique identification numbers, and arrival dates to warehouses. It utilizes a distributed system approach with Apache Kafka for messaging and Docker for containerization.

Programming language: Python

Database: MySQL

Message Broker: Apache Kafka

Containerization: Docker (Bitnami Kafka & Zookeeper images)  

IDE: PyCharm (with Big Data Tools)


## How to Start Kafka and Zookeeper

Run the following commands to start the Kafka and Zookeeper containers:

```
docker start zookeeper
docker start kafka
```
To stop them:
```
docker stop kafka zookeeper
```

Send a message to Kafka topic (`user-requests`)
```
docker exec -it kafka kafka-console-producer.sh --broker-list localhost:9092 --topic user-requests
```
Then type your message:
```
{"message": "Hello Kafka!"}
```
Consume messages from Kafka (user-requests)
```
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user-requests --from-beginning
```
