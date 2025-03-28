from confluent_kafka import Producer
import json

config = {'bootstrap.servers': 'localhost:9092'}

producer = Producer(config)

def delivery_report(err, msg):
    if err is not None:
        print(f'Ошибка доставки: {err}')
    else:
        print(f'Сообщение отправлено в {msg.topic()} [{msg.partition()}]')

test_data = {"user": "Alice", "action": "login"}
producer.produce('test-topic', key='key1', value=json.dumps(test_data), callback=delivery_report)

producer.flush()
