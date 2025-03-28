from confluent_kafka import Consumer, KafkaException
import json


def create_consumer():
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(config)
    consumer.subscribe(['test-topic'])

    try:
        with open("test_data.json", "a") as f:  # Открываем файл для записи
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                data = {
                    "key": msg.key().decode() if msg.key() else None,
                    "value": msg.value().decode()
                }

                print(f'Получено сообщение: {data}')

                # Записываем JSON-объект в файл
                json.dump(data, f)
                f.write("\n")  # Добавляем перенос строки для читаемости

    except KeyboardInterrupt:
        print("Завершение работы...")
    finally:
        consumer.close()


if __name__ == "__main__":
    create_consumer()
