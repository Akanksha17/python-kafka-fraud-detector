import json
from kafka import KafkaProducer
import os
from time import sleep
from transactions import create_random_transaction
# from .transactions import create_random_transaction

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")
SLEEP_TIME = 1

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda value: json.dumps(value).encode()
    )
    while True:
        transaction = create_random_transaction()
        print(transaction)
        producer.send(TRANSACTIONS_TOPIC, value=transaction)
        sleep(SLEEP_TIME)

