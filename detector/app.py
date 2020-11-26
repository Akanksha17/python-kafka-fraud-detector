from kafka import KafkaConsumer
import os
import json

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")

consumer = KafkaConsumer(
    TRANSACTIONS_TOPIC,
    bootstrap_servers=KAFKA_BROKER_URL,
    value_deserializer=json.loads,
)

for message in consumer:
    transaction = message.value

