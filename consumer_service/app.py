import json
import logging
import os
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_events")
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:rootpassword@mongodb:27017/"
)

# --- MongoDB setup ---
mongo_client = MongoClient(MONGO_URI)
db = mongo_client.sensor_db
collection = db.events

# Enforce idempotency at DB level
collection.create_index(
    [("sensor_id", 1), ("timestamp", 1)],
    unique=True
)

logging.info("Connected to MongoDB and ensured unique index")

# --- Kafka consumer ---
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

logging.info("Kafka consumer started")

def process_and_save_message(message: dict):
    try:
        unique_id = f"{message['sensor_id']}-{message['timestamp']}"
        message["_id"] = unique_id

        collection.insert_one(message)
        logging.info(f"Saved event {unique_id}")

    except DuplicateKeyError:
        logging.warning("Duplicate event detected, skipping")

    except Exception as e:
        logging.error(f"MongoDB write failed: {e}")

for msg in consumer:
    process_and_save_message(msg.value)
