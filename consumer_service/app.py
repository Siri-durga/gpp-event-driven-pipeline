import json
import logging
import os
import sys
import threading
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from fastapi import FastAPI
import uvicorn

# =========================
# Structured JSON Logging
# =========================
class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "service": "consumer_service",
            "message": record.getMessage()
        })

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())

logging.getLogger().handlers = []
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)

# =========================
# Environment Variables
# =========================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_events")
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:rootpassword@mongodb:27017/"
)

# =========================
# MongoDB Setup
# =========================
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client.sensor_db
    collection = db.events

    collection.create_index(
        [("sensor_id", 1), ("timestamp", 1)],
        unique=True
    )

    logger.info("Connected to MongoDB and ensured unique index")

except Exception as e:
    logger.critical(f"MongoDB connection failed: {e}")
    sys.exit(1)

# =========================
# Kafka Consumer Setup
# =========================
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    logger.info("Kafka consumer started")

except Exception as e:
    logger.critical(f"Kafka consumer initialization failed: {e}")
    sys.exit(1)

# =========================
# Core Processing Logic
# =========================
def process_and_save_message(message: dict):
    try:
        unique_id = f"{message['sensor_id']}-{message['timestamp']}"
        message["_id"] = unique_id

        collection.insert_one(message)
        logger.info(f"Saved event {unique_id}")

    except DuplicateKeyError:
        logger.warning(f"Duplicate event {unique_id} detected, skipping")

    except Exception as e:
        logger.error(f"MongoDB write failed: {e}")

# =========================
# Health Check API
# =========================
health_app = FastAPI()

@health_app.get("/health")
def health():
    return {"status": "healthy"}

def run_health_server():
    uvicorn.run(health_app, host="0.0.0.0", port=8002)

threading.Thread(target=run_health_server, daemon=True).start()

# =========================
# Consumer Loop
# =========================
for msg in consumer:
    process_and_save_message(msg.value)
