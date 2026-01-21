from fastapi import FastAPI, HTTPException
from pydantic import ValidationError
from kafka import KafkaProducer
import logging
import json
import os
import sys

from schemas import SensorData

# -----------------------------
# Structured JSON Logging
# -----------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "service": "ingestion_service",
            "message": record.getMessage()
        })

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())

logging.getLogger().handlers = []
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)

# -----------------------------
# App Initialization
# -----------------------------
app = FastAPI()

# -----------------------------
# Environment Configuration
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_events")

# -----------------------------
# Kafka Producer Initialization
# -----------------------------
producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all"
    )
    logger.info("Kafka producer initialized successfully")
except Exception as e:
    logger.critical(f"Kafka producer initialization failed: {e}")

# -----------------------------
# API Endpoints
# -----------------------------
@app.post("/ingest")
def ingest_sensor_data(data: dict):
    try:
        # Validate incoming payload
        validated = SensorData(**data)

        if producer is None:
            raise RuntimeError("Kafka producer not available")

        # Publish to Kafka
        producer.send(KAFKA_TOPIC, validated.model_dump())
        producer.flush()

        logger.info(f"Published sensor event sensor_id={validated.sensor_id}")
        return {"status": "success", "message": "Data accepted"}

    except ValidationError as e:
        logger.error(f"Validation failed: {e.errors()}")
        raise HTTPException(status_code=400, detail=e.errors())

    except Exception as e:
        logger.critical(f"Kafka publishing failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health")
def health():
    return {"status": "healthy"}
