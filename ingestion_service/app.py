from fastapi import FastAPI, HTTPException
from pydantic import ValidationError
from kafka import KafkaProducer
import logging
import json
import os

from schemas import SensorData

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_events")

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )
    logging.info("Kafka producer initialized")
except Exception as e:
    producer = None
    logging.critical(f"Kafka producer initialization failed: {e}")

@app.post("/ingest")
def ingest_sensor_data(data: dict):
    try:
        validated = SensorData(**data)

        if producer is None:
            raise RuntimeError("Kafka producer not available")

        producer.send(KAFKA_TOPIC, validated.model_dump())
        producer.flush()

        logging.info(f"Published data for sensor_id={validated.sensor_id}")
        return {"status": "success", "message": "Data accepted"}

    except ValidationError as e:
        logging.error(f"Validation failed: {e.errors()}")
        raise HTTPException(status_code=400, detail=e.errors())

    except Exception as e:
        logging.critical(f"Kafka publish failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health")
def health():
    return {"status": "ok"}
