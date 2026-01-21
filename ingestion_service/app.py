from fastapi import FastAPI, HTTPException
import logging
import json

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def publish_to_kafka(topic: str, message: dict):
    # placeholder
    logging.info(f"[PLACEHOLDER] Publishing to Kafka topic={topic} message={message}")

@app.post("/ingest")
def ingest(data: dict):
    logging.info(f"Received data: {json.dumps(data)}")
    publish_to_kafka("sensor-data", data)
    return {"status": "received"}

@app.get("/health")
def health():
    return {"status": "ok"}
