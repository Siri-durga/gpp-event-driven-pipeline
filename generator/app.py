import os
import time
import random
import requests
from datetime import datetime
from fastapi import FastAPI
import threading
import logging
import json
import sys

# ---------- structured logging ----------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "service": "generator",
            "message": record.getMessage()
        })

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logging.getLogger().handlers = []
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

# ---------- config ----------
INGEST_URL = os.getenv(
    "INGEST_URL",
    "http://ingestion_service:8000/ingest"
)
GENERATOR_RATE_SECONDS = int(os.getenv("GENERATOR_RATE_SECONDS", "2"))

# ---------- fastapi app ----------
app = FastAPI()

@app.get("/health")
def health():
    return {"status": "healthy"}

# ---------- generator logic ----------
def generate_sensor_data():
    return {
        "sensor_id": f"sensor_{random.randint(1,5)}",
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(20, 35), 2),
        "humidity": round(random.uniform(40, 80), 2),
        "location": {
            "latitude": round(random.uniform(-90, 90), 4),
            "longitude": round(random.uniform(-180, 180), 4)
        }
    }

def run_generator():
    logging.info("Generator started")
    while True:
        data = generate_sensor_data()
        try:
            r = requests.post(INGEST_URL, json=data, timeout=5)
            logging.info(f"Sent data status={r.status_code}")
        except Exception as e:
            logging.error(f"Failed to send data: {e}")
        time.sleep(GENERATOR_RATE_SECONDS)

def start_background_generator():
    t = threading.Thread(target=run_generator, daemon=True)
    t.start()

start_background_generator()
