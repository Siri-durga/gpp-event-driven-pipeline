from fastapi import FastAPI, Query, HTTPException
from typing import Optional
from pymongo import MongoClient
import logging
import json
import sys
import os

# -------------------------------
# Structured JSON Logging
# -------------------------------

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "service": "api_service",
            "message": record.getMessage()
        })

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())

logger = logging.getLogger()
logger.handlers = []
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# -------------------------------
# FastAPI App
# -------------------------------

app = FastAPI()

# -------------------------------
# MongoDB Configuration
# -------------------------------

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:rootpassword@mongodb:27017/"
)

try:
    client = MongoClient(MONGO_URI)
    db = client.sensor_db
    collection = db.events
    logger.info("Connected to MongoDB")
except Exception as e:
    logger.critical(f"Failed to connect to MongoDB: {e}")
    raise e

# -------------------------------
# MongoDB Query Logic
# -------------------------------

def query_mongodb(
    sensor_id: Optional[str],
    start_time: Optional[str],
    end_time: Optional[str]
):
    query = {}

    if sensor_id:
        query["sensor_id"] = sensor_id

    if start_time or end_time:
        query["timestamp"] = {}
        if start_time:
            query["timestamp"]["$gte"] = start_time
        if end_time:
            query["timestamp"]["$lte"] = end_time

    logger.info(f"Executing MongoDB query: {query}")

    try:
        results = []
        for doc in collection.find(query):
            doc["_id"] = str(doc["_id"])  # Mongo ObjectId → string
            results.append(doc)
        return results

    except Exception as e:
        logger.error(f"MongoDB query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

# -------------------------------
# API Endpoints
# -------------------------------

@app.get("/data")
def get_data(
    sensor_id: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None)
):
    data = query_mongodb(sensor_id, start_time, end_time)
    return {
        "count": len(data),
        "data": data
    }

@app.get("/health")
def health():
    return {"status": "healthy"}
