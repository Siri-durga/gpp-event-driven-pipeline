from fastapi import FastAPI, Query
from typing import Optional
from pymongo import MongoClient
import logging
import os
from datetime import datetime

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:rootpassword@mongodb:27017/"
)

client = MongoClient(MONGO_URI)
db = client.sensor_db
collection = db.events

logging.info("Connected to MongoDB")

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

    logging.info(f"MongoDB query: {query}")

    results = []
    for doc in collection.find(query):
        doc["_id"] = str(doc["_id"])
        results.append(doc)

    return results

@app.get("/data")
def get_data(
    sensor_id: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None)
):
    data = query_mongodb(sensor_id, start_time, end_time)
    return {"count": len(data), "data": data}

@app.get("/health")
def health():
    return {"status": "ok"}
