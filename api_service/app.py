from fastapi import FastAPI, Query
import logging
from typing import Optional

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

def query_mongodb(
    sensor_id: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None
):
    # placeholder
    logging.info(
        f"[PLACEHOLDER] Query MongoDB "
        f"sensor_id={sensor_id}, start_time={start_time}, end_time={end_time}"
    )
    return [{"message": "static response (mongo not wired yet)"}]

@app.get("/data")
def get_data(
    sensor_id: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None)
):
    result = query_mongodb(sensor_id, start_time, end_time)
    return {"data": result}

@app.get("/health")
def health():
    return {"status": "ok"}
