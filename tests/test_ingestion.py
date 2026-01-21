from ingestion_service.schemas import SensorData
import pytest
from datetime import datetime

def test_valid_sensor_data():
    data = {
        "sensor_id": "sensor_1",
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": 25.5,
        "humidity": 60.0,
        "location": {"latitude": 10.0, "longitude": 20.0}
    }
    sensor = SensorData(**data)
    assert sensor.sensor_id == "sensor_1"

def test_invalid_latitude():
    data = {
        "sensor_id": "sensor_1",
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": 25.5,
        "humidity": 60.0,
        "location": {"latitude": 200.0, "longitude": 20.0}
    }
    with pytest.raises(Exception):
        SensorData(**data)
