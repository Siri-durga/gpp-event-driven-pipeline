from pydantic import BaseModel, Field
from datetime import datetime

class Location(BaseModel):
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)

class SensorData(BaseModel):
    sensor_id: str
    timestamp: datetime
    temperature: float
    humidity: float
    location: Location
