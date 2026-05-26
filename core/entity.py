from pydantic import BaseModel
from typing import List, Optional


class Reading(BaseModel):
    timestamp: int
    sensor_id: str
    value: float
    unit: str
    qos: Optional[int] = None

class ReadingsPayload(BaseModel):
    readings: List[Reading]

class AggregateStats(BaseModel):
    sensor_id: str
    count: int
    min: float
    max: float
    avg: float