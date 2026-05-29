from typing import List
from fastapi import FastAPI, HTTPException
from core.entity import ReadingsPayload, AggregateStats
from core.service import process_single_reading

app = FastAPI(title="HSRW Telemetry Service - REST/HTTP/1.1")

@app.post("/readings", response_model=List[AggregateStats])
def post_readings(payload: ReadingsPayload):
    if not payload.readings:
        raise HTTPException(status_code=400, detail="readings list must not be empty")

    seen: dict[str, AggregateStats] = {}
    for reading in payload.readings:
        stats = process_single_reading(reading)
        seen[stats.sensor_id] = stats  # keeps latest aggregate per sensor

    return list(seen.values())

@app.get("/health")
def health_check():
    return {"status": "ok"}