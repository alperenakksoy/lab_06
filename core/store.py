from typing import Dict, List

# In-memory veri deposu
_db: Dict[str, List[float]] = {}

def add_reading_to_store(sensor_id: str, value: float):
    if sensor_id not in _db:
        _db[sensor_id] = []
    _db[sensor_id].append(value)

def get_values_for_sensor(sensor_id: str) -> List[float]:
    return _db.get(sensor_id, [])