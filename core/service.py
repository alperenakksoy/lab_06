from entity import Reading, AggregateStats
from store import add_reading_to_store, get_values_for_sensor


def process_single_reading(reading: Reading) -> AggregateStats:
    add_reading_to_store(reading.sensor_id, reading.value)
    values = get_values_for_sensor(reading.sensor_id)

    count = len(values)
    return AggregateStats(
        sensor_id=reading.sensor_id,
        count=count,
        min=min(values),
        max=max(values),
        avg=sum(values) / count
    )