import sys
import os
import asyncio
import grpc
from concurrent import futures

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.service import process_single_reading
from core.entity import Reading as CoreReading

import sensor_pb2
import sensor_pb2_grpc


class SensorServiceServicer(sensor_pb2_grpc.SensorServiceServicer):

    async def SubmitReadings(self, request_iterator, context):
        async for pb_reading in request_iterator:
            # Convert protobuf Reading → our shared Pydantic model
            core_reading = CoreReading(
                timestamp=pb_reading.timestamp,
                sensor_id=pb_reading.sensor_id,
                value=pb_reading.value,
                unit=pb_reading.unit,
            )

            stats = process_single_reading(core_reading)

            yield sensor_pb2.AggregateStats(
                sensor_id=stats.sensor_id,
                count=stats.count,
                min=stats.min,
                max=stats.max,
                avg=round(stats.avg, 4),
            )


async def serve():
    server = grpc.aio.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ("grpc.max_send_message_length",    10 * 1024 * 1024),
            ("grpc.max_receive_message_length", 10 * 1024 * 1024),
        ],
    )
    sensor_pb2_grpc.add_SensorServiceServicer_to_server(SensorServiceServicer(), server)

    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    print(f"gRPC server listening on {listen_addr}")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())