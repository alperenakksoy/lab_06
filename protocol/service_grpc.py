import sys
import os
import asyncio
import json
import websockets

# Allow imports from the core/ directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "core"))

from core.service import process_single_reading
from core.entity import Reading as CoreReading


async def telemetry_handler(websocket):
    """
    Handles a long-lived bidirectional WebSocket connection.
    Listens for text-based JSON reading messages and immediately
    sends back JSON aggregate messages.
    """
    try:
        async for message in websocket:
            try:
                data = json.loads(message)

                # Verify the incoming message type
                if data.get("type") == "reading":
                    # Convert incoming JSON to your Pydantic model
                    reading = CoreReading(
                        timestamp=data["timestamp"],
                        sensor_id=data["sensor_id"],
                        value=data["value"],
                        unit=data["unit"]
                    )

                    # Process the data using your unified business logic
                    stats = process_single_reading(reading)

                    # Construct the required JSON response
                    response = {
                        "type": "aggregate",
                        "sensor_id": stats.sensor_id,
                        "count": stats.count,
                        "min": stats.min,
                        "max": stats.max,
                        "avg": round(stats.avg, 4)
                    }

                    # Send it back to the client
                    await websocket.send(json.dumps(response))

            except json.JSONDecodeError:
                error_msg = json.dumps({"error": "Invalid JSON format."})
                await websocket.send(error_msg)
            except Exception as e:
                error_msg = json.dumps({"error": str(e)})
                await websocket.send(error_msg)

    except websockets.exceptions.ConnectionClosed:
        print("WebSocket client disconnected.")


async def serve():
    # The lab requires the WebSocket service to run on port 8003
    host = "0.0.0.0"
    port = 8003
    print(f"WebSocket server starting on ws://{host}:{port}/telemetry")

    # We don't specify the '/telemetry' path strictly here to keep it simple,
    # but the client will connect to ws://localhost:8003/telemetry
    async with websockets.serve(telemetry_handler, host, port):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(serve())