import sys
import os
import asyncio
import json
import websockets
from aiohttp import web

# Insert the *parent* of "core/" so "from core.X import ..." resolves correctly.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.service import process_single_reading
from core.entity import Reading as CoreReading

HOST = "0.0.0.0"
WS_PORT   = 8003
HTTP_PORT = 8080   # dedicated health-check port (avoids abusing the WS handler)


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------

async def handle_client(websocket):
    """
    One coroutine per connected client.

    Message flow:
      CLIENT  →  {"type": "reading", "timestamp": ..., "sensor_id": ...,
                  "value": ..., "unit": ...}
      SERVER  →  {"type": "aggregate", "sensor_id": ..., "count": ...,
                  "min": ..., "max": ..., "avg": ...}

    The connection stays open — client keeps sending readings,
    server keeps streaming aggregates back, one per reading.
    """
    client_addr = websocket.remote_address
    print(f"[WS] Client connected: {client_addr}")

    try:
        async for raw_message in websocket:
            # --- Parse incoming JSON ---
            try:
                data = json.loads(raw_message)
            except json.JSONDecodeError:
                await websocket.send(json.dumps({
                    "type": "error",
                    "detail": "invalid JSON"
                }))
                continue

            # --- Validate message type ---
            if data.get("type") != "reading":
                await websocket.send(json.dumps({
                    "type": "error",
                    "detail": f"unknown message type: {data.get('type')}"
                }))
                continue

            # --- Validate required fields ---
            required = {"timestamp", "sensor_id", "value", "unit"}
            missing = required - data.keys()
            if missing:
                await websocket.send(json.dumps({
                    "type": "error",
                    "detail": f"missing fields: {list(missing)}"
                }))
                continue

            # --- Convert to shared Pydantic model ---
            try:
                reading = CoreReading(
                    timestamp=data["timestamp"],
                    sensor_id=data["sensor_id"],
                    value=float(data["value"]),
                    unit=data["unit"],
                )
            except Exception as e:
                await websocket.send(json.dumps({
                    "type": "error",
                    "detail": str(e)
                }))
                continue

            # --- Same business logic as HTTP and gRPC services ---
            stats = process_single_reading(reading)

            # --- Send aggregate back ---
            await websocket.send(json.dumps({
                "type":      "aggregate",
                "sensor_id": stats.sensor_id,
                "count":     stats.count,
                "min":       stats.min,
                "max":       stats.max,
                "avg":       round(stats.avg, 4),
            }))

    except websockets.exceptions.ConnectionClosedOK:
        print(f"[WS] Client disconnected cleanly: {client_addr}")
    except websockets.exceptions.ConnectionClosedError as e:
        print(f"[WS] Client disconnected with error: {client_addr} — {e}")


# ---------------------------------------------------------------------------
# HTTP health endpoint (aiohttp) — avoids docker-compose opening live WS
# connections just to health-check the service.
# ---------------------------------------------------------------------------

async def health_handler(request):
    return web.json_response({"status": "ok"})


async def run_health_server():
    app = web.Application()
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, HTTP_PORT)
    await site.start()
    print(f"[HTTP] Health endpoint: http://{HOST}:{HTTP_PORT}/health")


# ---------------------------------------------------------------------------
# Main — run WS server and HTTP health server concurrently
# ---------------------------------------------------------------------------

async def main():
    print(f"[WS] WebSocket server starting on ws://{HOST}:{WS_PORT}/telemetry")
    await run_health_server()
    async with websockets.serve(handle_client, HOST, WS_PORT, path="/telemetry"):
        await asyncio.Future()   # run forever


if __name__ == "__main__":
    asyncio.run(main())