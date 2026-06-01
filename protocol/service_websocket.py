import sys
import os
import asyncio
import json
import websockets
from aiohttp import web

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.service import process_single_reading
from core.entity import Reading as CoreReading

HOST = "0.0.0.0"
WS_PORT   = 8003
HTTP_PORT = 8080

async def handle_client(websocket):
    path = websocket.request.path if hasattr(websocket, "request") else "/telemetry"
    if path != "/telemetry":
        await websocket.close(1008, "wrong path")
        return

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

            if data.get("type") != "reading":
                await websocket.send(json.dumps({
                    "type": "error",
                    "detail": f"unknown message type: {data.get('type')}"
                }))
                continue

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

async def main():
    print(f"[WS] WebSocket server starting on ws://{HOST}:{WS_PORT} (path: /telemetry)")
    await run_health_server()
    async with websockets.serve(handle_client, HOST, WS_PORT):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())