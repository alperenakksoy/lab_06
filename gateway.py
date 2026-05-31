"""
gateway.py — Task 4: Protocol Gateway (REST → gRPC + MQTT fan-out)

Listens on POST /readings/fanout (HTTP, port 8004).

Request body:
  {
    "readings": [{"timestamp": ..., "sensor_id": ..., "value": ..., "unit": ...}, ...],
    "grpc_target": "192.168.X.Y:50051",
    "mqtt_brokers": ["192.168.X.Y:1883", ...]
  }

Behaviour:
  - Simultaneously sends readings to the gRPC service (bidirectional streaming)
    AND publishes each reading to every MQTT broker listed.
  - Collects all responses within FAN_OUT_TIMEOUT_S seconds.
  - If one protocol fails or times out, still returns results from the other.
  - All interactions are logged.

Response:
  {
    "grpc_aggregates": [...] | null,
    "mqtt_aggregates": [...] | null,
    "total_time_ms": 1500,
    "errors": {
      "grpc": "timeout" | "<error message>" | null,
      "mqtt": "timeout" | "<error message>" | null
    }
  }

Run:
  python gateway.py
  # or via docker-compose on port 8004
"""

import sys
import os
import json
import time
import asyncio
import logging
import threading
from typing import Optional

import grpc
import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import uvicorn

# Insert project root so core.* imports resolve
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import sensor_pb2
import sensor_pb2_grpc

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GATEWAY] %(levelname)s %(message)s",
)
log = logging.getLogger("gateway")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GATEWAY_HOST       = "0.0.0.0"
GATEWAY_PORT       = 8004
FAN_OUT_TIMEOUT_S  = 5.0          # max seconds to wait for each protocol
MQTT_COLLECT_S     = 3.0          # how long to collect MQTT aggregate replies

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ReadingIn(BaseModel):
    timestamp: int
    sensor_id: str
    value: float
    unit: str

class FanOutRequest(BaseModel):
    readings: List[ReadingIn]
    grpc_target: str                # e.g. "192.168.1.10:50051"
    mqtt_brokers: List[str]         # e.g. ["192.168.1.10:1883", "192.168.1.11:1883"]

class FanOutResponse(BaseModel):
    grpc_aggregates: Optional[list] = None
    mqtt_aggregates: Optional[list] = None
    total_time_ms: float
    errors: dict

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="HSRW Telemetry Gateway", version="1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/readings/fanout", response_model=FanOutResponse)
async def fan_out(req: FanOutRequest):
    """
    Fan out readings to gRPC service and MQTT brokers simultaneously.
    Returns merged results from both within FAN_OUT_TIMEOUT_S seconds.
    """
    if not req.readings:
        raise HTTPException(status_code=400, detail="readings list must not be empty")
    if not req.grpc_target:
        raise HTTPException(status_code=400, detail="grpc_target is required")
    if not req.mqtt_brokers:
        raise HTTPException(status_code=400, detail="mqtt_brokers list must not be empty")

    log.info(
        "Fan-out request: %d readings → gRPC(%s) + MQTT(%s)",
        len(req.readings), req.grpc_target, req.mqtt_brokers,
    )

    t_start = time.perf_counter()

    # Run gRPC and MQTT fan-out concurrently
    grpc_task = asyncio.create_task(_grpc_fan_out(req.grpc_target, req.readings))
    mqtt_task = asyncio.create_task(_mqtt_fan_out(req.mqtt_brokers, req.readings))

    grpc_result, mqtt_result = await asyncio.gather(grpc_task, mqtt_task)

    total_ms = round((time.perf_counter() - t_start) * 1000, 2)

    log.info(
        "Fan-out complete in %.1f ms | gRPC: %s | MQTT: %s",
        total_ms,
        "OK" if grpc_result["aggregates"] is not None else grpc_result["error"],
        "OK" if mqtt_result["aggregates"] is not None else mqtt_result["error"],
    )

    return FanOutResponse(
        grpc_aggregates=grpc_result["aggregates"],
        mqtt_aggregates=mqtt_result["aggregates"],
        total_time_ms=total_ms,
        errors={
            "grpc": grpc_result["error"],
            "mqtt": mqtt_result["error"],
        },
    )


# ---------------------------------------------------------------------------
# gRPC fan-out
# ---------------------------------------------------------------------------

async def _grpc_fan_out(target: str, readings: List[ReadingIn]) -> dict:
    """
    Open a bidirectional streaming RPC to `target`, send all readings,
    collect all AggregateStats responses.  Enforces FAN_OUT_TIMEOUT_S.
    """
    try:
        async def _call():
            channel = grpc.aio.insecure_channel(target)
            try:
                stub = sensor_pb2_grpc.SensorServiceStub(channel)

                async def generate():
                    for r in readings:
                        yield sensor_pb2.Reading(
                            timestamp=r.timestamp,
                            sensor_id=r.sensor_id,
                            value=r.value,
                            unit=r.unit,
                        )

                aggregates = []
                async for agg in stub.SubmitReadings(generate()):
                    aggregates.append({
                        "sensor_id": agg.sensor_id,
                        "count":     agg.count,
                        "min":       round(agg.min, 4),
                        "max":       round(agg.max, 4),
                        "avg":       round(agg.avg, 4),
                    })

                log.info("[gRPC] Received %d aggregates from %s", len(aggregates), target)
                return {"aggregates": aggregates, "error": None}
            finally:
                await channel.close()

        return await asyncio.wait_for(_call(), timeout=FAN_OUT_TIMEOUT_S)

    except asyncio.TimeoutError:
        log.warning("[gRPC] Timed out after %.1fs connecting to %s", FAN_OUT_TIMEOUT_S, target)
        return {"aggregates": None, "error": f"timeout after {FAN_OUT_TIMEOUT_S}s"}
    except Exception as e:
        log.error("[gRPC] Error: %s", e)
        return {"aggregates": None, "error": str(e)}


# ---------------------------------------------------------------------------
# MQTT fan-out
# ---------------------------------------------------------------------------

async def _mqtt_fan_out(brokers: List[str], readings: List[ReadingIn]) -> dict:
    """
    Publish all readings to every broker in `brokers` at QoS 1.
    Subscribe to sensors/aggregate/# on each broker and collect replies
    for MQTT_COLLECT_S seconds.  Runs synchronous paho calls in a thread
    so the asyncio event loop isn't blocked.
    """
    loop = asyncio.get_event_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(None, _mqtt_fan_out_sync, brokers, readings),
            timeout=FAN_OUT_TIMEOUT_S,
        )
        return result
    except asyncio.TimeoutError:
        log.warning("[MQTT] Fan-out timed out after %.1fs", FAN_OUT_TIMEOUT_S)
        return {"aggregates": None, "error": f"timeout after {FAN_OUT_TIMEOUT_S}s"}
    except Exception as e:
        log.error("[MQTT] Fan-out error: %s", e)
        return {"aggregates": None, "error": str(e)}


def _mqtt_fan_out_sync(brokers: List[str], readings: List[ReadingIn]) -> dict:
    """
    Synchronous MQTT logic (runs in a thread-pool worker).

    For each broker:
      1. Connect a subscriber client → sensors/aggregate/#
      2. Connect a publisher client → sensors/readings
      3. Publish all readings
      4. Collect aggregate replies for MQTT_COLLECT_S seconds
      5. Disconnect both clients

    Aggregates from all brokers are merged (de-duplicated by sensor_id,
    keeping the latest aggregate seen).
    """
    all_aggregates: dict = {}       # sensor_id → latest aggregate dict
    broker_errors: list  = []
    lock = threading.Lock()

    def _handle_one_broker(broker_str: str):
        """Connect to one broker, publish readings, collect aggregates."""
        try:
            host, port_str = broker_str.rsplit(":", 1)
            port = int(port_str)
        except ValueError:
            host, port = broker_str, 1883

        collected = {}
        sub_done  = threading.Event()
        sub_lock  = threading.Lock()

        # --- Subscriber ---
        sub = mqtt.Client(
            client_id=f"gw-sub-{host}-{port}",
            protocol=mqtt.MQTTv5,
        )

        def on_connect_sub(client, userdata, flags, rc, properties=None):
            rc_val = rc.value if hasattr(rc, "value") else int(rc)
            if rc_val == 0:
                client.subscribe("sensors/aggregate/#", qos=1)
                log.info("[MQTT] Subscribed on %s:%d", host, port)
            else:
                log.warning("[MQTT] Sub connect failed rc=%s on %s:%d", rc, host, port)

        def on_message(client, userdata, msg):
            try:
                data = json.loads(msg.payload.decode())
                sid  = data.get("sensor_id", "unknown")
                with sub_lock:
                    collected[sid] = data
            except Exception as e:
                log.warning("[MQTT] Bad aggregate payload: %s", e)

        sub.on_connect = on_connect_sub
        sub.on_message = on_message

        try:
            sub.connect(host, port, keepalive=10)
        except Exception as e:
            broker_errors.append(f"{broker_str}: {e}")
            log.error("[MQTT] Cannot connect subscriber to %s: %s", broker_str, e)
            return

        sub.loop_start()

        # --- Publisher ---
        pub = mqtt.Client(
            client_id=f"gw-pub-{host}-{port}",
            protocol=mqtt.MQTTv5,
        )
        try:
            pub.connect(host, port, keepalive=10)
        except Exception as e:
            sub.loop_stop()
            sub.disconnect()
            broker_errors.append(f"{broker_str}: {e}")
            log.error("[MQTT] Cannot connect publisher to %s: %s", broker_str, e)
            return

        pub.loop_start()

        # Publish all readings
        for r in readings:
            payload = json.dumps({
                "timestamp": r.timestamp,
                "sensor_id": r.sensor_id,
                "value":     r.value,
                "unit":      r.unit,
            })
            pub.publish("sensors/readings", payload=payload, qos=1)

        log.info("[MQTT] Published %d readings to %s:%d", len(readings), host, port)

        # Collect aggregate replies
        time.sleep(MQTT_COLLECT_S)

        pub.loop_stop()
        sub.loop_stop()
        pub.disconnect()
        sub.disconnect()

        with lock:
            all_aggregates.update(collected)

        log.info("[MQTT] Collected %d aggregates from %s:%d", len(collected), host, port)

    # Fan out to all brokers in parallel threads
    threads = [threading.Thread(target=_handle_one_broker, args=(b,), daemon=True)
               for b in brokers]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=FAN_OUT_TIMEOUT_S)

    if not all_aggregates and broker_errors:
        return {
            "aggregates": None,
            "error": "; ".join(broker_errors),
        }

    return {
        "aggregates": list(all_aggregates.values()),
        "error":      "; ".join(broker_errors) if broker_errors else None,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Gateway starting on http://%s:%d", GATEWAY_HOST, GATEWAY_PORT)
    uvicorn.run(app, host=GATEWAY_HOST, port=GATEWAY_PORT, log_level="info")
