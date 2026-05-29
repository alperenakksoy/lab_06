import sys
import os
import json
import time
import threading
import paho.mqtt.client as mqtt

# Allow imports from shared/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "core"))

from core.service import process_single_reading
from core.entity import Reading as CoreReading

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "mqtt-broker")  # docker hostname
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))

SUBSCRIBE_TOPIC  = "sensors/readings"          # clients publish here
PUBLISH_TOPIC    = "sensors/aggregate/{sid}"   # server publishes here

QOS_LEVELS = [0, 1, 2]   # all three variants are handled by this single service


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print(f"[MQTT] Connected to broker {BROKER_HOST}:{BROKER_PORT}")
        # Subscribe at QoS 2 — broker will deliver at the highest requested level
        # Our service accepts readings at any QoS the publisher chose
        client.subscribe(SUBSCRIBE_TOPIC, qos=2)
        print(f"[MQTT] Subscribed to '{SUBSCRIBE_TOPIC}' (QoS 2)")
    else:
        print(f"[MQTT] Connection failed, reason code: {reason_code}")


def on_message(client, userdata, msg):
    """
    Called every time a reading arrives on sensors/readings.

    Flow:
      1. Parse JSON payload
      2. Convert to shared CoreReading model
      3. process_single_reading() → same shared logic as HTTP / gRPC / WS
      4. Publish aggregate to sensors/aggregate/{sensor_id} at QoS 0, 1, AND 2
    """
    try:
        data = json.loads(msg.payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"[MQTT] Bad payload on {msg.topic}: {e}")
        return

    # --- Validate fields ---
    required = {"timestamp", "sensor_id", "value", "unit"}
    missing = required - data.keys()
    if missing:
        print(f"[MQTT] Missing fields {missing} — skipping message")
        return

    # --- Convert to shared Pydantic model ---
    try:
        reading = CoreReading(
            timestamp=data["timestamp"],
            sensor_id=data["sensor_id"],
            value=float(data["value"]),
            unit=data["unit"],
        )
    except Exception as e:
        print(f"[MQTT] Model validation error: {e}")
        return

    # --- Same business logic as all other services ---
    stats = process_single_reading(reading)

    publish_topic = PUBLISH_TOPIC.format(sid=stats.sensor_id)

    # --- Publish one aggregate message per QoS level ---
    # This satisfies the lab requirement: QoS 0, 1, and 2 variants
    for qos in QOS_LEVELS:
        payload = json.dumps({
            "sensor_id": stats.sensor_id,
            "count":     stats.count,
            "min":       stats.min,
            "max":       stats.max,
            "avg":       round(stats.avg, 4),
            "qos":       qos,           # lab spec: include qos field in aggregate
        })
        result = client.publish(publish_topic, payload=payload, qos=qos)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"[MQTT] Publish failed QoS {qos}: rc={result.rc}")

    print(f"[MQTT] Processed {stats.sensor_id}: count={stats.count} avg={stats.avg:.2f}")


def on_disconnect(client, userdata, disconnect_flags, reason_code=None, properties=None):
    if reason_code != 0:
        print(f"[MQTT] Unexpected disconnect (rc={reason_code}), retrying...")


def on_publish(client, userdata, mid, reason_codes=None, properties=None):
    pass  # could log delivery confirmations here for QoS 1/2 debugging


# ---------------------------------------------------------------------------
# Reconnect loop — keeps the service alive if broker restarts
# ---------------------------------------------------------------------------

def connect_with_retry(client, host, port, retries=10, delay=3):
    for attempt in range(1, retries + 1):
        try:
            print(f"[MQTT] Connecting to {host}:{port} (attempt {attempt}/{retries})")
            client.connect(host, port, keepalive=60)
            return
        except (ConnectionRefusedError, OSError) as e:
            print(f"[MQTT] Connection failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError(f"[MQTT] Could not connect to broker after {retries} attempts")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    client = mqtt.Client(
        client_id="telemetry-server",
        protocol=mqtt.MQTTv5,
    )

    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect
    client.on_publish    = on_publish

    connect_with_retry(client, BROKER_HOST, BROKER_PORT)

    print("[MQTT] Entering network loop — waiting for readings...")
    client.loop_forever()   # blocking; handles reconnects automatically


if __name__ == "__main__":
    main()