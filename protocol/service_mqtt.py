import sys
import os
import json
import time
import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.service import process_single_reading
from core.entity import Reading as CoreReading

# Config
BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "mqtt-broker")   # Docker hostname
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))

SERVICE_QOS = int(os.getenv("MQTT_SERVICE_QOS", 1))

SUBSCRIBE_TOPIC = "sensors/readings"
PUBLISH_TOPIC   = "sensors/aggregate/{sid}"


# Callbacks
def on_connect(client, userdata, flags, reason_code, properties=None):
    rc_value = reason_code.value if hasattr(reason_code, "value") else int(reason_code)
    if rc_value == 0:
        print(f"[MQTT] Connected to broker {BROKER_HOST}:{BROKER_PORT} (QoS {SERVICE_QOS})")
        client.subscribe(SUBSCRIBE_TOPIC, qos=2)
        print(f"[MQTT] Subscribed to '{SUBSCRIBE_TOPIC}' (QoS 2)")
    else:
        print(f"[MQTT] Connection failed, reason code: {reason_code}")


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"[MQTT] Bad payload on {msg.topic}: {e}")
        return

    required = {"timestamp", "sensor_id", "value", "unit"}
    missing = required - data.keys()
    if missing:
        print(f"[MQTT] Missing fields {missing} — skipping message")
        return

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

    stats = process_single_reading(reading)

    publish_topic = PUBLISH_TOPIC.format(sid=stats.sensor_id)

    payload = json.dumps({
        "sensor_id": stats.sensor_id,
        "count":     stats.count,
        "min":       stats.min,
        "max":       stats.max,
        "avg":       round(stats.avg, 4),
        "qos":       SERVICE_QOS,
    })
    result = client.publish(publish_topic, payload=payload, qos=SERVICE_QOS)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        print(f"[MQTT] Publish failed QoS {SERVICE_QOS}: rc={result.rc}")

    print(f"[MQTT] Processed {stats.sensor_id}: count={stats.count} avg={stats.avg:.2f}")


def on_disconnect(client, userdata, disconnect_flags, reason_code=None, properties=None):
    if reason_code is not None:
        rc_value = reason_code.value if hasattr(reason_code, "value") else int(reason_code)
        if rc_value != 0:
            print(f"[MQTT] Unexpected disconnect (rc={reason_code}), retrying...")


def on_publish(client, userdata, mid, reason_codes=None, properties=None):
    pass

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

def main():
    client = mqtt.Client(
        client_id=f"telemetry-server-qos{SERVICE_QOS}",
        protocol=mqtt.MQTTv5,
    )

    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect
    client.on_publish    = on_publish

    connect_with_retry(client, BROKER_HOST, BROKER_PORT)

    print(f"[MQTT] Entering network loop — waiting for readings (QoS {SERVICE_QOS})...")
    client.loop_forever()


if __name__ == "__main__":
    main()