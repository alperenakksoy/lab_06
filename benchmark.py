import sys
import os
import json
import time
import random
import asyncio
import threading
import statistics
import requests
import websockets
import paho.mqtt.client as mqtt
import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "core"))

from core.entity import Reading as CoreReading

# gRPC stubs — generated from sensor.proto
import sensor_pb2
import sensor_pb2_grpc

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HTTP_URL    = os.getenv("HTTP_URL",    "http://localhost:8001")
GRPC_HOST   = os.getenv("GRPC_HOST",  "localhost:50051")
WS_URL      = os.getenv("WS_URL",     "ws://localhost:8003/telemetry")
MQTT_HOST   = os.getenv("MQTT_HOST",  "localhost")
MQTT_PORT   = int(os.getenv("MQTT_PORT", 1883))

TOTAL_READINGS  = 1000
SENSOR_COUNT    = 5                          # s1 … s5
READINGS_EACH   = TOTAL_READINGS // SENSOR_COUNT   # 200 per sensor

RESULTS_FILE = "benchmark_results.json"

# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

def make_readings(n_sensors=SENSOR_COUNT, per_sensor=READINGS_EACH):
    """Generate deterministic test readings: 5 sensors × 200 readings each."""
    readings = []
    base_ts = 1716864000
    for i in range(per_sensor):
        for s in range(1, n_sensors + 1):
            readings.append({
                "timestamp": base_ts + i,
                "sensor_id": f"s{s}",
                "value":     round(random.uniform(10.0, 40.0), 2),
                "unit":      "C",
            })
    return readings   # 1000 total

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_ms():
    return time.perf_counter() * 1000

def percentile(data, p):
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * p / 100)
    return sorted_data[min(idx, len(sorted_data) - 1)]

# ---------------------------------------------------------------------------
# 1. HTTP/1.1 benchmark
# ---------------------------------------------------------------------------

def bench_http(readings):
    print("\n[HTTP] Starting benchmark...")
    latencies = []
    total_bytes = 0

    # Connection setup time
    t0 = now_ms()
    resp = requests.get(f"{HTTP_URL}/health", timeout=5)
    setup_time = now_ms() - t0
    assert resp.status_code == 200

    start = now_ms()
    for reading in readings:
        payload = {"readings": [reading]}
        t0 = now_ms()
        resp = requests.post(f"{HTTP_URL}/readings", json=payload, timeout=10)
        latencies.append(now_ms() - t0)
        total_bytes += len(resp.content) + len(json.dumps(payload).encode())
        assert resp.status_code == 200

    total_time_s = (now_ms() - start) / 1000

    return {
        "protocol":        "http1",
        "setup_time_ms":   round(setup_time, 2),
        "latency_ms":      round(statistics.mean(latencies), 2),
        "latency_p95_ms":  round(percentile(latencies, 95), 2),
        "latency_p99_ms":  round(percentile(latencies, 99), 2),
        "throughput_msg_sec": round(len(readings) / total_time_s, 2),
        "bytes_total":     total_bytes,
    }

# ---------------------------------------------------------------------------
# 2. gRPC benchmark
# ---------------------------------------------------------------------------

def bench_grpc(readings):
    print("\n[gRPC] Starting benchmark...")
    latencies = []
    total_bytes = 0

    # Connection setup time
    t0 = now_ms()
    channel = grpc.insecure_channel(GRPC_HOST)
    stub = sensor_pb2_grpc.SensorServiceStub(channel)
    grpc.channel_ready_future(channel).result(timeout=10)
    setup_time = now_ms() - t0

    def reading_generator(batch):
        nonlocal total_bytes  # FIX: Allows modification of the outer variable
        for r in batch:
            pb = sensor_pb2.Reading(
                timestamp=r["timestamp"],
                sensor_id=r["sensor_id"],
                value=r["value"],
                unit=r["unit"],
            )
            total_bytes += pb.ByteSize()
            yield pb

    start = now_ms()

    # Bidirectional streaming — send all, collect all responses
    t0 = now_ms()
    responses = list(stub.SubmitReadings(reading_generator(readings)))
    rtt = now_ms() - t0
    latencies = [rtt / len(readings)] * len(readings)   # avg per message

    for r in responses:
        total_bytes += r.ByteSize()

    total_time_s = (now_ms() - start) / 1000
    channel.close()

    return {
        "protocol":           "grpc",
        "setup_time_ms":      round(setup_time, 2),
        "latency_ms":         round(statistics.mean(latencies), 2),
        "latency_p95_ms":     round(percentile(latencies, 95), 2),
        "latency_p99_ms":     round(percentile(latencies, 99), 2),
        "throughput_msg_sec": round(len(readings) / total_time_s, 2),
        "bytes_total":        total_bytes,
    }

# ---------------------------------------------------------------------------
# 3. WebSocket benchmark
# ---------------------------------------------------------------------------

async def _ws_bench(readings):
    latencies = []
    total_bytes = 0

    t0 = now_ms()
    async with websockets.connect(WS_URL) as ws:
        setup_time = now_ms() - t0

        start = now_ms()
        for reading in readings:
            msg = json.dumps({"type": "reading", **reading})
            t0 = now_ms()
            await ws.send(msg)
            resp = await ws.recv()
            latencies.append(now_ms() - t0)
            total_bytes += len(msg.encode()) + len(resp.encode())

        total_time_s = (now_ms() - start) / 1000

    return {
        "protocol":           "websocket",
        "setup_time_ms":      round(setup_time, 2),
        "latency_ms":         round(statistics.mean(latencies), 2),
        "latency_p95_ms":     round(percentile(latencies, 95), 2),
        "latency_p99_ms":     round(percentile(latencies, 99), 2),
        "throughput_msg_sec": round(len(readings) / total_time_s, 2),
        "bytes_total":        total_bytes,
    }

def bench_websocket(readings):
    print("\n[WebSocket] Starting benchmark...")
    return asyncio.run(_ws_bench(readings))

# ---------------------------------------------------------------------------
# 4. MQTT benchmark  (QoS 0, 1, 2)
# ---------------------------------------------------------------------------

def bench_mqtt(readings, qos=1):
    print(f"\n[MQTT QoS {qos}] Starting benchmark...")
    latencies   = []
    total_bytes = [0]  # FIX: Converted to list to bypass scope limitations
    received    = threading.Event()
    msg_count   = [0]
    lock        = threading.Lock()

    # --- Subscriber client ---
    sub_client = mqtt.Client(client_id=f"bench-sub-qos{qos}", protocol=mqtt.MQTTv5)

    def on_sub_message(client, userdata, msg):
        with lock:
            msg_count[0] += 1
            total_bytes[0] += len(msg.payload)  # FIX: Safely incrementing bytes
        if msg_count[0] >= len(readings):
            received.set()

    sub_client.on_message = on_sub_message
    sub_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    sub_client.subscribe("sensors/aggregate/#", qos=qos)
    sub_client.loop_start()

    # --- Publisher client ---
    pub_client = mqtt.Client(client_id=f"bench-pub-qos{qos}", protocol=mqtt.MQTTv5)

    t0 = now_ms()
    pub_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    setup_time = now_ms() - t0
    pub_client.loop_start()

    start = now_ms()
    for reading in readings:
        payload = json.dumps(reading)
        t_send = now_ms()
        pub_client.publish("sensors/readings", payload=payload, qos=qos)
        latencies.append(now_ms() - t_send)   # publish RTT (no ack for QoS 0)
        total_bytes[0] += len(payload.encode())  # FIX: Safely incrementing bytes

    # Wait for all aggregates to be received (max 30s)
    received.wait(timeout=30)
    total_time_s = (now_ms() - start) / 1000

    pub_client.loop_stop()
    sub_client.loop_stop()
    pub_client.disconnect()
    sub_client.disconnect()

    return {
        "protocol":           f"mqtt_qos{qos}",
        "setup_time_ms":      round(setup_time, 2),
        "latency_ms":         round(statistics.mean(latencies), 2),
        "latency_p95_ms":     round(percentile(latencies, 95), 2),
        "latency_p99_ms":     round(percentile(latencies, 99), 2),
        "throughput_msg_sec": round(len(readings) / total_time_s, 2),
        "bytes_total":        total_bytes[0],  # FIX: Extracting the value from the list
        "messages_received":  msg_count[0],
    }

# ---------------------------------------------------------------------------
# Main — run all benchmarks and save results
# ---------------------------------------------------------------------------

def main():
    random.seed(42)   # reproducible values
    readings = make_readings()
    print(f"Generated {len(readings)} readings across {SENSOR_COUNT} sensors")

    results = []

    results.append(bench_http(readings))
    results.append(bench_grpc(readings))
    results.append(bench_websocket(readings))

    for qos in [0, 1, 2]:
        results.append(bench_mqtt(readings, qos=qos))

    # --- Print summary table ---
    print("\n" + "=" * 75)
    print(f"{'Protocol':<16} {'Setup(ms)':>10} {'Avg Lat(ms)':>12} {'P95(ms)':>9} {'P99(ms)':>9} {'Msg/s':>9}")
    print("=" * 75)
    for r in results:
        print(
            f"{r['protocol']:<16}"
            f"{r['setup_time_ms']:>10.1f}"
            f"{r['latency_ms']:>12.2f}"
            f"{r['latency_p95_ms']:>9.2f}"
            f"{r['latency_p99_ms']:>9.2f}"
            f"{r['throughput_msg_sec']:>9.1f}"
        )
    print("=" * 75)

    # --- Save to JSON ---
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {RESULTS_FILE}")


if __name__ == "__main__":
    main()