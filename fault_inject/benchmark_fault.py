"""
benchmark_fault.py — Task 5: Fault injection benchmark runner

Workflow:
  1. Run benchmark under NORMAL conditions   → saves normal_results.json
  2. Apply latency fault   → run benchmark   → saves degraded_latency_results.json
  3. Apply loss fault      → run benchmark   → saves degraded_loss_results.json
  4. Apply bandwidth fault → run benchmark   → saves degraded_bandwidth_results.json
  5. Reset network
  6. Print comparison table across all conditions

Requires:
  - All four services running (docker-compose up)
  - benchmark.py in the same directory
  - fault_inject.sh in the same directory (run as sudo or root for tc)
"""

import subprocess
import json
import sys
import os
import time
import statistics

# Import the benchmark functions directly
sys.path.insert(0, os.path.dirname(__file__))
from benchmark import make_readings, bench_http, bench_grpc, bench_websocket, bench_mqtt

import random
random.seed(42)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCENARIOS = [
    ("normal",    None),
    ("latency",   ["sudo", "./fault_inject.sh", "apply", "latency"]),
    ("loss",      ["sudo", "./fault_inject.sh", "apply", "loss"]),
    ("bandwidth", ["sudo", "./fault_inject.sh", "apply", "bandwidth"]),
]

RESET_CMD = ["sudo", "./fault_inject.sh", "reset"]

OUTPUT_DIR = "."   # save JSON files here


# ---------------------------------------------------------------------------
# Run one full benchmark pass (all protocols)
# ---------------------------------------------------------------------------

def run_benchmark_pass(label: str, readings: list) -> list:
    print(f"\n{'='*60}")
    print(f"  Running benchmark: {label.upper()}")
    print(f"{'='*60}")

    results = []

    for fn, name in [
        (bench_http,                    "http1"),
        (bench_grpc,                    "grpc"),
        (bench_websocket,               "websocket"),
        (lambda r: bench_mqtt(r, qos=0), "mqtt_qos0"),
        (lambda r: bench_mqtt(r, qos=1), "mqtt_qos1"),
        (lambda r: bench_mqtt(r, qos=2), "mqtt_qos2"),
    ]:
        try:
            result = fn(readings)
            result["condition"] = label
            results.append(result)
            print(f"  [{name}] latency={result['latency_ms']}ms  "
                  f"throughput={result['throughput_msg_sec']} msg/s  "
                  f"p99={result['latency_p99_ms']}ms")
        except Exception as e:
            print(f"  [{name}] FAILED: {e}")
            results.append({
                "protocol":  name,
                "condition": label,
                "error":     str(e),
            })

    return results


# ---------------------------------------------------------------------------
# Apply / reset network faults
# ---------------------------------------------------------------------------

def apply_fault(cmd: list):
    print(f"\n[FAULT] Applying: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[WARN] tc command failed:\n{result.stderr}")
        print("[WARN] Continuing anyway — fault may not be applied.")
    else:
        print(result.stdout.strip())
    time.sleep(1)   # let netem settle


def reset_fault():
    print(f"\n[FAULT] Resetting network rules...")
    subprocess.run(RESET_CMD, capture_output=True, text=True)
    time.sleep(0.5)


# ---------------------------------------------------------------------------
# Comparison table
# ---------------------------------------------------------------------------

def print_comparison(all_results: dict):
    conditions = list(all_results.keys())
    protocols  = ["http1", "grpc", "websocket", "mqtt_qos0", "mqtt_qos1", "mqtt_qos2"]

    print(f"\n{'='*90}")
    print("COMPARISON TABLE — Avg Latency (ms)")
    print(f"{'='*90}")

    header = f"{'Protocol':<16}" + "".join(f"{c:>16}" for c in conditions)
    print(header)
    print("-" * 90)

    for proto in protocols:
        row = f"{proto:<16}"
        for cond in conditions:
            match = [r for r in all_results[cond] if r.get("protocol") == proto]
            if match and "latency_ms" in match[0]:
                row += f"{match[0]['latency_ms']:>16.2f}"
            else:
                row += f"{'ERR':>16}"
        print(row)

    print(f"\n{'='*90}")
    print("COMPARISON TABLE — Throughput (msg/s)")
    print(f"{'='*90}")
    print(header)
    print("-" * 90)

    for proto in protocols:
        row = f"{proto:<16}"
        for cond in conditions:
            match = [r for r in all_results[cond] if r.get("protocol") == proto]
            if match and "throughput_msg_sec" in match[0]:
                row += f"{match[0]['throughput_msg_sec']:>16.1f}"
            else:
                row += f"{'ERR':>16}"
        print(row)

    print(f"\n{'='*90}")
    print("COMPARISON TABLE — P99 Latency (ms)")
    print(f"{'='*90}")
    print(header)
    print("-" * 90)

    for proto in protocols:
        row = f"{proto:<16}"
        for cond in conditions:
            match = [r for r in all_results[cond] if r.get("protocol") == proto]
            if match and "latency_p99_ms" in match[0]:
                row += f"{match[0]['latency_p99_ms']:>16.2f}"
            else:
                row += f"{'ERR':>16}"
        print(row)

    print("=" * 90)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    readings = make_readings()
    print(f"Generated {len(readings)} readings across 5 sensors")

    all_results = {}

    for condition, fault_cmd in SCENARIOS:
        # Apply fault (or skip for normal)
        if fault_cmd:
            apply_fault(fault_cmd)
        else:
            reset_fault()   # ensure clean state for normal run

        # Run benchmark
        results = run_benchmark_pass(condition, readings)
        all_results[condition] = results

        # Save per-condition JSON
        filename = f"{OUTPUT_DIR}/{condition}_results.json"
        with open(filename, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n[SAVED] {filename}")

        # Reset after each degraded scenario
        if fault_cmd:
            reset_fault()

    # Save combined JSON
    combined_file = f"{OUTPUT_DIR}/all_results.json"
    with open(combined_file, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\n[SAVED] {combined_file}")

    # Print comparison
    print_comparison(all_results)


if __name__ == "__main__":
    main()
