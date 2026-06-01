#!/usr/bin/env bash

set -euo pipefail

IFACE=$(docker network inspect telemetry-net \
        --format '{{.Id}}' 2>/dev/null | cut -c1-12)
IFACE="br-${IFACE}"

# Fallback to docker0 if telemetry-net bridge not found
if ! ip link show "$IFACE" &>/dev/null; then
  echo "[WARN] Could not find telemetry-net bridge, falling back to docker0"
  IFACE="docker0"
fi

echo "[INFO] Using interface: $IFACE"
reset_tc() {
  tc qdisc del dev "$IFACE" root 2>/dev/null || true
  echo "[OK] Traffic rules cleared on $IFACE"
}

apply_latency() {

  reset_tc
  tc qdisc add dev "$IFACE" root netem delay 100ms 10ms distribution normal
  echo "[OK] Applied: 200ms RTT delay (100ms one-way + 10ms jitter)"
  tc qdisc show dev "$IFACE"
}

apply_loss() {
  reset_tc
  tc qdisc add dev "$IFACE" root netem loss 5%
  echo "[OK] Applied: 5% packet loss"
  tc qdisc show dev "$IFACE"
}

apply_bandwidth() {
  reset_tc
  tc qdisc add dev "$IFACE" root tbf rate 1mbit burst 32kb latency 400ms
  echo "[OK] Applied: 1 Mbit/s bandwidth cap"
  tc qdisc show dev "$IFACE"
}

CMD="${1:-help}"
SCENARIO="${2:-}"

case "$CMD" in
  apply)
    case "$SCENARIO" in
      latency)   apply_latency   ;;
      loss)      apply_loss      ;;
      bandwidth) apply_bandwidth ;;
      *)
        echo "Unknown scenario: $SCENARIO"
        echo "Available: latency | loss | bandwidth"
        exit 1
        ;;
    esac
    ;;
  reset)
    reset_tc
    ;;
  status)
    echo "[INFO] Current tc rules on $IFACE:"
    tc qdisc show dev "$IFACE"
    ;;
  *)
    echo "Usage: $0 apply {latency|loss|bandwidth} | reset | status"
    exit 1
    ;;
esac
