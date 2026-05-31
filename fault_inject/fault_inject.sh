#!/usr/bin/env bash
# =============================================================================
# fault_inject.sh — Network fault injection for Task 5
#
# Uses Linux 'tc' (traffic control) to degrade the network interface
# that Docker containers use (typically docker0 or the telemetry-net bridge).
#
# Usage:
#   ./fault_inject.sh apply latency      → add 200ms RTT delay
#   ./fault_inject.sh apply loss         → drop 5% of packets
#   ./fault_inject.sh apply bandwidth    → limit to 1 Mbit/s
#   ./fault_inject.sh reset              → remove all rules
#   ./fault_inject.sh status             → show current rules
#
# Run as root (or with sudo).
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Find the bridge interface for telemetry-net automatically
# ---------------------------------------------------------------------------
IFACE=$(docker network inspect telemetry-net \
        --format '{{.Id}}' 2>/dev/null | cut -c1-12)
IFACE="br-${IFACE}"

# Fallback to docker0 if telemetry-net bridge not found
if ! ip link show "$IFACE" &>/dev/null; then
  echo "[WARN] Could not find telemetry-net bridge, falling back to docker0"
  IFACE="docker0"
fi

echo "[INFO] Using interface: $IFACE"

# ---------------------------------------------------------------------------
# Helper: remove existing qdisc (ignore error if none exists)
# ---------------------------------------------------------------------------
reset_tc() {
  tc qdisc del dev "$IFACE" root 2>/dev/null || true
  echo "[OK] Traffic rules cleared on $IFACE"
}

# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

apply_latency() {
  # 200ms RTT = 100ms one-way delay
  # Add ±10ms jitter to make it realistic
  reset_tc
  tc qdisc add dev "$IFACE" root netem delay 100ms 10ms distribution normal
  echo "[OK] Applied: 200ms RTT delay (100ms one-way + 10ms jitter)"
  tc qdisc show dev "$IFACE"
}

apply_loss() {
  # 5% random packet loss
  reset_tc
  tc qdisc add dev "$IFACE" root netem loss 5%
  echo "[OK] Applied: 5% packet loss"
  tc qdisc show dev "$IFACE"
}

apply_bandwidth() {
  # Limit to 1 Mbit/s using Token Bucket Filter (TBF)
  # burst=32kb, latency=400ms (queue depth)
  reset_tc
  tc qdisc add dev "$IFACE" root tbf rate 1mbit burst 32kb latency 400ms
  echo "[OK] Applied: 1 Mbit/s bandwidth cap"
  tc qdisc show dev "$IFACE"
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

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
