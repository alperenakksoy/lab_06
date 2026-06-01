import sys
import requests

API = "http://localhost:8474"
PROXY_NAMES = ["http1", "grpc", "websocket", "mqtt"]

def remove_all_toxics():
    for name in PROXY_NAMES:
        r = requests.get(f"{API}/proxies/{name}/toxics")
        for toxic in r.json():
            requests.delete(f"{API}/proxies/{name}/toxics/{toxic['name']}")
    print("[OK] All toxics removed")

def apply_latency():
    remove_all_toxics()
    for name in PROXY_NAMES:
        requests.post(f"{API}/proxies/{name}/toxics", json={
            "name":       "latency_up",
            "type":       "latency",
            "stream":     "upstream",
            "attributes": {"latency": 100, "jitter": 10},
        })
        requests.post(f"{API}/proxies/{name}/toxics", json={
            "name":       "latency_down",
            "type":       "latency",
            "stream":     "downstream",
            "attributes": {"latency": 100, "jitter": 10},
        })
    print("[OK] Applied: 200ms RTT latency (100ms each direction + 10ms jitter)")

def apply_loss():
    """Drop 5% of packets"""
    remove_all_toxics()
    for name in PROXY_NAMES:
        requests.post(f"{API}/proxies/{name}/toxics", json={
            "name":       "loss_up",
            "type":       "slice",
            "stream":     "upstream",
            "attributes": {"average_size": 1, "size_variation": 0, "delay": 0},
            "toxicity":   0.05,   # 5% of data chunks get this toxic applied
        })
        requests.post(f"{API}/proxies/{name}/toxics", json={
            "name":       "loss_down",
            "type":       "slice",
            "stream":     "downstream",
            "attributes": {"average_size": 1, "size_variation": 0, "delay": 0},
            "toxicity":   0.05,
        })
    print("[OK] Applied: ~5% packet loss (slice toxic, toxicity=0.05)")

def apply_bandwidth():
    remove_all_toxics()
    for name in PROXY_NAMES:
        requests.post(f"{API}/proxies/{name}/toxics", json={
            "name":       "bw_up",
            "type":       "bandwidth",
            "stream":     "upstream",
            "attributes": {"rate": 125},   # KB/s
        })
        requests.post(f"{API}/proxies/{name}/toxics", json={
            "name":       "bw_down",
            "type":       "bandwidth",
            "stream":     "downstream",
            "attributes": {"rate": 125},
        })
    print("[OK] Applied: 1 Mbit/s bandwidth cap (125 KB/s each direction)")

def status():
    for name in PROXY_NAMES:
        r = requests.get(f"{API}/proxies/{name}/toxics")
        toxics = r.json()
        if toxics:
            print(f"\n{name}:")
            for t in toxics:
                print(f"  {t['name']}: type={t['type']} stream={t['stream']} attrs={t['attributes']} toxicity={t['toxicity']}")
        else:
            print(f"{name}: no toxics (clean)")

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    scenario = sys.argv[2] if len(sys.argv) > 2 else ""

    if cmd == "apply":
        {"latency": apply_latency, "loss": apply_loss, "bandwidth": apply_bandwidth}[scenario]()
    elif cmd == "reset":
        remove_all_toxics()
        print("[OK] Network reset to clean state")
    elif cmd == "status":
        status()
    else:
        print(f"Unknown command: {cmd}")