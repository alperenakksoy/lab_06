import requests
import json

API = "http://localhost:8474"

PROXIES = [
    {"name": "http1",      "listen": "0.0.0.0:18001",  "upstream": "http1:8001"},
    {"name": "grpc",       "listen": "0.0.0.0:55051",  "upstream": "grpc:50051"},
    {"name": "websocket",  "listen": "0.0.0.0:18003",  "upstream": "websocket:8003"},
    {"name": "mqtt",       "listen": "0.0.0.0:11883",  "upstream": "mqtt-broker:1883"},
]

def reset_all():
    """Delete all existing proxies."""
    r = requests.get(f"{API}/proxies")
    for name in r.json():
        requests.delete(f"{API}/proxies/{name}")
    print("[OK] All proxies cleared")

def create_proxies():
    for p in PROXIES:
        r = requests.post(f"{API}/proxies", json={
            "name":     p["name"],
            "listen":   p["listen"],
            "upstream": p["upstream"],
            "enabled":  True,
        })
        print(f"[OK] Created proxy: {p['name']} → {p['upstream']}  (status {r.status_code})")

def list_proxies():
    r = requests.get(f"{API}/proxies")
    for name, info in r.json().items():
        print(f"  {name}: {info['listen']} → {info['upstream']}")

if __name__ == "__main__":
    reset_all()
    create_proxies()
    print("\nActive proxies:")
    list_proxies()
    print("\nToxiproxy ready. Run benchmarks on proxy ports.")