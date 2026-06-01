import os
os.environ["HTTP_URL"]   = "http://localhost:18001"
os.environ["GRPC_HOST"]  = "localhost:55051"
os.environ["WS_URL"]     = "ws://localhost:18003/telemetry"
os.environ["MQTT_HOST"]  = "localhost"
os.environ["MQTT_PORT"]  = "11883"

import sys
sys.path.insert(0, ".")
from benchmark import main

if __name__ == "__main__":
    main()