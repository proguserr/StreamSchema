import os
from prometheus_client import start_http_server, Counter

PORT = int(os.getenv("METRICS_PORT", "9100"))

def start_metrics(port: int | None = None):
    p = port or PORT
    start_http_server(p, addr="0.0.0.0")
    print(f"[metrics] listening on 0.0.0.0:{p}")

SCHEMA_LOOKUPS = Counter("registry_schema_lookups_total", "Schema lookups served")
SCHEMA_REGISTRATIONS = Counter("registry_schema_registrations_total", "Schemas registered")
