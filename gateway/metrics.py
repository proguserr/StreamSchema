import os
from prometheus_client import start_http_server, Counter, Histogram, Gauge

PORT = int(os.getenv("METRICS_PORT", "9101"))

def start_metrics(port: int | None = None):
    p = port or PORT
    start_http_server(p, addr="0.0.0.0")
    print(f"[metrics] listening on 0.0.0.0:{p}")

# exported metrics
VALIDATIONS_PASS = Counter("gateway_validations_pass_total", "Valid events accepted")
VALIDATIONS_FAIL = Counter("gateway_validations_fail_total", "Events rejected by schema/validation")
PRODUCE_LATENCY = Histogram(
    "produce_latency_seconds",
    "Latency of /produce calls (s)",
    buckets=(0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2)
)
PRODUCE_REJECTS = Counter("produce_rejects_total", "Produce rejects by reason", ["reason"])
INFLIGHT = Gauge("gateway_inflight_requests", "Requests currently processing")
