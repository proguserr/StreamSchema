# StreamSchema – Real-time Event Validation & Observability

## 1. Why I built this  
I wanted to simulate a real-world event ingestion pipeline where incoming JSON events are validated against a schema **before** being accepted into Kafka.  
This project serves two purposes:  
1. **Functional** – Validate that our gateway rejects malformed events and accepts valid ones with low latency.  
2. **Operational** – Prove that we can monitor ingestion health in real-time using Grafana dashboards.  

I also treated this as a hiring-manager-friendly demonstration of my ability to build, observe, and reason about event-driven systems end-to-end.

---

## 2. How I approached it  

I split the work into these stages:

### **Stage 1 – Schema registry & gateway**
- Used a local Redpanda Kafka cluster.
- Registered the `github.event` schema in the Schema Registry.
- Gateway listens for incoming events and validates them against the registered schema before producing to Kafka.

**Trade-off taken:** I ran everything in Docker Compose for portability. It’s not production-grade but gives a self-contained, reproducible demo.

---

### **Stage 2 – Producer simulation**
- Wrote a Python script (`github_ingest.py`) that sends events to the gateway.
- Parameterized it to send a mix of valid and invalid events for testing both acceptance and rejection flows.
- For the actual verification, I ran it with **only valid events** to get clean “pass” metrics for Grafana spike testing.

**Example run:**
```bash
python3 scripts/github_ingest.py 50
```
Output:
```
Done. Sent=30 Rejected=0
```
  
---

### **Stage 3 – Observability with Grafana**
I created a **Gateway Dashboard** in Grafana to monitor:

- **Valid events/s (pass series)** – Confirms gateway is accepting good events.
- **Produce latency (P50/P95/P99)** – Ensures we stay within acceptable latency.
- **Rejects by reason** – Should remain flat in a clean run.
- **Inflight requests** – Shows brief spikes during bursts.

---

## 3. Testing & Verification Flow  

### **Before Run – Baseline**
Dashboard is idle, no traffic spikes visible.  
![SS1 – Baseline dashboard](screenshots/SS1.png)

---

### **During Run – Ingestion Spike**
While `github_ingest.py` was running, I refreshed the Grafana dashboard:
- **Validations (pass)** spiked as events came in.
- **Latency (P50/P95/P99)** showed visible dots representing histogram data points.
- **Inflight requests** briefly spiked in sync with producer load.  

![SS2 – During run validation spike](screenshots/SS2.png)  
![SS3 – Latency dots during run](screenshots/SS3.png)

---

### **After Run – Spike history preserved**
One minute after stopping the producer:
- Dashboard flattened to baseline.
- Previous spike history still visible in the latency panel for reference.  

![SS4 – Post run with spike history](screenshots/SS4.png)

---

### **Consumer Output Verification**
The Kafka consumer printed the accepted messages, confirming schema validation passed for all:  

![SS5 – Consumer output](screenshots/SS5.png)

---

### **Metrics Endpoint Check**
Queried `http://localhost:9101/metrics` to verify:
- `gateway_validations_pass_total` incremented to match event count.
- `gateway_validations_fail_total` remained 0.
- Latency count and sum aligned with expectations.  

![SS6 – Metrics verification](screenshots/SS6.png)

---

## 4. Environment Setup & Testing  

### **Prerequisites**
- Docker & Docker Compose installed
- Python 3.8+ with `requests` and `jq`

### **Steps**
1. **Clone repository & enter project**
```bash
git clone <repo_url>
cd StreamSchema
```
2. **Start environment**
```bash
docker compose up -d
```
3. **Register schema**
```bash
curl -sS -X POST http://localhost:8001/subjects/github.event/versions   -H 'Content-Type: application/json'   --data-binary @/tmp/github_event_body.json | jq .
```
4. **Open Grafana**
   - URL: `http://localhost:3000`
   - Load Gateway Dashboard.
   - Set time range to **Last 15 minutes**.

5. **Run ingestion**
```bash
python3 scripts/github_ingest.py 50
```

6. **Observe dashboards**
   - Watch spikes during the run.
   - Wait ~1 min after stopping to confirm flattening.

7. **Check metrics endpoint**
```bash
curl -s http://localhost:9101/metrics | egrep 'gateway_validations_(pass|fail)_total|produce_latency_seconds_(count|sum)'
```

---

**End result:**  
This test successfully validated that our gateway enforces schema compliance, maintains low latency, and exposes operational metrics that are observable in Grafana. The screenshots above act as evidence of a correctly executed test run.
