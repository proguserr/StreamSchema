import os, time, orjson
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
from aiokafka import AIOKafkaProducer
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

from metrics import (
    start_metrics,
    VALIDATIONS_PASS,
    VALIDATIONS_FAIL,
    PRODUCE_LATENCY,
    PRODUCE_REJECTS,
    INFLIGHT,
)

REGISTRY_URL = os.getenv("REGISTRY_URL", "http://localhost:8001")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")

app = FastAPI(title="StreamSchema Gateway")
producer: Optional[AIOKafkaProducer] = None
client = httpx.AsyncClient(timeout=5.0)

class ProduceBody(BaseModel):
    subject: str
    payload: dict
    topic: str
    key: Optional[str] = None

@app.on_event("startup")
async def startup():
    global producer
    # start metrics endpoint
    start_metrics()
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: v,  # we pass bytes already
    )
    await producer.start()

@app.on_event("shutdown")
async def shutdown():
    if producer:
        await producer.stop()
    await client.aclose()

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/produce")
async def produce(body: ProduceBody):
    # measure latency + inflight
    INFLIGHT.inc()
    start = time.perf_counter()
    try:
        # 1) get latest schema for subject
        try:
            r = await client.get(f"{REGISTRY_URL}/subjects/{body.subject}/versions/latest")
            r.raise_for_status()
            latest = r.json()
        except Exception as e:
            PRODUCE_REJECTS.labels(reason="registry_lookup_failed").inc()
            raise HTTPException(400, f"registry error: {e}")

        # 2) basic validation against "required" fields in schema
        schema = latest["schema"]
        required = set(schema.get("required", []))
        missing = [k for k in required if k not in body.payload]
        if missing:
            VALIDATIONS_FAIL.inc()
            PRODUCE_REJECTS.labels(reason="validation_required").inc()
            raise HTTPException(422, f"missing required fields: {missing}")

        # 3) produce to Kafka
        headers = [("schema-id", latest["fingerprint"].encode())]
        key_bytes = body.key.encode() if body.key else None
        value_bytes = orjson.dumps(body.payload)

        await producer.send_and_wait(
            body.topic,
            value=value_bytes,
            key=key_bytes,
            headers=headers,
        )

        VALIDATIONS_PASS.inc()
        return {"status": "ok", "subject": body.subject, "topic": body.topic}
    finally:
        PRODUCE_LATENCY.observe(time.perf_counter() - start)
        INFLIGHT.dec()
