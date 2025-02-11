
import os, asyncio, orjson, httpx
from aiokafka import AIOKafkaConsumer
REGISTRY_URL = os.getenv("REGISTRY_URL","http://localhost:8001")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS","localhost:9092")
TOPIC = os.getenv("TOPIC","events.demo")

async def main():
    consumer = AIOKafkaConsumer(TOPIC, bootstrap_servers=KAFKA_BROKERS, enable_auto_commit=True, auto_offset_reset="earliest", value_deserializer=lambda v: orjson.loads(v))
    await consumer.start()
    client = httpx.AsyncClient(timeout=5.0)
    try:
        async for msg in consumer:
            headers = dict((k,v) for k,v in msg.headers)
            schema_id = headers.get("schema-id", b"").decode()
            if not schema_id:
                print("WARN: no schema-id header")
                continue
            r = await client.get(f"{REGISTRY_URL}/schemas/{schema_id}")
            if r.status_code != 200:
                print("WARN: schema not found", schema_id)
                continue
            data = msg.value
            print(f"[OK] key={msg.key} schema={schema_id} payload={data}")
    finally:
        await consumer.stop()
        await client.aclose()

if __name__ == "__main__":
    asyncio.run(main())
