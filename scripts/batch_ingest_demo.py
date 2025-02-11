
#!/usr/bin/env python3
import os, json, time, random, httpx, asyncio
REGISTRY = os.getenv("REGISTRY_URL","http://127.0.0.1:8001")
GATEWAY = os.getenv("GATEWAY_URL","http://127.0.0.1:8002")
subject = "user.login"

async def main():
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.post(f"{REGISTRY}/subjects/{subject}", json={"compatibility_mode":"BACKWARD"})
        with open("schemas/user_login_v1.json") as f: v1 = json.load(f)
        r = await client.post(f"{REGISTRY}/subjects/{subject}/versions", json={"schema": v1}); print("register v1", r.status_code, r.text)
        with open("schemas/user_login_v2.json") as f: v2 = json.load(f)
        r = await client.post(f"{REGISTRY}/subjects/{subject}/versions", json={"schema": v2}); print("register v2", r.status_code, r.text)

        for i in range(200):
            ok = random.random() > 0.15
            payload = {"user_id": f"user-{i}", "ip": "203.0.113.5", "ts": "2025-08-10T00:00:00Z"}
            if ok and random.random() > 0.6:
                payload["ua"] = "Mozilla/5.0"
            if not ok:
                payload.pop("ts", None)
            body = {"subject": subject, "payload": payload, "topic": "events.demo", "key": f"k-{i}"}
            rr = await client.post(f"{GATEWAY}/produce", json=body)
            print(i, rr.status_code)
            time.sleep(0.02)

if __name__ == "__main__":
    asyncio.run(main())
