import asyncio, httpx, os, time

GITHUB_URL = "https://api.github.com/events"
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8002/produce")
SUBJECT = "github.events"
TOPIC = "github.events"

async def run():
    async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "StreamSchema-Demo"}) as client:
        last_seen = None
        while True:
            r = await client.get(GITHUB_URL)
            r.raise_for_status()
            events = r.json()
            new = []
            for e in events:
                if e["id"] == last_seen:
                    break
                new.append(e)
            if events:
                last_seen = events[0]["id"]

            # oldest first keeps ordering a bit more natural
            for e in reversed(new):
                payload = {
                    "subject": SUBJECT,
                    "topic": TOPIC,
                    "payload": {
                        "id": str(e.get("id")),
                        "type": e.get("type"),
                        "created_at": e.get("created_at"),
                        "repo": e.get("repo", {}),
                        "actor": e.get("actor", {})
                    }
                }
                try:
                    pr = await client.post(GATEWAY_URL, json=payload)
                    pr.raise_for_status()
                    print("200", payload["payload"]["id"], payload["payload"]["type"])
                except Exception as ex:
                    print("produce error:", ex)
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(run())
