import asyncio, httpx, time, os, json

GITHUB_URL = "https://api.github.com/events"
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8002/produce")
SUBJECT = "github.event"
TOPIC   = "github-events"

# minimal mapping that matches your schema keys
def to_payload(evt: dict) -> dict:
    actor = evt.get("actor") or {}
    repo  = evt.get("repo") or {}
    return {
        "id": str(evt.get("id")),
        "type": evt.get("type"),
        "actor_login": actor.get("login"),
        "repo_name": repo.get("name"),
        "created_at": evt.get("created_at"),
        "raw": evt,   # keep raw event too (schema has object)
    }

async def main():
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(GITHUB_URL, headers={"User-Agent":"streams-schema-demo"})
        r.raise_for_status()
        events = r.json()
        print(f"Fetched {len(events)} events")

        sent = 0
        rejected = 0
        for e in events:
            payload = to_payload(e)
            body = {
                "subject": SUBJECT,
                "topic": TOPIC,
                "payload": payload
            }
            try:
                pr = await client.post(GATEWAY_URL, json=body)
                if pr.status_code == 200:
                    sent += 1
                else:
                    rejected += 1
                    print("Reject", pr.status_code, pr.text[:200])
            except Exception as ex:
                rejected += 1
                print("Error", ex)

        print(f"Done. Sent={sent} Rejected={rejected}")

if __name__ == "__main__":
    asyncio.run(main())
