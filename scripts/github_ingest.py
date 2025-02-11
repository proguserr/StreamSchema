import asyncio, os, sys, time, httpx, json

GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8002")
FEED    = "https://api.github.com/events"
SUBJECT = "github.event"
TOPIC   = "github.events"

# Keep only the fields our schema requires
def project_event(e):
    return {
        "id": str(e.get("id", "")),
        "type": e.get("type", ""),
        "created_at": e.get("created_at", ""),
        "repo": {"id": (e.get("repo") or {}).get("id"), "name": (e.get("repo") or {}).get("name")},
        "actor": {"id": (e.get("actor") or {}).get("id"), "login": (e.get("actor") or {}).get("login")},
    }

async def main(limit=50):
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(FEED, headers={"User-Agent":"streams-schema-demo"})
        r.raise_for_status()
        events = r.json()

        sent = 0
        rejected = 0
        for ev in events[:limit]:
            payload = project_event(ev)
            body = {"subject": SUBJECT, "topic": TOPIC, "payload": payload, "key": str(payload["id"])}
            try:
                pr = await client.post(f"{GATEWAY}/produce", json=body)
                if pr.status_code == 200:
                    sent += 1
                else:
                    rejected += 1
                    sys.stderr.write(f"Reject {pr.status_code} {pr.text[:100]}\n")
            except Exception as e:
                rejected += 1
                sys.stderr.write(f"Error {e}\n")
            # tiny delay to be gentle
            await asyncio.sleep(0.05)

        print(f"Done. Sent={sent} Rejected={rejected}")

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    asyncio.run(main(limit))
