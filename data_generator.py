import json
import random
import uuid
import time
from datetime import datetime, timedelta

with open("event_schema.json", "r") as schema_file:
    SCHEMA = json.load(schema_file)

TENANTS = ["shopA", "shopB", "shopC"]
EVENT_TYPES = ["page_view", "purchase"]

def generate_event():
    tenant = random.choice(TENANTS)
    event_type = random.choice(EVENT_TYPES)
    ev = {
        "tenant_id": tenant,
        "event_type": event_type,
        "user_id": str(uuid.uuid4()),
        "timestamp": (datetime.utcnow() - timedelta(seconds=random.randint(0, 300))).isoformat() + "Z",
        "details": {}
    }
    if event_type == "purchase":
        ev["details"] = {
            "order_id": str(uuid.uuid4()),
            "amount": round(random.uniform(10, 200), 2)
        }
    return ev

if __name__ == "__main__":
    output_file = "events.jsonl"
    num_events = 1000
    with open(output_file, "w") as f:
        for _ in range(num_events):
            event = generate_event()
            f.write(json.dumps(event) + "\n")