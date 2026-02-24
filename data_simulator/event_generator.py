import random
import uuid
import time
from datetime import datetime

EVENT_TYPES = ["kill", "death", "purchase", "match_start", "match_end"]

def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "player_id": random.randint(1, 10_000_000),
        "game_id": random.randint(1, 1000),
        "event_type": random.choice(EVENT_TYPES),
        "event_value": random.randint(1, 100),
        "region": random.choice(["NA", "EU", "APAC"]),
        "timestamp": datetime.utcnow().isoformat()
    }

def generate_batch(n=100000):
    return [generate_event() for _ in range(n)]