import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
import os
import yaml

# Load config
with open("configs/pipeline.yaml") as f:
    config = yaml.safe_load(f)

EVENTS_PER_BATCH = config["telemetry"]["events_per_batch"]
OUTPUT_DIR = config["telemetry"]["output_dir"]
os.makedirs(OUTPUT_DIR, exist_ok=True)

event_types = ["login", "logout", "match_start", "kill", "purchase", "match_end"]
regions = ["NA", "EU", "ASIA", "SA"]

def generate_batch(batch_id):
    data = []
    base_time = datetime.now()
    for _ in range(EVENTS_PER_BATCH):
        player_id = str(uuid.uuid4())
        match_id = str(uuid.uuid4())
        event_type = random.choice(event_types)
        timestamp = base_time - timedelta(seconds=random.randint(0, 3600))
        region = random.choice(regions)
        latency_ms = random.randint(10, 500)
        data.append([player_id, match_id, event_type, timestamp, region, latency_ms])
    
    df = pd.DataFrame(data, columns=["player_id", "match_id", "event_type", "timestamp", "region", "latency_ms"])
    file_path = os.path.join(OUTPUT_DIR, f"batch_{batch_id}.csv")
    df.to_csv(file_path, index=False)
    print(f"Saved {file_path}")

if __name__ == "__main__":
    for batch_id in range(1, 4):  # generate 3 batches
        generate_batch(batch_id)