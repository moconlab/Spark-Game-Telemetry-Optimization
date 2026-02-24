import json
import time
import random
import argparse
from kafka import KafkaProducer
from data_simulator.event_generator import generate_event
from spark_jobs.utils.config_loader import load_config


def apply_skew(event: dict, config: dict) -> dict:
    """
    Intentionally introduce data skew for performance testing.
    """
    if config["simulation"]["skew_enabled"]:
        hot_value = config["simulation"]["skew_hot_value"]
        multiplier = config["simulation"]["skew_multiplier"]

        # Increase probability of hot partition key
        if random.randint(1, multiplier) != 1:
            event[config["simulation"]["skew_key"]] = hot_value

    return event


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        batch_size=32 * 1024,
        acks="all",
        retries=3,
    )


def stream_events(topic: str, rate: int, bootstrap_servers: str):
    config = load_config()
    producer = create_producer(bootstrap_servers)

    print(f"🚀 Streaming {rate} events/sec to topic '{topic}'")

    while True:
        start_time = time.time()

        for _ in range(rate):
            event = generate_event()
            event = apply_skew(event, config)
            producer.send(topic, event)

        producer.flush()

        elapsed = time.time() - start_time
        sleep_time = max(0, 1 - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Game Telemetry Stream Producer")
    parser.add_argument("--topic", default="game-events")
    parser.add_argument("--rate", type=int, default=1000)
    parser.add_argument("--bootstrap", default="localhost:9092")

    args = parser.parse_args()

    stream_events(
        topic=args.topic,
        rate=args.rate,
        bootstrap_servers=args.bootstrap,
    )