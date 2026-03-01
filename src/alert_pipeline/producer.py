"""CLI producer: generates random telemetry events and sends to Kafka.

Usage:
    uv run python -m alert_pipeline.producer --bootstrap localhost:9092 --count 10
"""
import argparse
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from alert_pipeline.models import TelemetryEvent

SOURCES = ["sensor-01", "sensor-02", "sensor-03", "sensor-42", "sensor-99"]
METRICS = ["cpu_temp", "memory_pct", "disk_io", "network_latency"]


def generate_event() -> TelemetryEvent:
    return TelemetryEvent(
        event_id=str(uuid.uuid4()),
        source_id=random.choice(SOURCES),
        metric_name=random.choice(METRICS),
        value=round(random.uniform(20.0, 100.0), 1),
        timestamp=datetime.now(timezone.utc),
    )


def main():
    parser = argparse.ArgumentParser(description="Produce telemetry events to Kafka")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="raw-telemetry", help="Target topic")
    parser.add_argument("--count", type=int, default=10, help="Number of events to produce")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between events")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap})
    print(f"Producing {args.count} events to {args.topic}...")

    for i in range(args.count):
        event = generate_event()
        producer.produce(
            args.topic,
            key=event.source_id.encode(),
            value=event.model_dump_json().encode(),
        )
        print(f"  [{i+1}/{args.count}] {event.source_id} | {event.metric_name}={event.value}")
        if args.interval > 0 and i < args.count - 1:
            time.sleep(args.interval)

    producer.flush()
    print(f"Done. {args.count} events sent to {args.topic}.")


if __name__ == "__main__":
    main()
