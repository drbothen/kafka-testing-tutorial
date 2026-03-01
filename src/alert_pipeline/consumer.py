"""CLI consumer: subscribes to alerts topic and prints received alerts.

Usage:
    uv run python -m alert_pipeline.consumer --bootstrap localhost:9092
"""
import argparse
import json
import signal
import sys

from confluent_kafka import Consumer, KafkaError

from alert_pipeline.models import Alert


def main():
    parser = argparse.ArgumentParser(description="Consume alerts from Kafka")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="alerts", help="Topic to consume from")
    parser.add_argument("--group", default="alert-viewer", help="Consumer group ID")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([args.topic])

    running = True

    def shutdown(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)

    print(f"Listening on {args.topic}... (Ctrl+C to stop)")
    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Error: {msg.error()}", file=sys.stderr)
            continue

        try:
            alert = Alert.model_validate_json(msg.value())
            print(f"  ALERT | {alert.severity.upper():>8} | {alert.source_id} | "
                  f"{alert.metric_name}={alert.value} (threshold={alert.threshold})")
        except Exception as exc:
            print(f"  Failed to parse: {exc}", file=sys.stderr)

    consumer.close()
    print("Stopped.")


if __name__ == "__main__":
    main()
