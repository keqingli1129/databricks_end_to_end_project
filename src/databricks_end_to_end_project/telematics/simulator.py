"""Send fake telematics events to Kafka, or print them with --dry-run."""

import argparse
import random
import time
from collections.abc import Callable
from datetime import datetime, timezone

from databricks_end_to_end_project.telematics import events, kafka_config

Send = Callable[[str, bytes, bytes], None]  # (topic, key, value)


def run(send: Send, topic: str, count: int, interval: float, rng: random.Random) -> int:
    """Generate `count` events and hand each one to `send`, waiting `interval` seconds between them."""
    for i in range(count):
        event = events.generate_event(rng, events.DEFAULT_CHASSIS_NUMBERS, datetime.now(timezone.utc))
        send(topic, event["chassis_number"].encode(), events.to_json(event))
        if i < count - 1:
            time.sleep(interval)
    return count


def _print_send(topic: str, key: bytes, value: bytes) -> None:
    print(f"[dry-run] {topic} key={key.decode()} {value.decode()}", flush=True)


def _file_send(landing_path: str) -> Send:
    """Return a `send` that writes each event as one JSON file into a Unity Catalog volume."""
    from io import BytesIO

    from databricks.sdk import WorkspaceClient

    files = WorkspaceClient().files

    def send(topic: str, key: bytes, value: bytes) -> None:
        name = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}_{key.decode()}.json"
        files.upload(f"{landing_path}/{name}", BytesIO(value + b"\n"), overwrite=True)

    return send


def _send_to_kafka(args: argparse.Namespace) -> None:
    from confluent_kafka import Producer
    from databricks.sdk.runtime import dbutils

    servers, key, secret = kafka_config.read_credentials(dbutils, args.secret_scope)
    producer = Producer(kafka_config.producer_config(servers, key, secret))
    errors = []

    def on_delivery(err, msg):
        if err is not None:
            errors.append(err)

    def send(topic: str, key_bytes: bytes, value: bytes) -> None:
        producer.produce(topic, key=key_bytes, value=value, on_delivery=on_delivery)
        producer.poll(0)

    run(send, args.topic, args.count, args.interval, random.Random())
    undelivered = producer.flush(30)
    if errors or undelivered:
        raise SystemExit(f"Kafka delivery failed: {undelivered} undelivered, errors: {errors[:3]}")
    print(f"Sent {args.count} events to topic {args.topic}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Send fake car telematics events to Kafka.")
    parser.add_argument("--topic", default="telematics")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--interval", type=float, default=3.0, help="seconds between events")
    parser.add_argument("--secret-scope", default="kafka_dev")
    parser.add_argument("--sink", choices=["kafka", "files"], default="kafka")
    parser.add_argument(
        "--landing-path", help="volume folder for --sink files, e.g. /Volumes/<catalog>/<schema>/files/telematics"
    )
    parser.add_argument("--dry-run", action="store_true", help="print events instead of sending them")
    args = parser.parse_args(argv)

    if args.dry_run:
        run(_print_send, args.topic, args.count, args.interval, random.Random())
    elif args.sink == "files":
        if not args.landing_path:
            parser.error("--landing-path is required with --sink files")
        run(_file_send(args.landing_path), args.topic, args.count, args.interval, random.Random())
        print(f"Wrote {args.count} event files to {args.landing_path}")
    else:
        _send_to_kafka(args)


if __name__ == "__main__":
    main()
