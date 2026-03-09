from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "clickstream.events"
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "purchase"]
PAGES = ["home", "search", "product", "cart", "checkout"]
PRODUCT_IDS = [f"SKU-{index:04d}" for index in range(1, 51)]


@dataclass(slots=True)
class ClickEvent:
    event_id: str
    user_id: str
    session_id: str
    event_type: str
    page: str
    product_id: str | None
    timestamp: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate sample clickstream events.")
    parser.add_argument("--count", type=int, default=10, help="Number of events to emit")
    parser.add_argument("--sleep", type=float, default=0.2, help="Delay between events in seconds")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic name")

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--send-kafka",
        "--send-to-kafka",
        dest="send_kafka",
        action="store_true",
        help="Send events to Kafka in addition to stdout",
    )
    mode_group.add_argument("--stdout-only", action="store_true", help="Disable Kafka delivery and print JSON only")
    return parser


def generate_event() -> ClickEvent:
    event_type = random.choices(
        EVENT_TYPES,
        weights=[0.55, 0.2, 0.18, 0.07],
        k=1,
    )[0]

    page = random.choice(PAGES)
    product_id = random.choice(PRODUCT_IDS) if event_type != "page_view" else None

    return ClickEvent(
        event_id=str(uuid.uuid4()),
        user_id=f"user-{random.randint(1, 5000):05d}",
        session_id=str(uuid.uuid4()),
        event_type=event_type,
        page=page,
        product_id=product_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


def build_kafka_producer(bootstrap_servers: str):
    try:
        from kafka import KafkaProducer
    except ImportError as exc:  # pragma: no cover - depends on local env
        raise SystemExit(
            "Kafka support requires 'kafka-python'. Install dependencies with 'pip install -r requirements.txt'."
        ) from exc

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            retries=3,
        )
    except Exception as exc:  # pragma: no cover - depends on local env
        raise SystemExit(f"Failed to create Kafka producer for '{bootstrap_servers}': {exc}") from exc

    if not producer.bootstrap_connected():  # pragma: no cover - depends on local env
        producer.close()
        raise SystemExit(
            f"Kafka broker connection failed for '{bootstrap_servers}'. Check Docker/Kafka status and topic settings."
        )

    return producer


def send_event(producer: Any, topic: str, payload: dict[str, Any]) -> tuple[int, int]:
    try:
        metadata = producer.send(topic, payload).get(timeout=10)
        return metadata.partition, metadata.offset
    except Exception as exc:  # pragma: no cover - depends on local env
        raise SystemExit(f"Failed to publish event to Kafka topic '{topic}': {exc}") from exc


def main() -> None:
    args = build_parser().parse_args()
    if args.count < 1:
        raise SystemExit("--count must be >= 1")
    if args.sleep < 0:
        raise SystemExit("--sleep must be >= 0")

    use_kafka = args.send_kafka and not args.stdout_only
    producer = build_kafka_producer(args.bootstrap_servers) if use_kafka else None

    try:
        for _ in range(args.count):
            event = generate_event()
            payload = asdict(event)
            print(json.dumps(payload, ensure_ascii=False))

            if producer is not None:
                partition, offset = send_event(producer, args.topic, payload)
                print(
                    f"[kafka] delivered topic={args.topic} partition={partition} offset={offset}"
                )

            if args.sleep > 0:
                time.sleep(args.sleep)
    finally:
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
