"""Fake order generator: publishes Avro events to the Kafka topic `orders`.

The schema (order.avsc) is registered in Karapace with the first message, and
every message carries the schema id in its header (Schema Registry format).

Examples:
  python producer.py --total 1000            # 1000 orders as fast as possible
  python producer.py --per-second 5          # continuous stream of 5 orders/s
  python producer.py --total 50000 --late-ratio 0.05 --late-max-hours 72
                                             # 5% of the events arrive late

Late events simulate devices that were offline (a store terminal, a phone
app) and send their buffered orders hours later: created_at is in the past,
but the message reaches Kafka now. See docs/MAINTENANCE.md.
"""

import argparse
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

PRODUCTS = {
    "keyboard": Decimal("189.90"), "mouse": Decimal("79.90"), "monitor 27": Decimal("1499.00"),
    "laptop": Decimal("4899.00"), "headset": Decimal("349.90"), "webcam": Decimal("259.00"),
    "chair": Decimal("1290.00"), "ssd 1tb": Decimal("459.90"),
}
STATUSES = ["CREATED"] * 5 + ["PAID"] * 3 + ["SHIPPED", "DELIVERED", "CANCELLED"]
CHANNELS = ["web", "app", "store", None]


def new_order(late_ratio: float = 0, late_max_hours: float = 72) -> dict:
    product = random.choice(list(PRODUCTS))
    created_at = datetime.now(timezone.utc)
    if late_ratio and random.random() < late_ratio:
        created_at -= timedelta(hours=random.uniform(1, late_max_hours))
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.randint(1, 5000),
        "product": product,
        "quantity": random.choices([1, 2, 3, 5], weights=[70, 20, 7, 3])[0],
        "unit_price": PRODUCTS[product],
        "status": random.choice(STATUSES),
        "channel": random.choice(CHANNELS),
        "created_at": created_at,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--total", type=int, default=0, help="how many orders to send (0 = endless)")
    ap.add_argument("--per-second", type=float, default=0, help="rate (0 = no pause)")
    ap.add_argument("--late-ratio", type=float, default=0,
                    help="fraction of events that arrive late, e.g. 0.05 (default 0)")
    ap.add_argument("--late-max-hours", type=float, default=72,
                    help="maximum delay of a late event, in hours (default 72)")
    args = ap.parse_args()

    registry = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL", "http://karapace:8081")})
    schema = (Path(__file__).parent / "order.avsc").read_text()
    avro_value = AvroSerializer(registry, schema)
    str_key = StringSerializer("utf_8")
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        "linger.ms": 50,
        "compression.type": "zstd",
        "enable.idempotence": True,
    })

    pause = 1 / args.per_second if args.per_second > 0 else 0
    sent = 0
    start = time.time()
    try:
        while args.total == 0 or sent < args.total:
            order = new_order(args.late_ratio, args.late_max_hours)
            ctx = SerializationContext(args.topic, MessageField.VALUE)
            producer.produce(
                args.topic,
                key=str_key(order["order_id"]),
                value=avro_value(order, ctx),
            )
            producer.poll(0)
            sent += 1
            if sent % 500 == 0:
                print(f"{sent} orders sent", flush=True)
            if pause:
                time.sleep(pause)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(30)
    print(f"total: {sent} orders in {time.time() - start:.1f}s to topic '{args.topic}'", flush=True)


if __name__ == "__main__":
    main()
