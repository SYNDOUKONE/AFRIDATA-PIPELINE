"""Producteur Kafka : émet des transactions simulées en temps réel.

Usage :
    python ingestion/kafka_producer.py --rate 200 --duration 60
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

from ingestion.config import TOPIC_TRANSACTIONS
from ingestion.generate_transactions import WeightedPicker, make_transaction
from ingestion.config import CHANNELS, OPERATORS

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None


def build_producer(bootstrap_servers: str):
    if KafkaProducer is None:
        raise SystemExit("Le package 'kafka-python' est requis : pip install kafka-python")
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        linger_ms=20,
        batch_size=32768,
    )


def stream(producer, topic: str, rate: float, duration: float | None, seed: int) -> int:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    operator_picker = WeightedPicker(OPERATORS, rng)
    channel_picker = WeightedPicker(CHANNELS, rng)
    sent = 0
    interval = 1.0 / rate
    deadline = None if duration is None else time.monotonic() + duration
    while deadline is None or time.monotonic() < deadline:
        transaction = make_transaction(rng, now, span_days=0)
        transaction["operator"] = operator_picker.pick()
        transaction["channel"] = channel_picker.pick()
        transaction["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        producer.send(topic, key=transaction["transaction_id"], value=transaction)
        sent += 1
        time.sleep(interval)
    producer.flush()
    return sent


def main():
    parser = argparse.ArgumentParser(description="Producteur Kafka de transactions")
    parser.add_argument("--bootstrap", type=str, default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", type=str, default=os.environ.get("KAFKA_TOPIC_TRANSACTIONS", TOPIC_TRANSACTIONS))
    parser.add_argument("--rate", type=float, default=200, help="transactions par seconde")
    parser.add_argument("--duration", type=float, default=None, help="secondes (infini si omis)")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    producer = build_producer(args.bootstrap)
    print(f"Envoi vers {args.topic} @ {args.rate}/s (Ctrl+C pour arrêter)")
    try:
        sent = stream(producer, args.topic, args.rate, args.duration, args.seed)
    except KeyboardInterrupt:
        producer.flush()
        sent = 0
    print(f"{sent} messages envoyés")


if __name__ == "__main__":
    main()
