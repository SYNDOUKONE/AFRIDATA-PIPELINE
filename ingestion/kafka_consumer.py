"""Consommateur Kafka : écrit les événements reçus en fichiers JSONL dans le Data Lake.

Batchs JSONL par lot de N messages, upload MinIO, et validation d'événements.

Usage :
    python ingestion/kafka_consumer.py --max-messages 100000
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import argparse
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from ingestion.config import LAKE_KAFKA_PREFIX, TOPIC_TRANSACTIONS
from ingestion.push_to_lake import build_client as build_minio, ensure_bucket
from ingestion.validators import validate_event

try:
    from kafka import KafkaConsumer
except ImportError:
    KafkaConsumer = None


def build_consumer(bootstrap_servers: str, topic: str, group: str):
    if KafkaConsumer is None:
        raise SystemExit("Le package 'kafka-python' est requis : pip install kafka-python")
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000,
    )


def flush_batch(batch: list, errors: list, outdir: Path, bucket, minio_client, prefix) -> dict:
    if not batch:
        return {"messages": 0, "object": None}
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")[:-3]
    path = outdir / f"kafka_transactions_{stamp}.jsonl"
    with open(path, "w", encoding="utf-8") as handle:
        for event in batch:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")
    object_name = None
    if minio_client is not None:
        object_name = f"{prefix}/{path.name}"
        minio_client.fput_object(bucket, object_name, str(path))
    return {
        "messages": len(batch),
        "invalid": Counter(errors),
        "object": object_name,
    }


def consume(consumer, outdir: Path, max_messages: int, batch_size: int, upload: bool,
            bucket: str, endpoint: str) -> dict:
    minio_client = None
    if upload:
        minio_client = build_minio(
            endpoint,
            os.environ.get("MINIO_ROOT_USER", "afridata"),
            os.environ.get("MINIO_ROOT_PASSWORD", "afridata_secret"),
        )
        ensure_bucket(minio_client, bucket)
    outdir.mkdir(parents=True, exist_ok=True)

    received = 0
    batch = []
    error_counter = Counter()
    stats = {"batches": 0}
    while received < max_messages:
        for message in consumer:
            event = message.value
            batch.append(event)
            error_counter.update(validate_event(event))
            received += 1
            if len(batch) >= batch_size or received >= max_messages:
                stats.update(flush_batch(batch, [], outdir, bucket, minio_client, LAKE_KAFKA_PREFIX))
                stats["batches"] += 1
                batch = []
                consumer.commit()
            if received >= max_messages:
                break
        else:
            time.sleep(0.5)
            continue
        break
    stats["received"] = received
    stats["errors"] = dict(error_counter)
    return stats


def main():
    parser = argparse.ArgumentParser(description="Consommateur Kafka -> JSONL Data Lake")
    parser.add_argument("--bootstrap", type=str, default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", type=str, default=os.environ.get("KAFKA_TOPIC_TRANSACTIONS", TOPIC_TRANSACTIONS))
    parser.add_argument("--group", type=str, default="afridata-landing-writer")
    parser.add_argument("--outdir", type=str, default="data/raw/kafka")
    parser.add_argument("--max-messages", type=int, default=100_000)
    parser.add_argument("--batch-size", type=int, default=5_000)
    parser.add_argument("--upload", action="store_true", default=True)
    parser.add_argument("--endpoint", type=str, default=os.environ.get("MINIO_ENDPOINT", "localhost:9000"))
    args = parser.parse_args()

    consumer = build_consumer(args.bootstrap, args.topic, args.group)
    stats = consume(consumer, Path(args.outdir), args.max_messages, args.batch_size,
                    args.upload, os.environ.get("DATALAKE_BUCKET", "datalake"), args.endpoint)
    print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
