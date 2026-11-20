"""Générateur de transactions mobile money fictives (batch CSV + JSONL).

Usage :
    python ingestion/generate_transactions.py --rows 1000000 --outdir data/raw
    python ingestion/generate_transactions.py --rows 10000 --outdir data/samples --seed 42
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import argparse
import csv
import json
import os
import random
import uuid
from bisect import bisect
from datetime import datetime, timedelta
from pathlib import Path

from ingestion.config import (
    CHANNELS,
    CURRENCY,
    FAILURE_REASONS,
    INVALID_CURRENCIES,
    OPERATORS,
    REGIONS,
    STATUS,
    TX_TYPES,
)

CHUNK_SIZE = 250_000

DIRTY_DATE_FORMATS = ["%d/%m/%Y %H:%M", "%Y%m%d%H%M%S", "%d-%m-%Y %H:%M:%S"]

CSV_HEADER = [
    "transaction_id",
    "timestamp",
    "operator",
    "tx_type",
    "channel",
    "amount",
    "currency",
    "sender_id",
    "receiver_id",
    "agent_id",
    "device_id",
    "sender_region",
    "fee",
    "status",
    "failure_reason",
]


class WeightedPicker:
    def __init__(self, weights: dict, seed: random.Random):
        self.keys = list(weights.keys())
        self.cum = []
        total = 0.0
        for key in self.keys:
            total += weights[key]
            self.cum.append(total)
        self.rng = seed

    def pick(self):
        return self.keys[bisect(self.cum, self.rng.random() * self.cum[-1])]


def random_phone(rng: random.Random) -> str:
    prefix = rng.choice(["07", "05", "01", "04"])
    return "+225" + prefix + "".join(str(rng.randint(0, 9)) for _ in range(8))


def maybe_dirty_timestamp(value: datetime, rng: random.Random) -> str:
    if rng.random() < 0.012:
        return value.strftime(rng.choice(DIRTY_DATE_FORMATS))
    return value.strftime("%Y-%m-%d %H:%M:%S")


def maybe_dirty_amount(value: float, rng: random.Random) -> object:
    roll = rng.random()
    if roll < 0.004:
        return -value
    if roll < 0.008:
        return f"{value:,.2f}".replace(",", " ") + " F"
    if roll < 0.009:
        return None
    return round(value, 2)


def make_transaction(rng: random.Random, now: datetime, span_days: int) -> dict:
    timestamp = now - timedelta(seconds=rng.uniform(0, span_days * 86400))
    amount = round(2.718 ** rng.gauss(8.6, 1.25), 2)
    fee = round(min(amount * 0.015, 2000) * (1 if rng.random() > 0.5 else 0), 2)
    status = WeightedPicker(STATUS, rng).pick()
    currency = CURRENCY if rng.random() >= 0.004 else rng.choice(INVALID_CURRENCIES)
    device_prefix = rng.choice(["ANDROID", "FEATURE", "POS"])
    return {
        "transaction_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "timestamp": maybe_dirty_timestamp(timestamp, rng),
        "operator": WeightedPicker(OPERATORS, rng).pick(),
        "tx_type": WeightedPicker(TX_TYPES, rng).pick(),
        "channel": WeightedPicker(CHANNELS, rng).pick(),
        "amount": maybe_dirty_amount(amount, rng),
        "currency": currency,
        "sender_id": random_phone(rng),
        "receiver_id": random_phone(rng),
        "agent_id": f"AG{rng.randint(10000, 99999)}",
        "device_id": f"{device_prefix}-{rng.randint(1000000, 9999999)}",
        "sender_region": WeightedPicker(REGIONS, rng).pick(),
        "fee": fee,
        "status": status,
        "failure_reason": rng.choice(FAILURE_REASONS["FAILED"]) if status == "FAILED" else "",
    }


def inject_duplicates(rows: list, rng: random.Random) -> list:
    n_dups = max(1, int(len(rows) * 0.015))
    rows = rows + [dict(rows[rng.randrange(len(rows))]) for _ in range(n_dups)]
    rng.shuffle(rows)
    return rows


def write_chunk(rows: list, outdir: Path, stem: str, part: int) -> tuple:
    csv_path = outdir / f"transactions_{stem}_{part:03d}.csv"
    json_path = outdir / f"transactions_{stem}_{part:03d}.jsonl"
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADER)
        writer.writeheader()
        writer.writerows(rows)
    with open(json_path, "w", encoding="utf-8") as json_file:
        for row in rows:
            json_file.write(json.dumps(row, ensure_ascii=False) + "\n")
    return csv_path, json_path


def generate(rows: int, outdir: Path, seed: int, span_days: int = 90) -> list:
    rng = random.Random(seed)
    now = datetime(2025, 1, 1)
    outdir.mkdir(parents=True, exist_ok=True)
    stem = f"{rows // 1000}k_seed{seed}"
    written = []
    done = 0
    part = 0
    while done < rows:
        size = min(CHUNK_SIZE, rows - done)
        chunk = [make_transaction(rng, now, span_days) for _ in range(size)]
        chunk = inject_duplicates(chunk, rng)
        written.extend(write_chunk(chunk, outdir, stem, part))
        done += size
        part += 1
    return written


def main():
    parser = argparse.ArgumentParser(description="Génère des transactions mobile money fictives")
    parser.add_argument("--rows", type=int, default=int(os.environ.get("NB_TRANSACTIONS", 1_000_000)))
    parser.add_argument("--outdir", type=str, default="data/raw")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    paths = generate(args.rows, Path(args.outdir), args.seed)
    total = sum(
        path.stat().st_size for path in paths
    )
    print(f"{args.rows} transactions générées dans {len(paths)} fichiers "
          f"({total / (1024 * 1024):.1f} Mo) -> {args.outdir}")


if __name__ == "__main__":
    main()
