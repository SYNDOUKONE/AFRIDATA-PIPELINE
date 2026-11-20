"""Tests du générateur de transactions."""

import csv as csv_module
from pathlib import Path

from ingestion.config import OPERATORS, REGIONS
from ingestion.generate_transactions import CSV_HEADER, generate


def test_generate_writes_files(tmp_path: Path):
    paths = generate(2_000, tmp_path, seed=1)
    assert len(paths) >= 2
    for path in paths:
        assert path.exists() and path.stat().st_size > 0


def test_generated_rows_match_request(tmp_path: Path):
    rows_count = 500
    generate(rows_count, tmp_path, seed=2)
    csv_files = sorted(tmp_path.glob("*.csv"))
    with open(csv_files[0], encoding="utf-8") as handle:
        header = handle.readline().strip().split(",")
        body = sum(1 for _ in handle)
    assert header == CSV_HEADER
    assert body >= rows_count


def test_dirty_data_injected(tmp_path: Path):
    generate(2_000, tmp_path, seed=3)
    rows = []
    for csv_file in tmp_path.glob("*.csv"):
        with open(csv_file, encoding="utf-8") as handle:
            rows.extend(csv_module.DictReader(handle))

    ids = [r["transaction_id"] for r in rows]
    assert len(ids) != len(set(ids)), "les doublons doivent être injectés"

    bad_dates = [r for r in rows if "/" in r["timestamp"]]
    assert bad_dates, "formats de dates sales attendus"

    negative = [r for r in rows if str(r.get("amount") or "").startswith("-")]
    assert negative, "montants négatifs attendus"

    assert all(r["sender_region"] in REGIONS for r in rows)
    assert all(r["operator"] in OPERATORS for r in rows)


def test_determinism(tmp_path: Path):
    out_a, out_b = tmp_path / "a", tmp_path / "b"
    generate(100, out_a, seed=7)
    generate(100, out_b, seed=7)
    files_a = sorted(p.read_bytes() for p in out_a.glob("*.csv"))
    files_b = sorted(p.read_bytes() for p in out_b.glob("*.csv"))
    assert files_a == files_b
