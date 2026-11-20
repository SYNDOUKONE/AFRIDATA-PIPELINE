"""Pousse les données brutes locales vers le Data Lake MinIO (zone de landing).

Usage :
    python ingestion/push_to_lake.py --source data/raw --pattern "*.csv"
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import argparse
import fnmatch
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from ingestion.config import LAKE_LANDING_PREFIX

try:
    from minio import Minio
except ImportError:
    Minio = None


def build_client(endpoint: str, access_key: str, secret_key: str, secure: bool = False):
    if Minio is None:
        raise SystemExit("Le package 'minio' est requis : pip install minio")
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def ensure_bucket(client, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def upload_directory(client, bucket: str, source: Path, prefix: str, pattern: str) -> list:
    uploaded = []
    for path in sorted(source.iterdir()):
        if not path.is_file() or not fnmatch.fnmatch(path.name, pattern):
            continue
        object_name = f"{prefix}/{path.name}"
        client.fput_object(
            bucket,
            object_name,
            str(path),
            metadata={"x-amz-meta-ingested-at": datetime.now(timezone.utc).isoformat()},
        )
        uploaded.append(object_name)
    return uploaded


def main():
    parser = argparse.ArgumentParser(description="Upload des fichiers bruts vers MinIO")
    parser.add_argument("--source", type=str, default="data/raw")
    parser.add_argument("--pattern", type=str, default="*.csv")
    parser.add_argument("--bucket", type=str, default=os.environ.get("DATALAKE_BUCKET", "datalake"))
    parser.add_argument("--endpoint", type=str, default=os.environ.get("MINIO_ENDPOINT", "localhost:9000"))
    parser.add_argument("--prefix", type=str, default=LAKE_LANDING_PREFIX)
    args = parser.parse_args()

    client = build_client(
        args.endpoint,
        os.environ.get("MINIO_ROOT_USER", "afridata"),
        os.environ.get("MINIO_ROOT_PASSWORD", "afridata_secret"),
    )
    ensure_bucket(client, args.bucket)
    uploaded = upload_directory(client, args.bucket, Path(args.source), args.prefix, args.pattern)
    report = {"bucket": args.bucket, "uploaded": len(uploaded), "objects": uploaded[:20]}
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
