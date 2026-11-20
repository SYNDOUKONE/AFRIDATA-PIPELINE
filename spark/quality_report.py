"""Rapport de qualité autonome sur la couche Silver (sans faire échouer le job).

Usage :
    spark-submit quality_report.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import os
from datetime import datetime, timezone

from ingestion.config import LAKE_SILVER_PREFIX
from spark.quality_checks import evaluate, run_checks, write_report
from spark.spark_session import build_spark, lake_uri


def main():
    spark = build_spark("afridata-quality-report")
    silver = spark.read.parquet(lake_uri(LAKE_SILVER_PREFIX))
    metrics = run_checks(spark, silver)
    report = evaluate(metrics)
    report["run_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    report["rows"] = silver.count()

    local_path = os.path.join("data", "processed", f"quality_report_{report['run_date'][:10]}.json")
    write_report(report, local_path)
    print(f"RAPPORT QUALITÉ : {report['status']} ({report['checks_passed']}/{report['checks_total']} contrôles OK)")
    for check in report["checks"]:
        print(f"  - {check['check']:<38} {check['violations']:>10}/{check['total']:<10} "
              f"ratio={check['violation_ratio']:.6f} seuil={check['threshold_ratio']} -> "
              f"{'OK' if check['passed'] else 'KO'}")
    spark.stop()


if __name__ == "__main__":
    main()
