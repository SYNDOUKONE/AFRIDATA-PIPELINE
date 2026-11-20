"""Pipeline Silver : dédoublonnage, nettoyage, normalisation, rejets + quality gate.

Lit la couche Bronze (Parquet MinIO), applique le contrat de données et écrit :
- s3a://datalake/silver/transactions           (valides, partitionné tx_date)
- s3a://datalake/silver/transactions_rejected  (invalides + raison)
- data/processed/silver_quality_<date>.json    (rapport qualité, quality gate)

Usage :
    spark-submit silver_pipeline.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F

from ingestion.config import (
    LAKE_BRONZE_PREFIX,
    LAKE_SILVER_PREFIX,
    LAKE_SILVER_REJECTED_PREFIX,
)
from spark.quality_checks import evaluate, run_checks, write_report
from spark.spark_session import build_spark, lake_uri

DATE_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "dd/MM/yyyy HH:mm",
    "yyyyMMddHHmmss",
    "dd-MM-yyyy HH:mm:ss",
]

AMOUNT_TO_NUM = "cast(regexp_replace(regexp_replace(cast({col} as string), ',', '.'), '[^0-9.\\-]', '') as double)"

OPERATORS = ["Orange Money", "MTN Mobile Money", "Wave", "Moov Money"]
TX_TYPES = ["TRANSFER", "DEPOSIT", "WITHDRAWAL", "PAYMENT", "AIRTIME", "BILL_PAYMENT"]
STATUSES = ["SUCCESS", "FAILED", "PENDING"]


def parse_timestamp(df: DataFrame) -> DataFrame:
    ts = F.coalesce(*[F.to_timestamp("timestamp", fmt) for fmt in DATE_FORMATS])
    return df.withColumn("tx_ts", ts)


def with_amounts(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("amount_num", F.expr(AMOUNT_TO_NUM.format(col="amount")))
        .withColumn("fee_num", F.expr(AMOUNT_TO_NUM.format(col="fee")))
    )


def with_reject_reason(df: DataFrame) -> DataFrame:
    reason = F.lit(None).cast("string")
    for condition, label in [
        (F.col("transaction_id").isNull() | (F.col("transaction_id") == ""), "missing_transaction_id"),
        (F.col("tx_ts").isNull(), "invalid_timestamp"),
        (F.col("amount_num").isNull(), "invalid_amount"),
        (F.col("amount_num") <= 0, "non_positive_amount"),
        (F.col("currency").isNull() | (F.upper(F.trim(F.col("currency"))) != "XOF"), "invalid_currency"),
        (F.col("operator").isNull() | (~F.col("operator").isin(OPERATORS)), "unknown_operator"),
        (F.col("tx_type").isNull() | (~F.col("tx_type").isin(TX_TYPES)), "unknown_tx_type"),
        (F.col("status").isNull() | (~F.col("status").isin(STATUSES)), "unknown_status"),
        (F.col("sender_id").isNull() | (F.col("sender_id") == ""), "missing_sender"),
        (F.col("sender_id") == F.col("receiver_id"), "self_transfer"),
        (F.coalesce("amount_num", F.lit(0)) > 100_000_000, "implausible_amount"),
    ]:
        reason = F.when(condition, label).otherwise(reason)
    return df.withColumn("reject_reason", reason)


def conform(df: DataFrame) -> DataFrame:
    return (
        df.drop("amount", "fee")
        .withColumnRenamed("amount_num", "amount")
        .withColumnRenamed("fee_num", "fee")
        .withColumn("tx_date", F.to_date("tx_ts"))
        .withColumn("tx_year_month", F.date_format("tx_ts", "yyyy-MM"))
        .withColumn("tx_hour", F.hour("tx_ts"))
        .withColumn("is_failed", (F.col("status") == "FAILED").cast("int"))
        .withColumn(
            "amount_bucket",
            F.expr(
                "CASE WHEN amount < 1000 THEN 'a_0_1k' WHEN amount < 10000 THEN 'b_1k_10k' "
                "WHEN amount < 100000 THEN 'c_10k_100k' WHEN amount < 1000000 THEN 'd_100k_1M' "
                "ELSE 'e_1M_plus' END"
            ),
        )
        .drop("timestamp", "amount_raw", "fee_raw")
    )


def main():
    spark = build_spark("afridata-silver")
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    bronze = spark.read.parquet(lake_uri(LAKE_BRONZE_PREFIX))
    staged = (
        bronze.dropDuplicates(["transaction_id"])
        .withColumnRenamed("_source_file", "source_file")
        .transform(parse_timestamp)
        .transform(with_amounts)
    )

    rejected = staged.transform(with_reject_reason).where(F.col("reject_reason").isNotNull())
    valid = staged.transform(with_reject_reason).where(F.col("reject_reason").isNull()).transform(conform)

    silver_output = lake_uri(LAKE_SILVER_PREFIX)
    valid.write.mode("overwrite").partitionBy("tx_date").parquet(silver_output)

    rejected_output = lake_uri(f"{LAKE_SILVER_REJECTED_PREFIX}/run_date={run_date}")
    rejected.repartition(4).write.mode("overwrite").partitionBy("reject_reason").parquet(rejected_output)

    metrics = run_checks(spark, valid)
    report = evaluate(metrics)
    report.update(
        {
            "run_date": run_date,
            "rows_silver": spark.read.parquet(silver_output).count(),
            "rows_rejected": rejected.count(),
            "silver_path": silver_output,
            "rejected_path": rejected_output,
        }
    )
    report_path = os.path.join("data", "processed", f"silver_quality_{run_date}.json")
    write_report(report, report_path)

    print(f"SILVER OK : {report['rows_silver']} valides / {report['rows_rejected']} rejetées")
    print(f"Rapport qualité : {report_path} -> {report['status']}")

    if report["status"] == "FAILED":
        raise SystemExit(f"QUALITY GATE ÉCHOUÉ : {report['failed_checks']}")

    spark.stop()


if __name__ == "__main__":
    main()
