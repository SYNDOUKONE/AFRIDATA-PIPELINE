"""Pipeline Gold : tables agrégées prêtes pour l'analyse (MinIO, Parquet).

Produit :
- agg_daily_operator, agg_monthly_operator, agg_daily_global
- agg_hourly_pattern, agg_region, agg_tx_type_channel
- kpi_overview (1 ligne, KPIs globaux)

Usage :
    spark-submit gold_pipeline.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import DataFrame, functions as F

from ingestion.config import LAKE_GOLD_PREFIX, LAKE_SILVER_PREFIX
from spark.spark_session import build_spark, lake_uri

SILVER_COLUMNS = [
    "transaction_id", "tx_ts", "tx_date", "tx_year_month", "tx_hour",
    "operator", "tx_type", "channel", "amount", "fee", "currency",
    "sender_id", "receiver_id", "sender_region", "status", "is_failed",
    "amount_bucket", "agent_id", "device_id",
]

FAILED_RATIO = F.round(F.col("failed_count") / F.col("tx_count"), 6)


def agg_daily_operator(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("tx_date", "operator")
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("amount").alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.sum("fee").alias("total_fees"),
            F.sum("is_failed").alias("failed_count"),
        )
        .withColumn("fail_rate", FAILED_RATIO)
    )


def agg_monthly_operator(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("tx_year_month", "operator", "tx_type")
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("amount").alias("total_amount"),
            F.sum("is_failed").alias("failed_count"),
        )
        .withColumn("fail_rate", FAILED_RATIO)
    )


def agg_daily_global(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("tx_date")
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("amount").alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.sum("fee").alias("total_fees"),
            F.sum("is_failed").alias("failed_count"),
            F.countDistinct("sender_id").alias("active_senders"),
        )
        .withColumn("fail_rate", FAILED_RATIO)
    )


def agg_hourly_pattern(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("tx_hour", "channel")
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("amount").alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
    )


def agg_region(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("sender_region", "operator")
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("amount").alias("total_amount"),
            F.sum("is_failed").alias("failed_count"),
        )
        .withColumn("fail_rate", FAILED_RATIO)
    )


def agg_tx_type_channel(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("tx_type", "channel", "amount_bucket")
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("amount").alias("total_amount"),
        )
    )


def kpi_overview(silver: DataFrame) -> DataFrame:
    return (
        silver.agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.sum("fee").alias("total_fees"),
            F.sum("is_failed").alias("total_failed"),
            F.countDistinct("sender_id").alias("distinct_senders"),
            F.countDistinct("operator").alias("operator_count"),
            F.max("tx_ts").alias("last_transaction_at"),
        )
        .withColumn("fail_rate", F.round(F.col("total_failed") / F.col("total_transactions"), 6))
    )


GOLD_TABLES = {
    "agg_daily_operator": agg_daily_operator,
    "agg_monthly_operator": agg_monthly_operator,
    "agg_daily_global": agg_daily_global,
    "agg_hourly_pattern": agg_hourly_pattern,
    "agg_region": agg_region,
    "agg_tx_type_channel": agg_tx_type_channel,
    "kpi_overview": kpi_overview,
}


def main():
    spark = build_spark("afridata-gold")
    silver = spark.read.parquet(lake_uri(LAKE_SILVER_PREFIX)).select(SILVER_COLUMNS)

    for table, builder in GOLD_TABLES.items():
        output = lake_uri(f"{LAKE_GOLD_PREFIX}/{table}")
        builder(silver).write.mode("overwrite").parquet(output)
        print(f"GOLD OK : {table} -> {output}")

    spark.stop()


if __name__ == "__main__":
    main()
