"""Chargement du Data Warehouse PostgreSQL à partir des couches Silver/Gold.

Tables écrites :
- fct_transactions (échantillon des transactions silver),
- kpi_overview, agg_daily_global, agg_daily_operator, agg_monthly_operator,
  agg_hourly_pattern, agg_region, agg_tx_type_channel,
- anomalies, agg_suspects.

Usage :
    spark-submit load_warehouse.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import os

from pyspark.sql import functions as F

from ingestion.config import LAKE_GOLD_PREFIX, LAKE_SILVER_PREFIX
from spark.spark_session import build_spark, lake_uri

FACT_COLUMNS_LIMIT = int(os.environ.get("WAREHOUSE_FACT_LIMIT", "1000000"))

TABLES = [
    ("kpi_overview", "kpi_overview"),
    ("agg_daily_global", "agg_daily_global"),
    ("agg_daily_operator", "agg_daily_operator"),
    ("agg_monthly_operator", "agg_monthly_operator"),
    ("agg_hourly_pattern", "agg_hourly_pattern"),
    ("agg_region", "agg_region"),
    ("agg_tx_type_channel", "agg_tx_type_channel"),
    ("anomalies", "fct_suspect_transactions"),
    ("agg_suspects", "agg_suspects"),
]


def jdbc_options():
    url = (
        f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:"
        f"{os.environ.get('POSTGRES_PORT', '5432')}/{os.environ.get('POSTGRES_DB', 'warehouse')}"
    )
    return {
        "url": url,
        "driver": "org.postgresql.Driver",
        "user": os.environ.get("POSTGRES_USER", "afridata"),
        "password": os.environ.get("POSTGRES_PASSWORD", "afridata_secret"),
    }


def write_table(spark, lake_table: str, pg_table: str, options: dict) -> int:
    df = spark.read.parquet(lake_uri(f"{LAKE_GOLD_PREFIX}/{lake_table}"))
    if lake_table.startswith("agg_") or lake_table == "kpi_overview":
        df = df.coalesce(2)
    (
        df.write.format("jdbc")
        .option("url", options["url"])
        .option("driver", options["driver"])
        .option("user", options["user"])
        .option("password", options["password"])
        .option("dbtable", pg_table)
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    return df.count()


def main():
    spark = build_spark("afridata-load-warehouse")
    options = jdbc_options()

    fct = (
        spark.read.parquet(lake_uri(LAKE_SILVER_PREFIX))
        .drop("source_file")
        .limit(FACT_COLUMNS_LIMIT)
        .withColumn("tx_ts_str", F.date_format("tx_ts", "yyyy-MM-dd HH:mm:ss"))
        .drop("tx_ts")
        .coalesce(4)
    )
    (
        fct.write.format("jdbc")
        .option("url", options["url"])
        .option("driver", options["driver"])
        .option("user", options["user"])
        .option("password", options["password"])
        .option("dbtable", "fct_transactions")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    print(f"WAREHOUSE OK : fct_transactions ({fct.count()} lignes)")

    for lake_table, pg_table in TABLES:
        count = write_table(spark, lake_table, pg_table, options)
        print(f"WAREHOUSE OK : {pg_table} ({count} lignes)")

    spark.stop()


if __name__ == "__main__":
    main()
