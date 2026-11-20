"""Pipeline Bronze : données brutes immuables au format Parquet partitionné (MinIO).

Lit la zone de landing (CSV batch + JSONL flux Kafka), ajoute les métadonnées
d'ingestion et écrit s3a://datalake/bronze/transactions.

Usage :
    spark-submit bronze_pipeline.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ingestion.config import LAKE_BRONZE_PREFIX, LAKE_KAFKA_PREFIX, LAKE_LANDING_PREFIX
from spark.spark_session import build_spark, lake_uri

RAW_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("operator", StringType()),
        StructField("tx_type", StringType()),
        StructField("channel", StringType()),
        StructField("amount", StringType()),
        StructField("currency", StringType()),
        StructField("sender_id", StringType()),
        StructField("receiver_id", StringType()),
        StructField("agent_id", StringType()),
        StructField("device_id", StringType()),
        StructField("sender_region", StringType()),
        StructField("fee", StringType()),
        StructField("status", StringType()),
        StructField("failure_reason", StringType()),
    ]
)


def read_landing(spark) -> DataFrame:
    landing_csv = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(RAW_SCHEMA)
        .load(lake_uri(LAKE_LANDING_PREFIX))
        .withColumn("_source_format", F.lit("csv"))
    )
    landing_json = (
        spark.read.format("json")
        .schema(RAW_SCHEMA)
        .load(lake_uri(LAKE_KAFKA_PREFIX))
        .withColumn("_source_format", F.lit("kafka_json"))
    )
    return landing_csv.unionByName(landing_json, allowMissingColumns=True)


def to_bronze(df: DataFrame, run_date: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp().cast("string"))
        .withColumn("amount_raw", F.col("amount").cast("string"))
        .withColumn("fee_raw", F.col("fee").cast("string"))
        .withColumn("_load_date", F.lit(run_date))
    )


def main():
    spark = build_spark("afridata-bronze")
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    bronze = to_bronze(read_landing(spark), run_date)
    output = lake_uri(LAKE_BRONZE_PREFIX)
    (
        bronze.repartition(8)
        .write.mode("append")
        .partitionBy("_source_format", "operator")
        .parquet(output)
    )

    print(f"BRONZE OK : {bronze.count()} lignes -> {output}")
    spark.stop()


if __name__ == "__main__":
    main()
