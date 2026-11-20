"""Détection d'anomalies sur les transactions Silver.

Deux moteurs combinés :
1. Règles statistiques (z-score montant, fréquence quotidienne, paires répétées) ;
2. Isolation Forest (scikit-learn) sur un échantillon de features.

Sorties :
- s3a://datalake/gold/anomalies     (Parquet, détail des transactions suspectes)
- s3a://datalake/gold/agg_suspects  (résumé par jour et opérateur)

Usage :
    spark-submit anomaly_detection.py [--sample 300000]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import argparse

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from ingestion.config import LAKE_GOLD_PREFIX, LAKE_SILVER_PREFIX
from spark.anomaly_features import build_anomaly_features, isolation_forest_scores, rule_based_flags
from spark.spark_session import build_spark, lake_uri

ANOMALY_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("tx_ts", StringType()),
        StructField("tx_date", StringType()),
        StructField("operator", StringType()),
        StructField("tx_type", StringType()),
        StructField("channel", StringType()),
        StructField("sender_id", StringType()),
        StructField("receiver_id", StringType()),
        StructField("sender_region", StringType()),
        StructField("amount", DoubleType()),
        StructField("status", StringType()),
        StructField("is_high_amount", BooleanType()),
        StructField("is_frequency_anomaly", BooleanType()),
        StructField("is_repeated_pair", BooleanType()),
        StructField("anomaly_rules", StringType()),
        StructField("if_label", StringType()),
        StructField("if_score", DoubleType()),
    ]
)

FEATURE_COLUMNS = [
    "transaction_id", "tx_ts", "tx_date", "operator", "tx_type", "channel",
    "sender_id", "receiver_id", "sender_region", "amount", "status", "tx_hour",
]


def to_spark_schema(suspects: pd.DataFrame):
    out = pd.DataFrame({
        "transaction_id": suspects["transaction_id"].astype(str),
        "tx_ts": suspects["tx_ts"].dt.strftime("%Y-%m-%d %H:%M:%S"),
        "tx_date": suspects["tx_date"].astype(str),
        "operator": suspects["operator"].astype(str),
        "tx_type": suspects["tx_type"].astype(str),
        "channel": suspects["channel"].astype(str),
        "sender_id": suspects["sender_id"].astype(str),
        "receiver_id": suspects["receiver_id"].astype(str),
        "sender_region": suspects["sender_region"].astype(str),
        "amount": suspects["amount"].astype("float64"),
        "status": suspects["status"].astype(str),
        "is_high_amount": suspects["is_high_amount"].astype(bool),
        "is_frequency_anomaly": suspects["is_frequency_anomaly"].astype(bool),
        "is_repeated_pair": suspects["is_repeated_pair"].astype(bool),
        "anomaly_rules": suspects["anomaly_rules"].astype(str),
        "if_label": suspects["if_label"].astype(str),
        "if_score": suspects["if_score"].astype("float64"),
    })
    return out


def detect(spark, sample_size: int):
    silver = spark.read.parquet(lake_uri(LAKE_SILVER_PREFIX)).select(*FEATURE_COLUMNS)

    sample = silver if sample_size <= 0 else silver.limit(sample_size)
    pandas_df = sample.toPandas()
    pandas_df["tx_ts"] = pd.to_datetime(pandas_df["tx_ts"])
    pandas_df["tx_date"] = pandas_df["tx_ts"].dt.date

    features = build_anomaly_features(pandas_df)
    flagged = rule_based_flags(features)
    labels, scores = isolation_forest_scores(flagged)
    flagged["if_label"] = pd.Series(labels).map({-1: "ANOMALY_IA", 1: "OK"}).values
    flagged["if_score"] = scores.astype("float64")
    flagged["anomaly_rules"] = flagged["anomaly_rules"].fillna("")

    suspects = flagged[(flagged["anomaly_rules"] != "") | (flagged["if_label"] == "ANOMALY_IA")]
    suspects_spark = to_spark_schema(suspects)
    spark_df = spark.createDataFrame(suspects_spark, schema=ANOMALY_SCHEMA)

    anomalies_output = lake_uri(f"{LAKE_GOLD_PREFIX}/anomalies")
    spark_df.write.mode("overwrite").parquet(anomalies_output)

    summary_output = lake_uri(f"{LAKE_GOLD_PREFIX}/agg_suspects")
    (
        spark_df.groupBy("tx_date", "operator")
        .agg(
            F.count("*").alias("suspect_count"),
            F.sum("is_high_amount").alias("high_amount_count"),
            F.sum("is_frequency_anomaly").alias("frequency_count"),
            F.sum("is_repeated_pair").alias("repeated_pairs_count"),
            F.round(F.avg("amount"), 2).alias("avg_suspect_amount"),
        )
        .write.mode("overwrite")
        .parquet(summary_output)
    )

    return len(suspects), anomalies_output, summary_output


def main():
    parser = argparse.ArgumentParser(description="Détection d'anomalies (règles + Isolation Forest)")
    parser.add_argument("--sample", type=int, default=300_000, help="taille de l'échantillon (0 = tout)")
    args = parser.parse_args()

    spark = build_spark("afridata-anomalies")
    suspects, anomalies_output, summary_output = detect(spark, args.sample)
    print(f"ANOMALIES OK : {suspects} suspects -> {anomalies_output} / {summary_output}")
    spark.stop()


if __name__ == "__main__":
    main()
