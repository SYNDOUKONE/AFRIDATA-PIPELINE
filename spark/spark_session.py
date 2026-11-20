"""Session Spark configurée pour MinIO (S3A) et le warehouse PostgreSQL."""

import os

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "afridata")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "afridata_secret")
    shuffle_partitions = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64")

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def lake_uri(path: str) -> str:
    bucket = os.environ.get("DATALAKE_BUCKET", "datalake")
    return f"s3a://{bucket}/{path.lstrip('/')}"
