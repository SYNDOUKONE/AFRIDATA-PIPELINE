"""DAG AFRIDATA - pipeline transactions de bout en bout.

Toutes les tâches s'exécutent via `docker exec` dans les conteneurs de travail
(afridata-python-jobs, afridata-spark-jobs) démarrés par docker-compose.

Étapes : generate -> push_landing -> bronze -> silver (quality gate)
         -> gold -> anomalies -> load_warehouse
"""

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime

PY_JOBS = os.environ.get("PY_JOBS_CONTAINER", "afridata-python-jobs")
SPARK_JOBS = os.environ.get("SPARK_JOBS_CONTAINER", "afridata-spark-jobs")
NB_TRANSACTIONS = os.environ.get("NB_TRANSACTIONS", "1000000")
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3"

SPARK_SUBMIT = (
    f"spark-submit --packages {SPARK_PACKAGES} "
    "--conf spark.sql.shuffle.partitions=64"
)

default_args = {
    "owner": "afridata",
    "retries": 1,
    "retry_delay": 300,
}

with DAG(
    dag_id="afridata_transaction_pipeline",
    description="Pipeline ETL complet des transactions mobile money",
    start_date=datetime(2025, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["afridata", "mobile-money", "medallion"],
    default_args=default_args,
) as dag:

    generate = BashOperator(
        task_id="generate_transactions",
        bash_command=(
            f"docker exec {PY_JOBS} python ingestion/generate_transactions.py "
            f"--rows {NB_TRANSACTIONS}"
        ),
    )

    push_landing = BashOperator(
        task_id="push_landing_to_lake",
        bash_command=f"docker exec {PY_JOBS} python ingestion/push_to_lake.py --pattern *.csv",
    )

    bronze = BashOperator(
        task_id="bronze_pipeline",
        bash_command=f"docker exec {SPARK_JOBS} {SPARK_SUBMIT} /opt/project/spark/bronze_pipeline.py",
    )

    silver = BashOperator(
        task_id="silver_pipeline_quality_gate",
        bash_command=f"docker exec {SPARK_JOBS} {SPARK_SUBMIT} /opt/project/spark/silver_pipeline.py",
    )

    gold = BashOperator(
        task_id="gold_pipeline",
        bash_command=f"docker exec {SPARK_JOBS} {SPARK_SUBMIT} /opt/project/spark/gold_pipeline.py",
    )

    anomalies = BashOperator(
        task_id="anomaly_detection",
        bash_command=f"docker exec {SPARK_JOBS} {SPARK_SUBMIT} /opt/project/spark/anomaly_detection.py --sample 300000",
    )

    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command=f"docker exec {SPARK_JOBS} {SPARK_SUBMIT} /opt/project/spark/load_warehouse.py",
    )

    generate >> push_landing >> bronze >> silver >> gold
    gold >> anomalies >> load_warehouse
