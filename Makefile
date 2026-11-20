.PHONY: up down restart logs build ps \
        generate samples ingest-csv push-json stream consume \
        run-bronze run-silver run-gold run-anomalies load-warehouse quality-report \
        airflow-init airflow \
        test lint spark-shell psql minio-console clean

COMPOSE = docker compose
PYJOBS  = afridata-python-jobs
SPARKJOBS = afridata-spark-jobs
SPARK_PACKAGES = org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3

up:
	$(COMPOSE) up -d postgres minio minio-init kafka
	$(COMPOSE) up -d python-jobs spark-jobs

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

## ---------- Ingestion ----------

NB ?= 1000000

generate:
	$(COMPOSE) exec $(PYJOBS) python ingestion/generate_transactions.py --rows $(NB)

generate-small:
	$(COMPOSE) exec $(PYJOBS) python ingestion/generate_transactions.py --rows 50000

samples:
	$(COMPOSE) exec $(PYJOBS) python ingestion/generate_transactions.py --rows 10000 --outdir data/samples

ingest-csv:
	$(COMPOSE) exec $(PYJOBS) python ingestion/push_to_lake.py --source data/raw --target s3-landing

push-json:
	$(COMPOSE) exec $(PYJOBS) python ingestion/push_to_lake.py --source data/raw --target s3 --pattern "*.json"

stream:
	$(COMPOSE) exec $(PYJOBS) python ingestion/kafka_producer.py --rate 200

consume:
	$(COMPOSE) exec $(PYJOBS) python ingestion/kafka_consumer.py

## ---------- Spark pipelines ----------

SPARK_SUBMIT = $(COMPOSE) exec $(SPARKJOBS) spark-submit \
	--packages $(SPARK_PACKAGES) \
	--conf spark.sql.shuffle.partitions=64 \
	--conf spark.hadoop.fs.s3a.endpoint=$$S3_ENDPOINT \
	--conf spark.hadoop.fs.s3a.path.style.access=true \
	--conf spark.hadoop.fs.s3a.access.key=$$AWS_ACCESS_KEY_ID \
	--conf spark.hadoop.fs.s3a.secret.key=$$AWS_SECRET_ACCESS_KEY \
	--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
	--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem

run-bronze:
	$(SPARK_SUBMIT) /opt/project/spark/bronze_pipeline.py

run-silver:
	$(SPARK_SUBMIT) /opt/project/spark/silver_pipeline.py

quality-report:
	$(SPARK_SUBMIT) /opt/project/spark/quality_report.py

run-gold:
	$(SPARK_SUBMIT) /opt/project/spark/gold_pipeline.py

run-anomalies:
	$(SPARK_SUBMIT) /opt/project/spark/anomaly_detection.py

load-warehouse:
	$(SPARK_SUBMIT) /opt/project/spark/load_warehouse.py

## ---------- Airflow ----------

airflow-up:
	$(COMPOSE) up -d airflow-init && $(COMPOSE) up -d airflow-scheduler airflow-webserver
	@echo "Airflow : http://localhost:8080"

## ---------- Qualité ----------

test:
	pytest tests/ -v

## ---------- Utilitaires ----------

psql:
	$(COMPOSE) exec postgres psql -U ${POSTGRES_USER:-afridata} -d ${POSTGRES_DB:-warehouse}

spark-shell:
	$(COMPOSE) exec $(SPARKJOBS) spark-shell --packages $(SPARK_PACKAGES)
