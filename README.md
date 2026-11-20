# AFRIDATA PIPELINE

**Plateforme Big Data de collecte, traitement et analyse des transactions financières (mobile money, Côte d'Ivoire).**

Ce projet simule une infrastructure de données de niveau production pour Orange Money, MTN Mobile Money, Wave et Moov Money : ingestion en flux via Kafka, traitement distribué avec Apache Spark, Data Lake Medallion (Bronze / Silver / Gold) sur MinIO, Data Warehouse PostgreSQL, orchestration Apache Airflow, contrôles qualité automatisés, détection d'anomalies et restitution BI (Power BI).

---

## 1. Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │                  SOURCES                     │
                    │  Générateur Python (batch 1M / streaming)    │
                    │  CSV + JSON + Kafka topic transactions       │
                    └──────────────┬───────────────────────────────┘
                                   │
                     ┌─────────────▼─────────────┐
                     │        KAFKA (KRaft)      │
                     │  topic: transactions.raw  │
                     └─────────────┬─────────────┘
                                   │  kafka_consumer.py
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        DATA LAKE  (MinIO / S3)                           │
│                                                                          │
│   BRONZE (brut immuable)      SILVER (nettoyé, dédoublonné)              │
│   Parquet / date, operator    Parquet / operator, date                   │
│            │                          │   ▲                              │
│            │      spark silver_pipeline.py + quality_checks.py (gate)    │
│            │                          ▼   │                              │
│            │                   GOLD (agrégats analytiques)  ──► ANOMALIES │
└────────────┼──────────────────────────────┼──────────────┬───────────────┘
             │                              │              │
   spark bronze_pipeline.py       load_warehouse.py   anomaly_detection.py
             │                              │              │
                     ┌─────────────▼─────────────▼──────────────┐
                     │   POSTGRESQL — DATA WAREHOUSE            │
                     │   fct_transactions + tables analytiques  │
                     └─────────────┬────────────────────────────┘
                                   │
                     ┌─────────────▼─────────────┐
                     │  POWER BI / SQL (vues BI) │
                     │  KPI, tendances, suspectes│
                     └───────────────────────────┘

        AIRFLOW orchestre : generate → bronze → silver → quality_gate
                                  → gold → anomalies → load_warehouse
```

## 2. Stack technique

| Technologie    | Rôle                                                        |
| -------------- | ----------------------------------------------------------- |
| Python 3.12    | Génération des données, ingestion, contrôles qualité, tests |
| Apache Kafka   | Ingestion streaming (KRaft, single node)                    |
| Apache Spark   | Traitement distribué ETL (Bronze → Silver → Gold)           |
| MinIO          | Data Lake (S3 compatible, Parquet)                          |
| PostgreSQL     | Data Warehouse (tables analytiques + vues BI)               |
| Apache Airflow | Orchestration et planification des pipelines                |
| scikit-learn   | Isolation Forest (détection d'anomalies non supervisée)     |
| Docker         | Conteneurisation de toute l'infrastructure                  |
| Power BI       | Visualisation décisionnelle                                 |

## 3. Démarrage rapide

Prérequis : Docker + Docker Compose v2 (WSL2, Linux ou macOS).

```bash
cp .env.example .env
make up            # démarre Kafka, MinIO, PostgreSQL, Spark, Airflow
make generate      # génère 1 000 000 transactions (approx. 1-2 min)
make ingest-csv    # pousse les fichiers bruts vers le Data Lake (landing)
make stream        # optionnel : producteur Kafka en continu (Ctrl+C pour stop)
make run-bronze    # Bronze : Parquet partitionné dans MinIO
make run-silver    # Silver : nettoyage + dédoublonnage + qualité (quality gate)
make run-gold      # Gold : agrégats analytiques
make run-anomalies # Détection d'anomalies (règles + Isolation Forest)
make load-warehouse# Chargement PostgreSQL
```

Airflow : http://localhost:8080 (cliquez le DAG `afridata_transaction_pipeline`).
MinIO : http://localhost:9001 · Spark UI : http://localhost:4040 pendant un job.

Le DAG Airflow enchaîne automatiquement toutes les étapes ; le `make` ci-dessus
sert à rejouer les étapes une par une (utile en développement).

## 4. Fonctionnalités

- **Ingestion** : 1 000 000+ transactions simulées (batch CSV/JSON + streaming Kafka), 4 opérateurs, dirty data injectée volontairement (doublons, formats de dates hétérogènes, montants négatifs, devises invalides, valeurs manquantes).
- **ETL distribué Spark** : dédoublonnage, normalisation dates/montants/devise, filtres métier, rejet des transactions invalides (couche de rejet dédiée).
- **Data Lake Medallion** : Bronze (immuable) / Silver (contrat de données) / Gold (agrégats), Parquet partitionné, stockage MinIO.
- **Qualité des données** : 10 contrôles automatisés (unicité, complétude, domaines de valeurs, cohérence des montants, fraîcheur des dates, taux d'échec…), rapport JSON dans le lake et quality gate Airflow.
- **Détection d'anomalies** : percentiles par opérateur/type, z-scores, paires répétées, fréquences anormales, Isolation Forest (scikit-learn).
- **Warehouse** : 8 tables analytiques PostgreSQL + 5 vues BI prêtes pour Power BI.
- **Dashboard** : guide de construction Power BI + requêtes SQL + KPI listés dans `dashboard/`.

## 5. Captures d'écran du dashboard BI

Générées depuis les tables Gold (50 000 transactions) par `dashboard/generate_dashboard.py` :

| Page | Visuel |
| ---- | ------ |
| [Vue d'ensemble](dashboard/img/page1_overview.png) | KPI, évolution, part opérateurs |
| [Anomalies](dashboard/img/page2_anomalies.png) | 495 suspects, règles + Isolation Forest |
| [Comportement](dashboard/img/page3_behavior.png) | Heatmap horaire, régions, types |

## 6. Structure du dépôt

```
AFRIDATA-PIPELINE/
├── data/                  # local uniquement (raw/, processed/, samples/)
├── ingestion/             # générateur + producteur/consommateur Kafka
├── spark/                 # pipelines Bronze/Silver/Gold, qualité, anomalies
├── airflow/dags/          # DAG transaction_pipeline
├── warehouse/             # schema.sql + transformations.sql (vues BI)
├── dashboard/             # plan du dashboard Power BI + KPI
├── tests/                 # tests unitaires (pytest)
├── docker-compose.yml     # Kafka, MinIO, PostgreSQL, Spark, Airflow
├── Makefile               # commandes courantes
└── README.md
```

## 6. Indicateurs de réalisation

| Indicateur            | Objectif             | Résultat |
| --------------------- | -------------------- | -------- |
| Transactions générées | 1 000 000            | 1 000 000 (batch) + flux continu |
| Format de stockage    | Parquet              | ✓ partitionné (operator, date) |
| Pipelines automatisés | 3 à 5                | 7 étapes dans le DAG Airflow |
| Contrôles qualité     | Au moins 8           | 10 |
| Tables analytiques    | 5 à 8                | 8 + 5 vues BI |
| Dashboard             | 1 complet            | Power BI (`dashboard/`) |

## 7. Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## 8. Présentation sur un CV

**AFRIDATA PIPELINE — Plateforme Big Data de traitement des transactions financières**
*Data Engineering / Big Data / Streaming — Python, Kafka, Spark, Airflow, MinIO, PostgreSQL, Docker, Power BI*

- Conception d'une architecture Big Data end-to-end pour l'ingestion de transactions financières simulées (mobile money, Côte d'Ivoire).
- Pipelines ETL distribués Apache Spark avec architecture Data Lake Medallion (Bronze / Silver / Gold) sur MinIO (Parquet partitionné).
- Ingestion hybride batch + streaming : producteur/consommateur Kafka, files de transactions temps réel.
- Orchestration et planification des workflows avec Apache Airflow (quality gate automatisé).
- Mise en place de 10 contrôles qualité automatisés et d'un module de détection d'anomalies (règles statistiques + Isolation Forest).
- Data Warehouse PostgreSQL (8 tables analytiques, 5 vues BI) alimentant un dashboard Power BI (KPI, tendances, transactions suspectes).
