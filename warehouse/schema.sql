-- =====================================================================
-- AFRIDATA PIPELINE - Data Warehouse PostgreSQL
-- Créé automatiquement au premier démarrage du conteneur postgres
-- (monté dans /docker-entrypoint-initdb.d/01-schema.sql)
-- =====================================================================

-- ---------------------------------------------------------------------
-- Table de faits : transactions nettoyées (couche Silver)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fct_transactions (
    transaction_id      VARCHAR(64) PRIMARY KEY,
    tx_ts_str           VARCHAR(32),
    tx_date             DATE,
    tx_year_month       VARCHAR(7),
    tx_hour             INTEGER,
    operator            VARCHAR(50),
    tx_type             VARCHAR(30),
    channel             VARCHAR(20),
    amount              NUMERIC(18, 2),
    fee                 NUMERIC(18, 2),
    currency            VARCHAR(8),
    sender_id           VARCHAR(20),
    receiver_id         VARCHAR(20),
    agent_id            VARCHAR(20),
    device_id           VARCHAR(30),
    sender_region       VARCHAR(50),
    status              VARCHAR(20),
    is_failed           INTEGER,
    amount_bucket       VARCHAR(20),
    source_file         TEXT
);

CREATE INDEX IF NOT EXISTS idx_fct_tx_date ON fct_transactions (tx_date);
CREATE INDEX IF NOT EXISTS idx_fct_operator ON fct_transactions (operator);
CREATE INDEX IF NOT EXISTS idx_fct_sender ON fct_transactions (sender_id);

-- ---------------------------------------------------------------------
-- KPIs globaux (couche Gold)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kpi_overview (
    total_transactions  BIGINT,
    total_amount        NUMERIC(20, 2),
    avg_amount          NUMERIC(18, 2),
    total_fees          NUMERIC(20, 2),
    total_failed        BIGINT,
    distinct_senders    BIGINT,
    operator_count      BIGINT,
    last_transaction_at TIMESTAMP,
    fail_rate           NUMERIC(8, 6)
);

-- Volume quotidien global
CREATE TABLE IF NOT EXISTS agg_daily_global (
    tx_date         DATE,
    tx_count        BIGINT,
    total_amount    NUMERIC(20, 2),
    avg_amount      NUMERIC(18, 2),
    total_fees      NUMERIC(20, 2),
    failed_count    BIGINT,
    active_senders  BIGINT,
    fail_rate       NUMERIC(8, 6),
    PRIMARY KEY (tx_date)
);

-- Volume quotidien par opérateur
CREATE TABLE IF NOT EXISTS agg_daily_operator (
    tx_date         DATE,
    operator        VARCHAR(50),
    tx_count        BIGINT,
    total_amount    NUMERIC(20, 2),
    avg_amount      NUMERIC(18, 2),
    total_fees      NUMERIC(20, 2),
    failed_count    BIGINT,
    fail_rate       NUMERIC(8, 6),
    PRIMARY KEY (tx_date, operator)
);

-- Volumes mensuels par opérateur et type
CREATE TABLE IF NOT EXISTS agg_monthly_operator (
    tx_year_month   VARCHAR(7),
    operator        VARCHAR(50),
    tx_type         VARCHAR(30),
    tx_count        BIGINT,
    total_amount    NUMERIC(20, 2),
    failed_count    BIGINT,
    fail_rate       NUMERIC(8, 6),
    PRIMARY KEY (tx_year_month, operator, tx_type)
);

-- Profil horaire par canal
CREATE TABLE IF NOT EXISTS agg_hourly_pattern (
    tx_hour         INTEGER,
    channel         VARCHAR(20),
    tx_count        BIGINT,
    total_amount    NUMERIC(20, 2),
    avg_amount      NUMERIC(18, 2),
    PRIMARY KEY (tx_hour, channel)
);

-- Répartition géographique simulée
CREATE TABLE IF NOT EXISTS agg_region (
    sender_region   VARCHAR(50),
    operator        VARCHAR(50),
    tx_count        BIGINT,
    total_amount    NUMERIC(20, 2),
    failed_count    BIGINT,
    fail_rate       NUMERIC(8, 6),
    PRIMARY KEY (sender_region, operator)
);

-- Répartition par type, canal et tranche de montant
CREATE TABLE IF NOT EXISTS agg_tx_type_channel (
    tx_type         VARCHAR(30),
    channel         VARCHAR(20),
    amount_bucket   VARCHAR(20),
    tx_count        BIGINT,
    total_amount    NUMERIC(20, 2),
    PRIMARY KEY (tx_type, channel, amount_bucket)
);

-- ---------------------------------------------------------------------
-- Anomalies (détection : règles + Isolation Forest)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fct_suspect_transactions (
    transaction_id         VARCHAR(64),
    tx_ts_str              VARCHAR(32),
    tx_date                DATE,
    operator               VARCHAR(50),
    tx_type                VARCHAR(30),
    channel                VARCHAR(20),
    sender_id              VARCHAR(20),
    receiver_id            VARCHAR(20),
    sender_region          VARCHAR(50),
    amount                 NUMERIC(18, 2),
    status                 VARCHAR(20),
    is_high_amount         BOOLEAN,
    is_frequency_anomaly   BOOLEAN,
    is_repeated_pair       BOOLEAN,
    anomaly_rules          TEXT,
    if_label               VARCHAR(20),
    if_score               NUMERIC(10, 6)
);

CREATE INDEX IF NOT EXISTS idx_suspects_tx_date ON fct_suspect_transactions (tx_date);

CREATE TABLE IF NOT EXISTS agg_suspects (
    tx_date               DATE,
    operator              VARCHAR(50),
    suspect_count         BIGINT,
    high_amount_count     BIGINT,
    frequency_count       BIGINT,
    repeated_pairs_count  BIGINT,
    avg_suspect_amount    NUMERIC(18, 2)
);

-- ---------------------------------------------------------------------
-- Suivi qualité
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_quality_reports (
    id              BIGSERIAL PRIMARY KEY,
    run_date        VARCHAR(32),
    status          VARCHAR(10),
    checks_passed   INTEGER,
    checks_total    INTEGER,
    failed_checks   TEXT,
    rows_silver     BIGINT,
    rows_rejected   BIGINT,
    created_at      TIMESTAMP DEFAULT now()
);
