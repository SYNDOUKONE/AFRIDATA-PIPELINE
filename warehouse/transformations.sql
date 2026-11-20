-- =====================================================================
-- AFRIDATA PIPELINE - Transformations analytiques (vues BI)
-- Chargeable après chaque pipeline :  make psql  puis  \i /opt/project/warehouse/transformations.sql
-- Ces vues alimentent directement le dashboard Power BI.
-- =====================================================================

-- Vue 1 : vue d'ensemble des KPIs
CREATE OR REPLACE VIEW v_kpi_overview AS
SELECT
    total_transactions,
    total_amount,
    avg_amount,
    total_fees,
    total_failed,
    fail_rate,
    distinct_senders,
    operator_count,
    last_transaction_at
FROM kpi_overview;

-- Vue 2 : évolution quotidienne du volume et des montants
CREATE OR REPLACE VIEW v_daily_evolution AS
SELECT
    d.tx_date,
    d.tx_count,
    d.total_amount,
    d.avg_amount,
    d.failed_count,
    d.fail_rate,
    d.active_senders
FROM agg_daily_global d
ORDER BY d.tx_date;

-- Vue 3 : performance des opérateurs (part de marché calculée)
CREATE OR REPLACE VIEW v_operator_performance AS
SELECT
    o.operator,
    SUM(o.tx_count)                                        AS tx_count,
    SUM(o.total_amount)                                    AS total_amount,
    ROUND(SUM(o.total_amount) / NULLIF(SUM(SUM(o.total_amount)) OVER (), 0), 4) AS market_share,
    ROUND(SUM(o.failed_count)::numeric / NULLIF(SUM(o.tx_count), 0), 6)         AS fail_rate,
    ROUND(SUM(o.total_fees), 2)                            AS total_fees
FROM agg_daily_operator o
GROUP BY o.operator;

-- Vue 4 : répartition géographique (carte Power BI)
CREATE OR REPLACE VIEW v_region_map AS
SELECT
    r.sender_region,
    SUM(r.tx_count)     AS tx_count,
    SUM(r.total_amount) AS total_amount,
    ROUND(SUM(r.failed_count)::numeric / NULLIF(SUM(r.tx_count), 0), 6) AS fail_rate
FROM agg_region r
GROUP BY r.sender_region;

-- Vue 5 : liste des transactions suspectes (détail)
CREATE OR REPLACE VIEW v_suspect_list AS
SELECT
    s.transaction_id,
    s.tx_ts_str,
    s.operator,
    s.tx_type,
    s.amount,
    s.sender_id,
    s.receiver_id,
    s.sender_region,
    s.anomaly_rules,
    s.if_label,
    s.if_score
FROM fct_suspect_transactions s
ORDER BY s.amount DESC;

-- Vue 6 : activité horaire (heatmap heure x canal)
CREATE OR REPLACE VIEW v_hourly_activity AS
SELECT
    h.tx_hour,
    h.channel,
    h.tx_count,
    h.total_amount,
    h.avg_amount
FROM agg_hourly_pattern h
ORDER BY h.tx_hour;

-- Vue 7 : taux d'échec quotidien par opérateur (alerte qualité de service)
CREATE OR REPLACE VIEW v_daily_failures AS
SELECT
    o.tx_date,
    o.operator,
    o.failed_count,
    o.tx_count,
    o.fail_rate
FROM agg_daily_operator o
WHERE o.fail_rate > 0.05
ORDER BY o.fail_rate DESC, o.tx_date;
