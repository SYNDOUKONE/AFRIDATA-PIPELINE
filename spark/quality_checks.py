"""Contrôles qualité déclaratifs (SQL Spark) + évaluateur pur Python testable hors Spark."""

import json
from dataclasses import dataclass
from pathlib import Path

OPERATOR_WHITELIST = ["Orange Money", "MTN Mobile Money", "Wave", "Moov Money"]
STATUS_WHITELIST = ["SUCCESS", "FAILED", "PENDING"]
TX_TYPE_WHITELIST = ["TRANSFER", "DEPOSIT", "WITHDRAWAL", "PAYMENT", "AIRTIME", "BILL_PAYMENT"]
CURRENCY_WHITELIST = ["XOF"]
MAX_AMOUNT_FCFA = 100_000_000.00


@dataclass(frozen=True)
class QualityCheck:
    name: str
    violations_sql: str
    threshold_ratio: float
    severity: str


def _in_list(values: list) -> str:
    return ", ".join(f"'{v}'" for v in values)


QUALITY_CHECKS = [
    QualityCheck(
        "uniqueness_transaction_id",
        "transaction_id IN (SELECT transaction_id FROM __df__ GROUP BY transaction_id HAVING count(*) > 1)",
        0.0,
        "critical",
    ),
    QualityCheck(
        "completeness_required_fields",
        "transaction_id IS NULL OR timestamp IS NULL OR operator IS NULL OR amount IS NULL "
        "OR currency IS NULL OR sender_id IS NULL OR status IS NULL OR tx_ts IS NULL",
        0.0,
        "critical",
    ),
    QualityCheck("amount_positivity", "amount IS NULL OR amount <= 0", 0.0, "critical"),
    QualityCheck("amount_plausibility", f"amount > {MAX_AMOUNT_FCFA:.2f}", 0.001, "critical"),
    QualityCheck("currency_domain", f"currency NOT IN ({_in_list(CURRENCY_WHITELIST)})", 0.0, "critical"),
    QualityCheck("operator_domain", f"operator NOT IN ({_in_list(OPERATOR_WHITELIST)})", 0.0, "critical"),
    QualityCheck("tx_type_domain", f"tx_type NOT IN ({_in_list(TX_TYPE_WHITELIST)})", 0.0, "critical"),
    QualityCheck("status_domain", f"status NOT IN ({_in_list(STATUS_WHITELIST)})", 0.0, "critical"),
    QualityCheck("parseable_timestamp", "tx_ts IS NULL", 0.0, "critical"),
    QualityCheck(
        "referential_integrity_sender_receiver",
        "sender_id = receiver_id",
        0.001,
        "warning",
    ),
    QualityCheck("failure_ratio", "status = 'FAILED'", 0.12, "warning"),
]


def run_checks(spark, df, view_name: str = "__df__") -> dict:
    """Exécute les contrôles déclaratifs sur un DataFrame et retourne {nom: (total, violations)}."""
    df.createOrReplaceTempView(view_name)
    metrics = {}
    for check in QUALITY_CHECKS:
        query = (
            f"SELECT count(*) AS total, "
            f"sum(CASE WHEN ({check.violations_sql}) THEN 1 ELSE 0 END) AS violations "
            f"FROM {view_name}"
        )
        row = spark.sql(query).collect()[0]
        metrics[check.name] = (row["total"], row["violations"] or 0)
    return metrics


def evaluate(raw_metrics: dict, thresholds: dict | None = None) -> dict:
    """Évalue {nom: (total, violations)} et produit un rapport (pass/fail par contrôle)."""
    thresholds = thresholds or {c.name: c.threshold_ratio for c in QUALITY_CHECKS}
    records = []
    for name, (total, violations) in raw_metrics.items():
        total = int(total or 0)
        violations = int(violations or 0)
        ratio = violations / total if total else 0.0
        threshold = thresholds.get(name, 0.0)
        records.append(
            {
                "check": name,
                "total": total,
                "violations": violations,
                "violation_ratio": round(ratio, 6),
                "threshold_ratio": threshold,
                "passed": ratio <= threshold,
            }
        )
    failed = sorted(r["check"] for r in records if not r["passed"])
    return {
        "status": "PASSED" if not failed else "FAILED",
        "checks_passed": len(records) - len(failed),
        "checks_total": len(records),
        "failed_checks": failed,
        "checks": records,
    }


def write_report(report: dict, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False)
