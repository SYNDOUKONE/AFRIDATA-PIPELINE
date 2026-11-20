"""Tests de l'évaluateur de contrôles qualité."""

from spark.quality_checks import QUALITY_CHECKS, evaluate


def test_all_checks_pass():
    metrics = {c.name: (1_000, 0) for c in QUALITY_CHECKS}
    report = evaluate(metrics)
    assert report["status"] == "PASSED"
    assert report["failed_checks"] == []
    assert report["checks_total"] == len(QUALITY_CHECKS)


def test_duplicate_ids_fail_uniqueness():
    metrics = {"uniqueness_transaction_id": (1_000, 25)}
    report = evaluate(metrics)
    assert report["status"] == "FAILED"
    assert "uniqueness_transaction_id" in report["failed_checks"]


def test_failure_ratio_warning_threshold():
    report = evaluate({"failure_ratio": (1_000, 119)})
    assert report["checks"][0]["passed"] is True

    report = evaluate({"failure_ratio": (1_000, 150)})
    assert report["checks"][0]["passed"] is False


def test_partial_failure_reports_failed_checks_sorted():
    metrics = {
        "amount_positivity": (500, 10),
        "currency_domain": (500, 0),
        "uniqueness_transaction_id": (500, 3),
    }
    report = evaluate(metrics)
    assert report["failed_checks"] == ["amount_positivity", "uniqueness_transaction_id"]
    assert report["checks_passed"] == 1


def test_empty_table_passes():
    report = evaluate({"amount_positivity": (0, 0)})
    assert report["status"] == "PASSED"


def test_at_least_eight_checks_defined():
    assert len(QUALITY_CHECKS) >= 8
