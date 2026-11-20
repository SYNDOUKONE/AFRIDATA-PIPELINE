"""Tests du module de validation d'événements."""

from ingestion.validators import parse_amount, parse_timestamp, validate_event


def make_valid_event():
    return {
        "transaction_id": "abc-123",
        "timestamp": "2024-12-31 10:23:00",
        "operator": "Wave",
        "tx_type": "TRANSFER",
        "amount": 5_000.0,
        "currency": "XOF",
        "sender_id": "+2250701020304",
        "receiver_id": "+2250501020304",
        "status": "SUCCESS",
    }


def test_valid_event_has_no_errors():
    assert validate_event(make_valid_event()) == []


def test_missing_required_fields():
    event = make_valid_event()
    del event["amount"]
    del event["status"]
    errors = validate_event(event)
    assert "missing_fields:amount,status" in errors


def test_unknown_operator_and_type():
    event = make_valid_event()
    event["operator"] = "Free Money"
    event["tx_type"] = "TELEPORTATION"
    errors = validate_event(event)
    assert "unknown_operator" in errors
    assert "unknown_tx_type" in errors


def test_invalid_and_negative_amounts():
    event = make_valid_event()
    event["amount"] = "abc"
    assert "invalid_amount" in validate_event(event)

    event["amount"] = -150
    assert "negative_amount" in validate_event(event)


def test_timestamp_accepts_dirty_formats():
    event = make_valid_event()
    for raw, expected in [
        ("31/12/2024 10:23", True),
        ("20241231102300", True),
        ("31-12-2024 10:23:00", True),
        ("2024-13-45 99:99", False),
        ("", False),
        (None, False),
    ]:
        event["timestamp"] = raw
        errors = validate_event(event)
        assert ("invalid_timestamp" in errors) != expected


def test_parse_timestamp_returns_datetime():
    from datetime import datetime

    assert parse_timestamp("2024-12-31 10:23:00") == datetime(2024, 12, 31, 10, 23)
    assert parse_timestamp("31/12/2024 10:23") == datetime(2024, 12, 31, 10, 23)
    assert parse_timestamp("garbage") is None


def test_parse_amount_variants():
    assert parse_amount("1 250,50 F") == 1250.50
    assert parse_amount("1250.50") == 1250.50
    assert parse_amount(1234) == 1234.0
    assert parse_amount("F CFA 99,99") == 99.99
    assert parse_amount("n/a") is None
    assert parse_amount(None) is None
