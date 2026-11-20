"""Validation pure d'événements de transaction (utilisé par le consommateur Kafka et les tests)."""

from datetime import datetime

from ingestion.config import DATE_FORMATS, OPERATORS, TX_TYPES

REQUIRED_FIELDS = [
    "transaction_id",
    "timestamp",
    "operator",
    "tx_type",
    "amount",
    "currency",
    "sender_id",
    "status",
]


def validate_event(event: dict) -> list:
    """Retourne la liste des erreurs détectées (liste vide = événement valide)."""
    errors = []

    missing = [f for f in REQUIRED_FIELDS if event.get(f) in (None, "")]
    if missing:
        errors.append(f"missing_fields:{','.join(missing)}")

    if parse_timestamp(event.get("timestamp")) is None:
        errors.append("invalid_timestamp")

    amount = parse_amount(event.get("amount"))
    if amount is None:
        errors.append("invalid_amount")
    elif amount < 0:
        errors.append("negative_amount")

    if event.get("operator") not in OPERATORS:
        errors.append("unknown_operator")

    if event.get("tx_type") not in TX_TYPES:
        errors.append("unknown_tx_type")

    return errors


def parse_timestamp(raw):
    if isinstance(raw, datetime):
        return raw
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def parse_amount(raw):
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip().replace("\u00a0", " ")
    for token in ["F CFA", "FCFA", "F", "XOF"]:
        text = text.replace(token, "").strip()
    text = text.replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None
