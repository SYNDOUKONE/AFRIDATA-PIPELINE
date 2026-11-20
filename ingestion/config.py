"""Constantes métier partagées pour la simulation mobile money (Côte d'Ivoire)."""

OPERATORS = {
    "Orange Money": 0.34,
    "MTN Mobile Money": 0.27,
    "Wave": 0.29,
    "Moov Money": 0.10,
}

TX_TYPES = {
    "TRANSFER": 0.30,
    "DEPOSIT": 0.22,
    "WITHDRAWAL": 0.24,
    "PAYMENT": 0.14,
    "AIRTIME": 0.08,
    "BILL_PAYMENT": 0.02,
}

CHANNELS = {"USSD": 0.45, "APP": 0.30, "AGENT": 0.25}

STATUS = {"SUCCESS": 0.93, "FAILED": 0.05, "PENDING": 0.02}

FAILURE_REASONS = {
    "FAILED": [
        "insufficient_balance",
        "network_timeout",
        "limit_exceeded",
        "invalid_pin",
        "receiver_account_not_found",
    ]
}

REGIONS = {
    "Abidjan": 0.42,
    "Yamoussoukro": 0.06,
    "Bouaké": 0.09,
    "Daloa": 0.06,
    "San-Pédro": 0.06,
    "Korhogo": 0.07,
    "Man": 0.05,
    "Gagnoa": 0.05,
    "Abengourou": 0.05,
    "Divo": 0.04,
    "Bondoukou": 0.03,
    "Séguéla": 0.02,
}

CURRENCY = "XOF"

INVALID_CURRENCIES = ["EUR", "USD", "$", "FCFA", ""]

DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%Y%m%d%H%M%S",
    "%d-%m-%Y %H:%M:%S",
]

LAKE_LANDING_PREFIX = "raw/transactions/landing"
LAKE_KAFKA_PREFIX = "raw/transactions/kafka"
LAKE_BRONZE_PREFIX = "bronze/transactions"
LAKE_SILVER_PREFIX = "silver/transactions"
LAKE_SILVER_REJECTED_PREFIX = "silver/transactions_rejected"
LAKE_GOLD_PREFIX = "gold"
LAKE_ANOMALIES_PREFIX = "gold/anomalies"
LAKE_QUALITY_PREFIX = "quality/reports"

TOPIC_TRANSACTIONS = "transactions.raw"
