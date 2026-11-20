"""Tests des features et règles de détection d'anomalies (pandas)."""

from datetime import datetime

import pandas as pd
import pytest

from spark.anomaly_features import (
    amount_zscore_features,
    build_anomaly_features,
    repeated_pair_counts,
    rule_based_flags,
    flag_repeated_pairs,
    isolation_forest_scores,
)


@pytest.fixture
def base_df():
    rows = []
    for i in range(200):
        rows.append(
            {
                "transaction_id": f"tx-{i:04d}",
                "tx_ts": datetime(2025, 1, 1, 9, 0, 0),
                "tx_date": datetime(2025, 1, 1).date(),
                "tx_hour": 9,
                "operator": "Wave",
                "tx_type": "TRANSFER",
                "channel": "USSD",
                "sender_id": f"+22507010{i:04d}",
                "receiver_id": f"+22505010{i:04d}",
                "sender_region": "Abidjan",
                "amount": 1_000.0,
                "status": "SUCCESS",
            }
        )
    return pd.DataFrame(rows)


def test_amount_zscore_detects_extreme(base_df):
    base_df.loc[0, "amount"] = 10_000_000.0
    features = amount_zscore_features(base_df)
    assert features.loc[0, "amount_zscore"] > 6.0
    assert abs(features.loc[1, "amount_zscore"]) < 0.2


def test_repeated_pairs_detected(base_df):
    base_df.loc[0:5, "sender_id"] = "+2250701888888"
    base_df.loc[0:5, "receiver_id"] = "+2250777777777"
    flagged = flag_repeated_pairs(base_df)
    pairs = repeated_pair_counts(base_df)
    assert len(pairs) == 1
    assert pairs.iloc[0]["pair_count"] == 6
    assert int(flagged["is_repeated_pair"].sum()) == 6


def test_rule_based_flags(base_df):
    base_df.loc[0, "amount"] = 10_000_000.0
    base_df.loc[1:10, "sender_id"] = "+2250701999999"
    base_df.loc[1:10, "receiver_id"] = "+2250777777777"

    features = build_anomaly_features(base_df)
    flagged = rule_based_flags(features)

    assert bool(flagged.loc[0, "is_high_amount"]) is True
    assert "extreme_amount" in flagged.loc[0, "anomaly_rules"]
    assert "abnormal_frequency" in flagged.loc[1, "anomaly_rules"]


def test_isolation_forest_runs(base_df):
    features = build_anomaly_features(base_df)
    labels, scores = isolation_forest_scores(features, contamination=0.05)
    assert len(labels) == len(features)
    assert set(labels).issubset({-1, 1})
    assert len(scores) == len(labels)
