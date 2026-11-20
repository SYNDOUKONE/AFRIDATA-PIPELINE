"""Fonctions de features et de détection d'anomalies en pandas (testables hors Spark)."""

import numpy as np
import pandas as pd

ZSCORE_THRESHOLD = 6.0


def amount_zscore_features(df: pd.DataFrame) -> pd.DataFrame:
    """Z-score des montants et références attendues par (operator, tx_type)."""
    stats = (
        df.groupby(["operator", "tx_type"])["amount"]
        .agg(["mean", "std", "median"])
        .reset_index()
        .rename(columns={"mean": "exp_amount", "std": "std_amount", "median": "median_amount"})
    )
    out = df.merge(stats, on=["operator", "tx_type"], how="left")
    out["amount_zscore"] = (out["amount"] - out["exp_amount"]) / out["std_amount"].replace(0, np.nan)
    out["amount_zscore"] = out["amount_zscore"].fillna(0.0)
    return out


def flag_frequency_outliers(df: pd.DataFrame, threshold_percentile: float = 99.0) -> pd.DataFrame:
    """Marque les expéditeurs dont la fréquence quotidienne dépasse un percentile global."""
    daily = df.groupby(["sender_id", "tx_date"]).size().reset_index(name="daily_count")
    cutoff = float(np.percentile(daily["daily_count"], threshold_percentile)) if len(daily) else 0.0
    outliers = daily[daily["daily_count"] > cutoff][["sender_id", "tx_date"]]
    out = df.merge(outliers.assign(is_frequency_anomaly=True), on=["sender_id", "tx_date"], how="left")
    out["is_frequency_anomaly"] = out["is_frequency_anomaly"].fillna(False).astype(bool)
    return out


def repeated_pair_counts(df: pd.DataFrame, max_within_hour: int = 5) -> pd.DataFrame:
    """Couples expéditeur/destinataire répétés au-delà d'un seuil dans la même heure."""
    return (
        df.groupby(["sender_id", "receiver_id", "tx_date", "tx_hour"])
        .size()
        .reset_index(name="pair_count")
        .query("pair_count > @max_within_hour")
    )


def flag_repeated_pairs(df: pd.DataFrame, max_within_hour: int = 5) -> pd.DataFrame:
    pairs = repeated_pair_counts(df, max_within_hour)
    out = df.merge(
        pairs.assign(is_repeated_pair=True),
        on=["sender_id", "receiver_id", "tx_date", "tx_hour"],
        how="left",
    )
    out["is_repeated_pair"] = out["is_repeated_pair"].fillna(False).astype(bool)
    return out


def build_anomaly_features(df: pd.DataFrame) -> pd.DataFrame:
    """Construit les features par transaction pour la détection d'anomalies."""
    features = amount_zscore_features(df)
    daily = (
        df.groupby(["sender_id", "tx_date"])["amount"]
        .agg(["count", "sum"])
        .reset_index()
        .rename(columns={"count": "sender_daily_tx", "sum": "sender_daily_amount"})
    )
    features = features.merge(daily, on=["sender_id", "tx_date"], how="left")
    features["sender_daily_tx"] = features["sender_daily_tx"].fillna(0).astype(int)
    features["sender_daily_amount"] = features["sender_daily_amount"].fillna(0.0)
    return features


def rule_based_flags(features: pd.DataFrame) -> pd.DataFrame:
    """Applique les règles métier : montant extrême, z-score, fréquence, paires répétées."""
    out = flag_repeated_pairs(flag_frequency_outliers(features))
    out["is_high_amount"] = out["amount_zscore"] > ZSCORE_THRESHOLD
    reasons = []
    for row in out.itertuples(index=False):
        row_reasons = []
        if row.is_high_amount:
            row_reasons.append("extreme_amount")
        if row.is_frequency_anomaly:
            row_reasons.append("abnormal_frequency")
        if row.is_repeated_pair:
            row_reasons.append("repeated_pair")
        reasons.append("|".join(row_reasons))
    out["anomaly_rules"] = reasons
    return out


def isolation_forest_scores(features: pd.DataFrame, columns=None, contamination: float = 0.01, seed: int = 42):
    """Scores Isolation Forest (labels -1 = anomalie) sur les features numériques."""
    from sklearn.ensemble import IsolationForest

    if columns is None:
        columns = ["amount", "amount_zscore", "sender_daily_tx", "sender_daily_amount", "tx_hour"]
    matrix = features[columns].to_numpy(dtype="float64")
    model = IsolationForest(n_estimators=100, contamination=contamination, random_state=seed)
    labels = model.fit_predict(matrix)
    scores = model.score_samples(matrix)
    return labels, scores
