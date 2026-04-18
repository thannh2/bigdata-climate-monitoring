from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_probability_score(df: DataFrame, probability_col: str = "probability", score_col: str = "pred_prob") -> DataFrame:
    return df.withColumn(score_col, F.col(probability_col).getItem(1).cast("double"))


def tune_binary_threshold(
    df: DataFrame,
    label_col: str,
    score_col: str = "pred_prob",
    min_precision: float = 0.35,
    min_recall: float = 0.80,
    default_threshold: float = 0.50,
    steps: int = 101,
) -> dict[str, float]:
    pdf = df.select(label_col, score_col).dropna().toPandas()
    if pdf.empty:
        return {
            "threshold": float(default_threshold),
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0,
        }

    best = {
        "threshold": float(default_threshold),
        "precision": 0.0,
        "recall": 0.0,
        "f1": 0.0,
    }

    y_true = pdf[label_col].astype(int).to_numpy()
    y_score = pdf[score_col].astype(float).to_numpy()

    for threshold in np.linspace(0.0, 1.0, int(steps)):
        y_pred = (y_score >= threshold).astype(int)

        tp = float(np.sum((y_pred == 1) & (y_true == 1)))
        fp = float(np.sum((y_pred == 1) & (y_true == 0)))
        fn = float(np.sum((y_pred == 0) & (y_true == 1)))

        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) else 0.0

        if precision >= min_precision and recall >= min_recall and f1 >= best["f1"]:
            best = {
                "threshold": float(threshold),
                "precision": float(precision),
                "recall": float(recall),
                "f1": float(f1),
            }

    if best["f1"] == 0.0:
        return {
            "threshold": float(default_threshold),
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0,
        }

    return best


def apply_threshold(df: DataFrame, threshold: float, score_col: str = "pred_prob", pred_col: str = "prediction") -> DataFrame:
    return df.withColumn(pred_col, F.when(F.col(score_col) >= F.lit(float(threshold)), F.lit(1)).otherwise(F.lit(0)))
