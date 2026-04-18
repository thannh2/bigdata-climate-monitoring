from __future__ import annotations

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _safe_div(num: float, den: float) -> float:
    return float(num / den) if den else 0.0


def evaluate_classification(
    df: DataFrame,
    label_col: str,
    prediction_col: str,
    raw_prediction_col: str = "rawPrediction",
) -> dict[str, float]:
    auc_roc = 0.0
    auprc = 0.0
    try:
        evaluator_roc = BinaryClassificationEvaluator(
            labelCol=label_col,
            rawPredictionCol=raw_prediction_col,
            metricName="areaUnderROC",
        )
        evaluator_pr = BinaryClassificationEvaluator(
            labelCol=label_col,
            rawPredictionCol=raw_prediction_col,
            metricName="areaUnderPR",
        )
        auc_roc = float(evaluator_roc.evaluate(df))
        auprc = float(evaluator_pr.evaluate(df))
    except Exception:
        pass

    cm = (
        df.select(
            F.sum(F.when((F.col(prediction_col) == 1) & (F.col(label_col) == 1), 1).otherwise(0)).alias("tp"),
            F.sum(F.when((F.col(prediction_col) == 1) & (F.col(label_col) == 0), 1).otherwise(0)).alias("fp"),
            F.sum(F.when((F.col(prediction_col) == 0) & (F.col(label_col) == 1), 1).otherwise(0)).alias("fn"),
            F.sum(F.when((F.col(prediction_col) == 0) & (F.col(label_col) == 0), 1).otherwise(0)).alias("tn"),
        )
        .collect()[0]
    )

    tp = float(cm["tp"] or 0.0)
    fp = float(cm["fp"] or 0.0)
    fn = float(cm["fn"] or 0.0)
    tn = float(cm["tn"] or 0.0)

    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f1 = _safe_div(2.0 * precision * recall, precision + recall)
    fnr = _safe_div(fn, fn + tp)

    return {
        "auc_roc": auc_roc,
        "auprc": auprc,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_negative_rate": fnr,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
    }
