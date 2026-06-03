from __future__ import annotations

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def evaluate_regression(
    df: DataFrame,
    label_col: str,
    prediction_col: str,
    high_pollution_threshold: float = 75.0,
) -> dict[str, float]:
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol=prediction_col)

    rmse = float(evaluator.evaluate(df, {evaluator.metricName: "rmse"}))
    mae = float(evaluator.evaluate(df, {evaluator.metricName: "mae"}))
    r2 = float(evaluator.evaluate(df, {evaluator.metricName: "r2"}))

    safe_df = df.filter(F.col(label_col).isNotNull() & F.col(prediction_col).isNotNull())
    mape_row = safe_df.filter(F.abs(F.col(label_col)) > F.lit(1e-9)).select(
        F.avg(F.abs((F.col(prediction_col) - F.col(label_col)) / F.col(label_col))).alias("mape")
    ).collect()[0]
    mape = float(mape_row["mape"] or 0.0)

    high_df = safe_df.filter(F.col(label_col) >= F.lit(high_pollution_threshold))
    if high_df.take(1):
        mae_high = float(
            RegressionEvaluator(labelCol=label_col, predictionCol=prediction_col, metricName="mae").evaluate(high_df)
        )
    else:
        mae_high = 0.0

    return {
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "mape": mape,
        "mae_high_pollution": mae_high,
    }
