from __future__ import annotations

import mlflow
from mlflow.models import infer_signature
import pandas as pd
from pyspark.sql import DataFrame


def infer_model_signature(df: DataFrame, prediction_col: str = "prediction"):
    input_pdf = df.limit(200).drop(prediction_col).toPandas()
    output_pdf = df.limit(200).select(prediction_col).toPandas()
    return infer_signature(input_pdf, output_pdf)


def log_input_example(df: DataFrame, prediction_col: str = "prediction") -> pd.DataFrame:
    return df.limit(1).drop(prediction_col).toPandas()
