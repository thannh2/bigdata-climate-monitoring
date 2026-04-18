from __future__ import annotations

from typing import Any

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame


def _set_supported_params(model: Any, params: dict[str, Any]) -> Any:
    for k, v in params.items():
        if model.hasParam(k):
            model = model.set(model.getParam(k), v)
    return model


def _build_regressor(model_type: str, label_col: str, prediction_col: str, params: dict[str, Any]) -> Any:
    if model_type == "linear":
        model = LinearRegression(labelCol=label_col, featuresCol="features", predictionCol=prediction_col)
    elif model_type == "rf":
        model = RandomForestRegressor(labelCol=label_col, featuresCol="features", predictionCol=prediction_col)
    else:
        model = GBTRegressor(labelCol=label_col, featuresCol="features", predictionCol=prediction_col)

    return _set_supported_params(model, params)


def build_regression_pipeline(
    numeric_features: list[str],
    categorical_features: list[str],
    label_col: str,
    prediction_col: str,
    model_type: str,
    params: dict[str, Any],
) -> tuple[Pipeline, list[str]]:
    stages = []

    imputed_numeric = [f"{c}_imputed" for c in numeric_features]
    if numeric_features:
        stages.append(Imputer(inputCols=numeric_features, outputCols=imputed_numeric, strategy="median"))

    indexed_cols: list[str] = []
    encoded_cols: list[str] = []
    for c in categorical_features:
        idx = f"{c}_idx"
        ohe = f"{c}_ohe"
        stages.append(StringIndexer(inputCol=c, outputCol=idx, handleInvalid="keep"))
        stages.append(OneHotEncoder(inputCols=[idx], outputCols=[ohe], handleInvalid="keep"))
        indexed_cols.append(idx)
        encoded_cols.append(ohe)

    vector_inputs = imputed_numeric + encoded_cols
    assembler = VectorAssembler(inputCols=vector_inputs, outputCol="features", handleInvalid="keep")
    stages.append(assembler)

    regressor = _build_regressor(model_type=model_type, label_col=label_col, prediction_col=prediction_col, params=params)
    stages.append(regressor)

    return Pipeline(stages=stages), vector_inputs


def train_and_predict_regression(
    train_df: DataFrame,
    val_df: DataFrame,
    test_df: DataFrame,
    numeric_features: list[str],
    categorical_features: list[str],
    label_col: str,
    prediction_col: str,
    model_type: str,
    params: dict[str, Any],
) -> tuple[PipelineModel, DataFrame, DataFrame, list[dict[str, Any]]]:
    pipeline, feature_names = build_regression_pipeline(
        numeric_features=numeric_features,
        categorical_features=categorical_features,
        label_col=label_col,
        prediction_col=prediction_col,
        model_type=model_type,
        params=params,
    )

    model = pipeline.fit(train_df)
    val_pred = model.transform(val_df)
    test_pred = model.transform(test_df)

    feature_importance: list[dict[str, Any]] = []
    estimator_model = model.stages[-1]
    if hasattr(estimator_model, "featureImportances"):
        importances = list(estimator_model.featureImportances)
        feature_importance = [
            {
                "feature": feature_names[i] if i < len(feature_names) else f"feature_{i}",
                "importance": float(score),
            }
            for i, score in enumerate(importances)
        ]
        feature_importance = sorted(feature_importance, key=lambda x: x["importance"], reverse=True)

    return model, val_pred, test_pred, feature_importance
