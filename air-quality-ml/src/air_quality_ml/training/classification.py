from __future__ import annotations

from typing import Any

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _set_supported_params(model: Any, params: dict[str, Any]) -> Any:
    for k, v in params.items():
        if model.hasParam(k):
            model = model.set(model.getParam(k), v)
    return model


def add_class_weight_col(df: DataFrame, label_col: str, weight_col: str = "class_weight") -> DataFrame:
    stats = df.groupBy(label_col).count().collect()
    if not stats:
        return df.withColumn(weight_col, F.lit(1.0))

    total = float(sum(r["count"] for r in stats))
    n_classes = float(len(stats))
    label_to_weight = {float(r[label_col]): total / (n_classes * float(r["count"])) for r in stats}

    expr = None
    for label_value, weight in label_to_weight.items():
        cond = F.when(F.col(label_col).cast("double") == F.lit(label_value), F.lit(float(weight)))
        expr = cond if expr is None else expr.otherwise(cond)

    if expr is None:
        return df.withColumn(weight_col, F.lit(1.0))

    return df.withColumn(weight_col, expr.otherwise(F.lit(1.0)))


def _build_classifier(
    model_type: str,
    label_col: str,
    prediction_col: str,
    probability_col: str,
    weight_col: str,
    params: dict[str, Any],
) -> Any:
    if model_type == "logistic":
        model = LogisticRegression(
            labelCol=label_col,
            featuresCol="features",
            predictionCol=prediction_col,
            probabilityCol=probability_col,
            weightCol=weight_col,
        )
    elif model_type == "rf":
        model = RandomForestClassifier(
            labelCol=label_col,
            featuresCol="features",
            predictionCol=prediction_col,
            probabilityCol=probability_col,
            weightCol=weight_col,
        )
    else:
        model = GBTClassifier(
            labelCol=label_col,
            featuresCol="features",
            predictionCol=prediction_col,
            probabilityCol=probability_col,
            weightCol=weight_col,
        )

    return _set_supported_params(model, params)


def build_classification_pipeline(
    numeric_features: list[str],
    categorical_features: list[str],
    label_col: str,
    prediction_col: str,
    probability_col: str,
    model_type: str,
    params: dict[str, Any],
    weight_col: str,
) -> tuple[Pipeline, list[str]]:
    stages = []

    imputed_numeric = [f"{c}_imputed" for c in numeric_features]
    if numeric_features:
        stages.append(Imputer(inputCols=numeric_features, outputCols=imputed_numeric, strategy="median"))

    encoded_cols: list[str] = []
    for c in categorical_features:
        idx = f"{c}_idx"
        ohe = f"{c}_ohe"
        stages.append(StringIndexer(inputCol=c, outputCol=idx, handleInvalid="keep"))
        stages.append(OneHotEncoder(inputCols=[idx], outputCols=[ohe], handleInvalid="keep"))
        encoded_cols.append(ohe)

    vector_inputs = imputed_numeric + encoded_cols
    stages.append(VectorAssembler(inputCols=vector_inputs, outputCol="features", handleInvalid="keep"))

    classifier = _build_classifier(
        model_type=model_type,
        label_col=label_col,
        prediction_col=prediction_col,
        probability_col=probability_col,
        weight_col=weight_col,
        params=params,
    )
    stages.append(classifier)

    return Pipeline(stages=stages), vector_inputs


def train_and_predict_classification(
    train_df: DataFrame,
    val_df: DataFrame,
    test_df: DataFrame,
    numeric_features: list[str],
    categorical_features: list[str],
    label_col: str,
    prediction_col: str,
    probability_col: str,
    model_type: str,
    params: dict[str, Any],
    weight_col: str = "class_weight",
) -> tuple[PipelineModel, DataFrame, DataFrame, list[dict[str, Any]]]:
    weighted_train = add_class_weight_col(train_df, label_col=label_col, weight_col=weight_col)

    pipeline, feature_names = build_classification_pipeline(
        numeric_features=numeric_features,
        categorical_features=categorical_features,
        label_col=label_col,
        prediction_col=prediction_col,
        probability_col=probability_col,
        model_type=model_type,
        params=params,
        weight_col=weight_col,
    )

    model = pipeline.fit(weighted_train)
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
