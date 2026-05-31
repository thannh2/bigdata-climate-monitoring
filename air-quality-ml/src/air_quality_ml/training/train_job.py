from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import mlflow.spark

from air_quality_ml.features.feature_catalog import get_default_feature_columns
from air_quality_ml.features.l4_targets import target_metadata
from air_quality_ml.mlflow_tracking.artifacts import log_feature_importance, log_json_artifact
from air_quality_ml.mlflow_tracking.registry import try_register_logged_model
from air_quality_ml.mlflow_tracking.signatures import infer_model_signature, log_input_example
from air_quality_ml.mlflow_tracking.tracking import configure_mlflow, set_experiment, start_training_run
from air_quality_ml.settings import load_base_settings, load_job_config, resolve_path
from air_quality_ml.training.dataset_loader import load_training_table, prepare_training_frame
from air_quality_ml.training.evaluate_classification import evaluate_classification
from air_quality_ml.training.evaluate_regression import evaluate_regression
from air_quality_ml.training.model_selection import (
    candidates_from_config,
    select_best_classification,
    select_best_regression,
)
from air_quality_ml.training.registry import should_promote_classification, should_promote_regression
from air_quality_ml.training.splitters import time_based_split
from air_quality_ml.training.thresholding import apply_threshold, tune_binary_threshold, with_probability_score
from air_quality_ml.utils.logger import get_logger, log_event
from air_quality_ml.utils.parquet_io import get_dataset_version, write_dataset_safe
from air_quality_ml.utils.spark import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train MLlib model for air-quality forecasting/alerting")
    parser.add_argument("--base-config", required=True, help="Path to base.yaml")
    parser.add_argument("--job-config", required=True, help="Path to training config file")
    parser.add_argument(
        "--data-path",
        required=False,
        default=None,
        help="Optional override for training table path. Default uses base config curated_dataset_path.",
    )
    return parser.parse_args()


def _count_rows(df) -> int:
    return int(df.count())


def _to_run_name(job_name: str, horizon: int) -> str:
    return f"{job_name}-h{horizon}"


def _log_dataset_stats(train_df, val_df, test_df) -> None:
    mlflow.log_metric("rows_train", float(_count_rows(train_df)))
    mlflow.log_metric("rows_val", float(_count_rows(val_df)))
    mlflow.log_metric("rows_test", float(_count_rows(test_df)))


def _prepare_training_data(args: argparse.Namespace):
    base_config_path = Path(args.base_config).resolve()
    job_config_path = Path(args.job_config).resolve()

    base_settings = load_base_settings(base_config_path)
    job_settings = load_job_config(job_config_path)

    data_path_cfg = args.data_path or base_settings.data.curated_dataset_path
    data_path = str(resolve_path(base_config_path.parent, data_path_cfg))

    return base_settings, job_settings, data_path


def _job_target_metadata(job_settings) -> dict[str, str]:
    metadata = target_metadata(job_settings.target_col, fallback_task=job_settings.task)
    if job_settings.target_name:
        metadata["target_name"] = job_settings.target_name
    if job_settings.prediction_unit:
        metadata["prediction_unit"] = job_settings.prediction_unit
    if job_settings.output_kind:
        metadata["target_family"] = job_settings.output_kind
    return metadata


def _common_params(
    base_settings,
    job_settings,
    numeric_features,
    categorical_features,
    data_path: str,
    data_format: str,
    dataset_version: str | None,
) -> dict[str, str]:
    metadata = _job_target_metadata(job_settings)
    params: dict[str, str] = {
        "task": job_settings.task,
        "horizon": str(job_settings.horizon),
        "target_col": job_settings.target_col,
        "target_name": metadata["target_name"],
        "prediction_unit": metadata["prediction_unit"],
        "output_kind": metadata["target_family"],
        "model_type": job_settings.model_type,
        "model_name": job_settings.model_name,
        "data_path": data_path,
        "data_format": data_format,
        "dataset_version": dataset_version or "unknown",
        "feature_count_numeric": str(len(numeric_features)),
        "feature_count_categorical": str(len(categorical_features)),
        "environment": base_settings.environment,
        "owner": base_settings.owner,
    }
    for k, v in job_settings.params.items():
        params[f"model_param_{k}"] = str(v)
    return params


def _write_eval_snapshot(
    spark,
    base_settings,
    job_settings,
    val_metrics: dict[str, float],
    test_metrics: dict[str, float],
    run_id: str,
    data_path: str,
    data_format: str,
    dataset_version: str | None,
    eval_path: str,
) -> None:
<<<<<<< Updated upstream
    rows: list[dict[str, str | float | int | None]] = []
    recorded_at = datetime.now(timezone.utc).isoformat()
=======
    schema = StructType(
        [
            StructField("recorded_at", StringType(), nullable=False),
            StructField("run_id", StringType(), nullable=False),
            StructField("task", StringType(), nullable=False),
            StructField("model_name", StringType(), nullable=False),
            StructField("target_name", StringType(), nullable=False),
            StructField("target_col", StringType(), nullable=False),
            StructField("prediction_unit", StringType(), nullable=False),
            StructField("horizon", IntegerType(), nullable=False),
            StructField("split", StringType(), nullable=False),
            StructField("metric_name", StringType(), nullable=False),
            StructField("metric_value", DoubleType(), nullable=False),
            StructField("data_path", StringType(), nullable=False),
            StructField("data_format", StringType(), nullable=False),
            StructField("dataset_version", StringType(), nullable=False),
        ]
    )
    rows: list[dict[str, str | float | int | None]] = []
    recorded_at = datetime.now(timezone.utc).isoformat()
    dataset_version_value = dataset_version or "unknown"
    metadata = _job_target_metadata(job_settings)
>>>>>>> Stashed changes
    for split_name, metrics in [("val", val_metrics), ("test", test_metrics)]:
        for metric_name, metric_value in metrics.items():
            rows.append(
                {
                    "recorded_at": recorded_at,
                    "run_id": run_id,
                    "task": job_settings.task,
                    "model_name": job_settings.model_name,
                    "target_name": metadata["target_name"],
                    "target_col": metadata["target_col"],
                    "prediction_unit": metadata["prediction_unit"],
                    "horizon": int(job_settings.horizon),
                    "split": split_name,
                    "metric_name": metric_name,
                    "metric_value": float(metric_value),
                    "data_path": data_path,
                    "data_format": data_format,
                    "dataset_version": dataset_version,
                }
            )

    if not rows:
        return

    eval_df = spark.createDataFrame(rows)
    write_dataset_safe(
        eval_df,
        eval_path,
        dataset_format=base_settings.storage.eval_format,
        mode="append",
        partition_cols=["task", "model_name", "horizon", "split"],
    )


def _log_model(model, signature_df, prediction_col: str) -> None:
    signature = infer_model_signature(signature_df, prediction_col=prediction_col)
    input_example = log_input_example(signature_df, prediction_col=prediction_col)
    mlflow.spark.log_model(
        spark_model=model,
        artifact_path="model",
        signature=signature,
        input_example=input_example,
    )


def _log_sample_predictions(df, columns: list[str]) -> None:
    present = [c for c in columns if c in df.columns]
    sample = df.select(*present).limit(200).toPandas().to_dict(orient="records")
    log_json_artifact("sample_predictions.json", sample, artifact_path="analysis")


def _run_regression(job_settings, base_settings, train_df, val_df, test_df, numeric_features, categorical_features):
    search_cfg = job_settings.model_selection or {}
    selection_metric = str(search_cfg.get("metric", "mae"))
    candidates = candidates_from_config("regression", search_cfg)

    def _on_candidate(candidate, val_metrics):
        mlflow.log_metric(f"cand_{candidate.label}_val_{selection_metric}", float(val_metrics.get(selection_metric, 0.0)))

    selection = select_best_regression(
        train_df=train_df,
        val_df=val_df,
        test_df=test_df,
        numeric_features=numeric_features,
        categorical_features=categorical_features,
        label_col=job_settings.target_col,
        prediction_col=job_settings.prediction_col,
        candidates=candidates,
        high_pollution_threshold=base_settings.features.high_pollution_threshold,
        selection_metric=selection_metric,
        on_candidate=_on_candidate,
    )
    best = selection.best

    mlflow.set_tag("selected_model_type", best.candidate.model_type)
    mlflow.log_param("selected_model_type", best.candidate.model_type)
    mlflow.log_param("selected_model_params", str(best.candidate.params))
    mlflow.log_param("n_candidates", str(len(selection.candidates)))
    log_json_artifact("model_selection.json", selection.report, artifact_path="analysis")

    val_metrics = best.val_metrics
    test_metrics = evaluate_regression(
        best.test_pred,
        label_col=job_settings.target_col,
        prediction_col=job_settings.prediction_col,
        high_pollution_threshold=base_settings.features.high_pollution_threshold,
    )
    for k, v in val_metrics.items():
        mlflow.log_metric(f"val_{k}", float(v))
    for k, v in test_metrics.items():
        mlflow.log_metric(f"test_{k}", float(v))
    if selection.baseline_metrics:
        for k, v in selection.baseline_metrics.items():
            mlflow.log_metric(f"baseline_val_{k}", float(v))

    if best.feature_importance:
        log_feature_importance(best.feature_importance)

    promote = should_promote_regression(val_metrics, base_settings, baseline_metrics=selection.baseline_metrics)
    mlflow.log_metric("promote_candidate", float(1 if promote else 0))

    signature_df = best.test_pred.select(
        *[c for c in (numeric_features + categorical_features + [job_settings.prediction_col]) if c in best.test_pred.columns]
    )
    _log_model(best.model, signature_df, job_settings.prediction_col)
    _log_sample_predictions(
        best.test_pred,
        ["station_id", "timestamp", job_settings.target_col, job_settings.prediction_col],
    )

    return val_metrics, test_metrics, promote


def _run_classification(job_settings, base_settings, train_df, val_df, test_df, numeric_features, categorical_features):
    probability_col = job_settings.probability_col or "probability"
    search_cfg = job_settings.model_selection or {}
    selection_metric = str(search_cfg.get("metric", "auprc"))
    candidates = candidates_from_config("classification", search_cfg)

    def _on_candidate(candidate, val_metrics):
        mlflow.log_metric(f"cand_{candidate.label}_val_{selection_metric}", float(val_metrics.get(selection_metric, 0.0)))

    selection = select_best_classification(
        train_df=train_df,
        val_df=val_df,
        test_df=test_df,
        numeric_features=numeric_features,
        categorical_features=categorical_features,
        label_col=job_settings.target_col,
        prediction_col=job_settings.prediction_col,
        probability_col=probability_col,
        candidates=candidates,
        selection_metric=selection_metric,
        on_candidate=_on_candidate,
    )
    best = selection.best

    mlflow.set_tag("selected_model_type", best.candidate.model_type)
    mlflow.log_param("selected_model_type", best.candidate.model_type)
    mlflow.log_param("selected_model_params", str(best.candidate.params))
    mlflow.log_param("n_candidates", str(len(selection.candidates)))
    log_json_artifact("model_selection.json", selection.report, artifact_path="analysis")

    val_scored = with_probability_score(best.val_pred, probability_col=probability_col, score_col="pred_prob")
    test_scored = with_probability_score(best.test_pred, probability_col=probability_col, score_col="pred_prob")

    threshold_cfg = job_settings.threshold_tuning or {}
    default_threshold = float(threshold_cfg.get("default_threshold", 0.50))
    if bool(threshold_cfg.get("enabled", True)):
        tuned = tune_binary_threshold(
            val_scored,
            label_col=job_settings.target_col,
            score_col="pred_prob",
            min_precision=float(threshold_cfg.get("min_precision", 0.35)),
            min_recall=float(threshold_cfg.get("min_recall", 0.80)),
            default_threshold=default_threshold,
        )
        decision_threshold = float(tuned["threshold"])
        mlflow.log_metric("threshold_tune_precision", float(tuned["precision"]))
        mlflow.log_metric("threshold_tune_recall", float(tuned["recall"]))
        mlflow.log_metric("threshold_tune_f1", float(tuned["f1"]))
    else:
        decision_threshold = default_threshold

    mlflow.log_param("decision_threshold", str(decision_threshold))

    val_eval_df = apply_threshold(val_scored, threshold=decision_threshold, score_col="pred_prob", pred_col=job_settings.prediction_col)
    test_eval_df = apply_threshold(test_scored, threshold=decision_threshold, score_col="pred_prob", pred_col=job_settings.prediction_col)

    val_metrics = evaluate_classification(
        val_eval_df,
        label_col=job_settings.target_col,
        prediction_col=job_settings.prediction_col,
        raw_prediction_col="rawPrediction",
    )
    test_metrics = evaluate_classification(
        test_eval_df,
        label_col=job_settings.target_col,
        prediction_col=job_settings.prediction_col,
        raw_prediction_col="rawPrediction",
    )
    for k, v in val_metrics.items():
        mlflow.log_metric(f"val_{k}", float(v))
    for k, v in test_metrics.items():
        mlflow.log_metric(f"test_{k}", float(v))
    if selection.baseline_metrics:
        for k, v in selection.baseline_metrics.items():
            mlflow.log_metric(f"baseline_val_{k}", float(v))

    if best.feature_importance:
        log_feature_importance(best.feature_importance)

    promote = should_promote_classification(val_metrics, base_settings, baseline_metrics=selection.baseline_metrics)
    mlflow.log_metric("promote_candidate", float(1 if promote else 0))

    signature_df = test_eval_df.select(
        *[c for c in (numeric_features + categorical_features + ["pred_prob", job_settings.prediction_col]) if c in test_eval_df.columns]
    )
    _log_model(best.model, signature_df, job_settings.prediction_col)
    _log_sample_predictions(
        test_eval_df,
        ["station_id", "timestamp", job_settings.target_col, "pred_prob", job_settings.prediction_col],
    )

    return val_metrics, test_metrics, promote


def main() -> None:
    args = parse_args()
    logger = get_logger("air_quality_ml.train_job")

    base_settings, job_settings, data_path = _prepare_training_data(args)
    base_config_path = Path(args.base_config).resolve()
    eval_path = str(resolve_path(base_config_path.parent, base_settings.data.gold_eval_path))

    spark = create_spark_session(base_settings)
    try:
        log_event(logger, "training_start", job_config=args.job_config, data_path=data_path)
        data_format = base_settings.storage.curated_format
        dataset_version = get_dataset_version(spark, data_path, data_format)

        raw_df = load_training_table(
            spark,
            path=data_path,
            dataset_format=data_format,
            max_rows=job_settings.training.get("max_rows"),
        )
        dataset = prepare_training_frame(
            raw_df,
            target_col=job_settings.target_col,
            dropna_label=bool(job_settings.training.get("dropna_label", True)),
        )

        numeric_features, categorical_features = get_default_feature_columns(dataset, target_col=job_settings.target_col)
        if not numeric_features and not categorical_features:
            raise ValueError("No usable feature columns found for training")

        train_df, val_df, test_df = time_based_split(
            dataset,
            timestamp_col=base_settings.split.timestamp_col,
            train_end=base_settings.split.train_end,
            val_end=base_settings.split.val_end,
        )
        train_df = train_df.cache()
        val_df = val_df.cache()
        test_df = test_df.cache()

        if _count_rows(train_df) == 0 or _count_rows(val_df) == 0 or _count_rows(test_df) == 0:
            raise ValueError("Train/validation/test split produced an empty partition")

        configure_mlflow(base_settings)
        experiment_name = set_experiment(job_settings, base_settings)

        tags = {
            "pipeline_version": "v0.2",
            "environment": base_settings.environment,
            "owner": base_settings.owner,
            "task": job_settings.task,
            "horizon": str(job_settings.horizon),
            **_job_target_metadata(job_settings),
            "data_path": data_path,
            "data_format": data_format,
            "dataset_version": dataset_version or "unknown",
        }

        with start_training_run(run_name=_to_run_name(job_settings.model_name, job_settings.horizon), tags=tags) as run:
            mlflow.log_params(
                _common_params(
                    base_settings,
                    job_settings,
                    numeric_features,
                    categorical_features,
                    data_path,
                    data_format,
                    dataset_version,
                )
            )
            if dataset_version is not None:
                mlflow.log_param("dataset_version", dataset_version)
            _log_dataset_stats(train_df, val_df, test_df)
            log_json_artifact(
                "selected_features.json",
                {
                    "numeric_features": numeric_features,
                    "categorical_features": categorical_features,
                    "target_col": job_settings.target_col,
                },
                artifact_path="metadata",
            )
            mlflow.set_tag("experiment_name", experiment_name)

            if job_settings.task == "regression":
                val_metrics, test_metrics, _ = _run_regression(
                    job_settings, base_settings, train_df, val_df, test_df, numeric_features, categorical_features
                )
            elif job_settings.task == "classification":
                val_metrics, test_metrics, _ = _run_classification(
                    job_settings, base_settings, train_df, val_df, test_df, numeric_features, categorical_features
                )
            else:
                raise ValueError(f"Unsupported task type: {job_settings.task}")

            model_version = None
            if bool(job_settings.register_if_pass) and bool(mlflow.active_run()):
                model_version = try_register_logged_model(job_settings.model_name, run_id=run.info.run_id, artifact_path="model")
                if model_version:
                    mlflow.set_tag("model_version", model_version)

            _write_eval_snapshot(
                spark=spark,
                base_settings=base_settings,
                job_settings=job_settings,
                val_metrics=val_metrics,
                test_metrics=test_metrics,
                run_id=run.info.run_id,
                data_path=data_path,
                data_format=data_format,
                dataset_version=dataset_version,
                eval_path=eval_path,
            )

            mlflow.set_tag("registered_model_name", job_settings.model_name)
            mlflow.set_tag("registered_model_version", model_version or "not_registered")

            log_event(
                logger,
                "training_complete",
                run_id=run.info.run_id,
                model_name=job_settings.model_name,
                model_version=model_version,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
