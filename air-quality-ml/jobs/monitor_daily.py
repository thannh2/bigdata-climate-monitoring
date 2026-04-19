from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import functions as F

from air_quality_ml.monitoring.data_quality import duplicate_rate, ingestion_delay_stats, missing_rate, station_silence
from air_quality_ml.monitoring.performance import classification_drift_status, regression_drift_status
from air_quality_ml.settings import load_base_settings, read_yaml, resolve_path
from air_quality_ml.utils.logger import get_logger, log_event
from air_quality_ml.utils.parquet_io import read_dataset_safe, write_dataset_safe
from air_quality_ml.utils.spark import create_spark_session


def _severity(metric_value: float, warning_threshold: float, critical_threshold: float) -> str:
    if metric_value >= critical_threshold:
        return "critical"
    if metric_value >= warning_threshold:
        return "warning"
    return "stable"


def _missing_rate_rows(df, thresholds: dict[str, Any], snapshot_ts: str, metric_date: str) -> list[dict[str, Any]]:
    monitored_columns = ["station_id", "timestamp", "pm2_5", "temp_c", "humidity", "pressure", "wind_speed"]
    row = missing_rate(df, monitored_columns).collect()[0].asDict()
    warning = float(thresholds["data_quality"]["missing_rate"]["warning"])
    critical = float(thresholds["data_quality"]["missing_rate"]["critical"])

    rows = []
    for metric_name, metric_value in row.items():
        value = float(metric_value or 0.0)
        rows.append(
            {
                "metric_time": snapshot_ts,
                "metric_date": metric_date,
                "metric_category": "data_quality",
                "metric_name": metric_name,
                "metric_value": value,
                "severity": _severity(value, warning, critical),
                "model_name": None,
                "horizon": None,
                "data_window": "latest_snapshot",
            }
        )
    return rows


def _duplicate_rows(df, thresholds: dict[str, Any], snapshot_ts: str, metric_date: str) -> list[dict[str, Any]]:
    duplicate = duplicate_rate(df, ["station_id", "timestamp"]).collect()[0].asDict()
    rate_value = float(duplicate["duplicate_rate"] or 0.0)
    warning = float(thresholds["data_quality"]["duplicate_rate"]["warning"])
    critical = float(thresholds["data_quality"]["duplicate_rate"]["critical"])
    return [
        {
            "metric_time": snapshot_ts,
            "metric_date": metric_date,
            "metric_category": "data_quality",
            "metric_name": "duplicate_rate",
            "metric_value": rate_value,
            "severity": _severity(rate_value, warning, critical),
            "model_name": None,
            "horizon": None,
            "data_window": "latest_snapshot",
            "total_rows": int(duplicate["total_rows"] or 0),
            "duplicate_rows": int(duplicate["duplicate_rows"] or 0),
        }
    ]


def _delay_rows(df, thresholds: dict[str, Any], snapshot_ts: str, metric_date: str) -> list[dict[str, Any]]:
    if "ingestion_time" not in df.columns:
        return []

    delay = ingestion_delay_stats(df, event_col="timestamp", ingestion_col="ingestion_time").collect()[0].asDict()
    warning = float(thresholds["data_quality"]["ingestion_delay_p95_seconds"]["warning"])
    critical = float(thresholds["data_quality"]["ingestion_delay_p95_seconds"]["critical"])
    p95_delay = float(delay["p95_delay_seconds"] or 0.0)

    return [
        {
            "metric_time": snapshot_ts,
            "metric_date": metric_date,
            "metric_category": "data_quality",
            "metric_name": "ingestion_delay_p95_seconds",
            "metric_value": p95_delay,
            "severity": _severity(p95_delay, warning, critical),
            "model_name": None,
            "horizon": None,
            "data_window": "latest_snapshot",
            "avg_delay_seconds": float(delay["avg_delay_seconds"] or 0.0),
            "max_delay_seconds": float(delay["max_delay_seconds"] or 0.0),
        }
    ]


def _station_silence_rows(df, thresholds: dict[str, Any], snapshot_ts: str, metric_date: str) -> list[dict[str, Any]]:
    if "timestamp" not in df.columns or "station_id" not in df.columns:
        return []

    silence_df = station_silence(df, station_col="station_id", event_col="timestamp")
    row = silence_df.agg(F.max("silence_minutes").alias("max_silence_minutes")).collect()[0].asDict()
    silence_minutes = float(row["max_silence_minutes"] or 0.0)
    warning = float(thresholds["data_quality"]["station_silence_minutes"]["warning"])
    critical = float(thresholds["data_quality"]["station_silence_minutes"]["critical"])

    return [
        {
            "metric_time": snapshot_ts,
            "metric_date": metric_date,
            "metric_category": "data_quality",
            "metric_name": "station_silence_minutes",
            "metric_value": silence_minutes,
            "severity": _severity(silence_minutes, warning, critical),
            "model_name": None,
            "horizon": None,
            "data_window": "latest_snapshot",
        }
    ]


def _performance_rows(spark, eval_path: str, eval_format: str, snapshot_ts: str, metric_date: str) -> list[dict[str, Any]]:
    eval_df = read_dataset_safe(spark, eval_path, dataset_format=eval_format)
    if eval_df.rdd.isEmpty():
        return []

    pdf = eval_df.filter(F.col("split") == F.lit("test")).toPandas()
    if pdf.empty:
        return []

    rows: list[dict[str, Any]] = []
    grouped = pdf.groupby(["task", "model_name", "horizon", "run_id"])
    run_metrics = []
    for (task, model_name, horizon, run_id), group in grouped:
        metrics = {row["metric_name"]: float(row["metric_value"]) for _, row in group.iterrows()}
        recorded_at = group["recorded_at"].iloc[0]
        run_metrics.append(
            {
                "task": task,
                "model_name": model_name,
                "horizon": int(horizon),
                "run_id": run_id,
                "recorded_at": recorded_at,
                "metrics": metrics,
            }
        )

    if not run_metrics:
        return []

    run_pdf = pd.DataFrame(run_metrics).sort_values(["task", "model_name", "horizon", "recorded_at"])

    for (task, model_name, horizon), group in run_pdf.groupby(["task", "model_name", "horizon"]):
        if len(group) < 2:
            continue
        previous = group.iloc[-2]
        current = group.iloc[-1]
        if task == "regression":
            current_mae = float(current["metrics"].get("mae", 0.0))
            previous_mae = float(previous["metrics"].get("mae", 0.0))
            status = regression_drift_status(current_mae=current_mae, baseline_mae=previous_mae)
            rows.append(
                {
                    "metric_time": snapshot_ts,
                    "metric_date": metric_date,
                    "metric_category": "performance",
                    "metric_name": "mae_drift_status",
                    "metric_value": current_mae,
                    "severity": status,
                    "model_name": model_name,
                    "horizon": int(horizon),
                    "data_window": "test_vs_previous_run",
                    "baseline_metric_value": previous_mae,
                }
            )
        elif task == "classification":
            current_recall = float(current["metrics"].get("recall", 0.0))
            current_auprc = float(current["metrics"].get("auprc", 0.0))
            previous_recall = float(previous["metrics"].get("recall", 0.0))
            previous_auprc = float(previous["metrics"].get("auprc", 0.0))
            status = classification_drift_status(
                current_recall=current_recall,
                baseline_recall=previous_recall,
                current_auprc=current_auprc,
                baseline_auprc=previous_auprc,
            )
            rows.append(
                {
                    "metric_time": snapshot_ts,
                    "metric_date": metric_date,
                    "metric_category": "performance",
                    "metric_name": "classification_drift_status",
                    "metric_value": current_auprc,
                    "severity": status,
                    "model_name": model_name,
                    "horizon": int(horizon),
                    "data_window": "test_vs_previous_run",
                    "baseline_metric_value": previous_auprc,
                    "current_recall": current_recall,
                    "baseline_recall": previous_recall,
                }
            )

    return rows


def main() -> None:
    logger = get_logger("air_quality_ml.monitor_daily")
    root = Path(__file__).resolve().parents[1]
    base_cfg_path = root / "configs" / "base.yaml"
    thresholds_path = root / "configs" / "monitoring_thresholds.yaml"
    settings = load_base_settings(base_cfg_path)
    thresholds = read_yaml(thresholds_path)

    curated_path = str(resolve_path(base_cfg_path.parent, settings.data.curated_dataset_path))
    eval_path = str(resolve_path(base_cfg_path.parent, settings.data.gold_eval_path))
    monitoring_path = str(resolve_path(base_cfg_path.parent, settings.data.monitoring_path))

    snapshot_ts = datetime.now(timezone.utc).isoformat()
    metric_date = snapshot_ts[:10]

    spark = create_spark_session(settings)
    try:
        df = read_dataset_safe(spark, curated_path, dataset_format=settings.storage.curated_format)

        rows: list[dict[str, Any]] = []
        rows.extend(_missing_rate_rows(df, thresholds, snapshot_ts, metric_date))
        rows.extend(_duplicate_rows(df, thresholds, snapshot_ts, metric_date))
        rows.extend(_delay_rows(df, thresholds, snapshot_ts, metric_date))
        rows.extend(_station_silence_rows(df, thresholds, snapshot_ts, metric_date))

        if Path(eval_path).exists():
            rows.extend(_performance_rows(spark, eval_path, settings.storage.eval_format, snapshot_ts, metric_date))

        if not rows:
            log_event(logger, "monitor_daily_skipped", reason="no_metrics")
            return

        output_df = spark.createDataFrame(rows)
        write_dataset_safe(
            output_df,
            monitoring_path,
            dataset_format=settings.storage.monitoring_format,
            mode="append",
            partition_cols=["metric_date", "metric_category"],
        )

        log_event(
            logger,
            "monitor_daily_written",
            path=monitoring_path,
            output_format=settings.storage.monitoring_format,
            metrics_written=len(rows),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
