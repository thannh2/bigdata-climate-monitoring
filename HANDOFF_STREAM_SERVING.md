# Handoff: Air Quality ML Pipeline, Stream Serving, Dashboard

## Current Context

Workspace:

```text
C:\Users\Admin\bigdata-climate-monitoring
```

Main ML app:

```text
air-quality-ml
```

Ingestion app:

```text
Ingestion
```

Kafka stack is managed by:

```powershell
docker compose -f Ingestion\docker-compose.yml up -d
```

ML/MLflow stack is managed by:

```powershell
docker compose -f docker-compose.ml.yml ...
```

MLflow UI:

```text
http://localhost:5001
```

Kafka UI / AKHQ:

```text
http://localhost:8080
```

## What Already Works

### PM2.5 h1 training

This runs successfully:

```powershell
docker compose -f docker-compose.ml.yml run --rm air-quality-ml python jobs/train_pm25_h1.py
```

It registered model versions under:

```text
aq_pm25_forecast_h1
```

### Alert h1 training

This now runs successfully after fixes:

```powershell
docker compose -f docker-compose.ml.yml run --rm air-quality-ml python jobs/train_alert_h1.py
```

It registered:

```text
aq_alert_classifier_h1
```

### Monitoring

This now runs successfully:

```powershell
docker compose -f docker-compose.ml.yml run --rm air-quality-ml python jobs/monitor_daily.py
```

Last known successful log:

```text
{"event": "monitor_daily_written", "path": "/workspace/Data/gold/monitoring", "output_format": "parquet", "metrics_written": 11}
```

### Stream prediction

Implemented and tested once successfully.

One-shot test command:

```powershell
docker compose -f docker-compose.ml.yml run --rm `
  -e STREAM_ONCE=1 `
  -e STARTING_OFFSETS=earliest `
  -e MODEL_URI=models:/aq_pm25_forecast_h1/latest `
  -e HORIZON=1 `
  air-quality-ml python jobs/stream_score_latest.py
```

Successful log:

```text
{"event": "stream_score_written_batch", "batch_id": 0, "output_path": "/workspace/Data/gold/predictions_stream", "rows": 1}
```

Output path:

```text
Data/gold/predictions_stream
```

Continuous realtime command:

```powershell
docker compose -f docker-compose.ml.yml run --rm `
  -e MODEL_URI=models:/aq_pm25_forecast_h1/latest `
  -e HORIZON=1 `
  air-quality-ml python jobs/stream_score_latest.py
```

This is expected to keep running. Stop with `Ctrl+C`.

## Important Behavior Notes

### Collector timing

Ingestion collectors default to:

```text
--poll-seconds 300
```

That means they poll every 5 minutes.

However, the upstream API may only change `event_time` hourly. So after 5 minutes, the message may look unchanged or be skipped as duplicate.

For one-hour polling:

```powershell
python Ingestion\collectors\weather_stream_collector.py --locations Hanoi --poll-seconds 3600
python Ingestion\collectors\air_stream_collector.py --locations Hanoi --poll-seconds 3600
```

### DLQ duplicate records

Records in:

```text
weather.raw.dlq
air_quality.raw.dlq
```

with:

```text
error_type = duplicate_record
```

mean the collector saw the same city/event_time again. This is not a severe failure. It is noisy because duplicate records are being sent to DLQ.

Recommended improvement: change duplicate handling to log/skip only, not send to DLQ.

## Files Changed Recently

### MLflow / Docker / Compose

```text
docker-compose.ml.yml
air-quality-ml/Dockerfile
air-quality-ml/src/air_quality_ml/mlflow_tracking/tracking.py
air-quality-ml/src/air_quality_ml/mlflow_tracking/signatures.py
```

Key changes:

- Added MLflow service earlier.
- Changed tracking to respect `MLFLOW_TRACKING_URI`.
- Changed artifact root away from `mlflow-artifacts:/...`.
- Added Docker packages `git` and `procps`.

### Training fixes

```text
air-quality-ml/src/air_quality_ml/training/train_job.py
air-quality-ml/src/air_quality_ml/training/classification.py
air-quality-ml/src/air_quality_ml/training/thresholding.py
```

Key fixes:

- Explicit schema for eval snapshot.
- Fixed class weight `when/otherwise` chain.
- Fixed `GBTClassifier` parameter handling.
- Converted Spark ML probability vector using `vector_to_array`.

### Import path fixes

Many imports were changed from:

```python
from src.air_quality_ml...
```

to:

```python
from air_quality_ml...
```

Reason: Docker installs package as `air_quality_ml`; `src` is only the source folder, not the package name.

### Stream scoring implementation

```text
air-quality-ml/src/air_quality_ml/inference/stream_score.py
air-quality-ml/jobs/stream_score_latest.py
air-quality-ml/src/air_quality_ml/utils/spark.py
```

Key changes:

- Reads Kafka topics `weather.raw.stream,air_quality.raw.stream`.
- Parses JSON payloads.
- Joins latest weather + air records by city inside each micro-batch.
- Builds realtime features with defaults for unavailable lag features.
- Loads MLflow Spark model.
- Writes parquet predictions to `Data/gold/predictions_stream`.
- Can optionally write to MongoDB via `MONGO_URI`.
- Adds Spark Kafka package through `SPARK_EXTRA_PACKAGES`.

### Serving / dashboard work started

```text
air-quality-ml/dashboard/app.py
air-quality-ml/jobs/batch_score_latest.py
air-quality-ml/jobs/stream_score_latest.py
air-quality-ml/requirements.txt
air-quality-ml/pyproject.toml
docker-compose.ml.yml
```

Key changes:

- Added `streamlit==1.41.1`.
- Added MongoDB service in `docker-compose.ml.yml`.
- Added Streamlit dashboard service.
- Dashboard reads:
  - realtime predictions from MongoDB
  - historical predictions from parquet
  - monitoring metrics from parquet
- Scoring wrappers now pass `MONGO_COLLECTION` as well as `MONGO_URI` and `MONGO_DB`.

## Current Dangling Issue

The last operation was interrupted while running:

```powershell
docker compose -f docker-compose.ml.yml build
```

This build is needed because `streamlit` was added as a dependency.

Next chat should rerun:

```powershell
docker compose -f docker-compose.ml.yml build
```

Then start MongoDB + dashboard:

```powershell
docker compose -f docker-compose.ml.yml up -d mongodb dashboard
```

Open dashboard:

```text
http://localhost:8501
```

## Commands To Continue

### 1. Rebuild image

```powershell
docker compose -f docker-compose.ml.yml build
```

### 2. Start MongoDB and dashboard

```powershell
docker compose -f docker-compose.ml.yml up -d mongodb dashboard
```

### 3. Produce stream records

Terminal 1:

```powershell
python Ingestion\collectors\weather_stream_collector.py --run-once --locations Hanoi
```

Terminal 2:

```powershell
python Ingestion\collectors\air_stream_collector.py --run-once --locations Hanoi
```

### 4. Run stream prediction and write MongoDB

One-shot:

```powershell
docker compose -f docker-compose.ml.yml run --rm `
  -e STREAM_ONCE=1 `
  -e STARTING_OFFSETS=earliest `
  -e MODEL_URI=models:/aq_pm25_forecast_h1/latest `
  -e HORIZON=1 `
  air-quality-ml python jobs/stream_score_latest.py
```

Continuous:

```powershell
docker compose -f docker-compose.ml.yml run --rm `
  -e MODEL_URI=models:/aq_pm25_forecast_h1/latest `
  -e HORIZON=1 `
  air-quality-ml python jobs/stream_score_latest.py
```

### 5. Open dashboard

```text
http://localhost:8501
```

## Environment Warning

During testing, this was run locally:

```powershell
pip install -r Ingestion\requirements.txt
```

It installed `pydantic==1.10.22`, which caused Anaconda warnings:

```text
cannot import name 'AliasGenerator' from 'pydantic'
```

Docker ML is not affected.

Recommended cleanup: use a separate venv for Ingestion, or restore the main env's Pydantic version if needed.

Potential restore command if the current env needs Pydantic v2 again:

```powershell
pip install pydantic==2.10.6
```

But note: Ingestion requirements currently pin Pydantic v1.

## Recommended Next Tasks

1. Finish rebuild after Streamlit dependency.
2. Start `mongodb` and `dashboard` services.
3. Run stream scoring with Mongo enabled and verify dashboard realtime tab.
4. Patch duplicate handling so duplicate records do not go to DLQ.
5. Train remaining horizons:

```text
pm25: h6, h12, h24
alert: h6, h12, h24
```

6. Optionally update docs to say `merge_weather_air_features.py` is the current transform path and `transform_feature.py` is legacy/HDFS-oriented.

