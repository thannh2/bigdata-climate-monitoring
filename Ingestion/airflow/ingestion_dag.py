from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path


try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator
except (ModuleNotFoundError, ImportError):
    DAG = None  # type: ignore[assignment]


ROOT_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT_DIR.parent
PYTHON_EXE = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
PYTHON_CMD = str(PYTHON_EXE) if os.name == "nt" and PYTHON_EXE.exists() else "python"
WEATHER_BATCH_SCRIPT = ROOT_DIR / "collectors" / "weather_batch_collector.py"
AIR_BATCH_SCRIPT = ROOT_DIR / "collectors" / "air_batch_collector.py"
WEATHER_STREAM_SCRIPT = ROOT_DIR / "collectors" / "weather_stream_collector.py"
AIR_STREAM_SCRIPT = ROOT_DIR / "collectors" / "air_stream_collector.py"
OWM_WEATHER_STREAM_SCRIPT = ROOT_DIR / "collectors" / "openweathermap_weather_stream_collector.py"
OWM_AIR_STREAM_SCRIPT = ROOT_DIR / "collectors" / "openweathermap_air_stream_collector.py"
AQICN_AIR_STREAM_SCRIPT = ROOT_DIR / "collectors" / "aqicn_air_stream_collector.py"
IQAIR_AIR_STREAM_SCRIPT = ROOT_DIR / "collectors" / "iqair_air_stream_collector.py"


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _batch_command(script_path: Path) -> str:
    return (
        f'"{PYTHON_CMD}" "{script_path}" '
        '--start-date "{{ dag_run.conf.get(\'start_date\', ds) }}" '
        '--end-date "{{ dag_run.conf.get(\'end_date\', ds) }}"'
    )


def _stream_command(script_path: Path) -> str:
    return (
        "{% set locations = dag_run.conf.get('locations', params.locations) %} "
        f'"{PYTHON_CMD}" "{script_path}" '
        '--run-once'
        '{% if locations %} --locations {{ locations | join(" ") }}{% endif %}'
    )


if DAG is not None:
    with DAG(
        dag_id="historical_ingestion_dag",
        description="Run weather and air-quality historical ingestion by date range.",
        default_args=DEFAULT_ARGS,
        start_date=datetime(2026, 4, 1),
        schedule="0 2 * * *",
        catchup=False,
        params={"start_date": "{{ ds }}", "end_date": "{{ ds }}"},
        tags=["ingestion", "batch", "climate"],
    ) as historical_ingestion_dag:
        start = EmptyOperator(task_id="start")

        fetch_weather_history = BashOperator(
            task_id="fetch_weather_history",
            bash_command=_batch_command(WEATHER_BATCH_SCRIPT),
            cwd=str(REPO_ROOT),
        )

        fetch_air_history = BashOperator(
            task_id="fetch_air_history",
            bash_command=_batch_command(AIR_BATCH_SCRIPT),
            cwd=str(REPO_ROOT),
        )

        finalize_batch = EmptyOperator(task_id="finalize_batch")

        start >> [fetch_weather_history, fetch_air_history] >> finalize_batch

    with DAG(
        dag_id="streaming_trigger_dag",
        description="Trigger one polling cycle for all configured multi-source streaming collectors.",
        default_args=DEFAULT_ARGS,
        start_date=datetime(2026, 4, 1),
        schedule="*/5 * * * *",
        catchup=False,
        params={"locations": []},
        tags=["ingestion", "stream", "climate"],
    ) as streaming_trigger_dag:
        start_stream = EmptyOperator(task_id="start_stream")

        trigger_weather_stream = BashOperator(
            task_id="trigger_weather_stream",
            bash_command=_stream_command(WEATHER_STREAM_SCRIPT),
            cwd=str(REPO_ROOT),
        )

        trigger_air_stream = BashOperator(
            task_id="trigger_air_stream",
            bash_command=_stream_command(AIR_STREAM_SCRIPT),
            cwd=str(REPO_ROOT),
        )

        trigger_owm_weather_stream = BashOperator(
            task_id="trigger_owm_weather_stream",
            bash_command=_stream_command(OWM_WEATHER_STREAM_SCRIPT),
            cwd=str(REPO_ROOT),
        )

        trigger_owm_air_stream = BashOperator(
            task_id="trigger_owm_air_stream",
            bash_command=_stream_command(OWM_AIR_STREAM_SCRIPT),
            cwd=str(REPO_ROOT),
        )

        trigger_aqicn_air_stream = BashOperator(
            task_id="trigger_aqicn_air_stream",
            bash_command=_stream_command(AQICN_AIR_STREAM_SCRIPT),
            cwd=str(REPO_ROOT),
        )

        trigger_iqair_air_stream = BashOperator(
            task_id="trigger_iqair_air_stream",
            bash_command=_stream_command(IQAIR_AIR_STREAM_SCRIPT),
            cwd=str(REPO_ROOT),
        )

        finalize_stream = EmptyOperator(task_id="finalize_stream")

        start_stream >> [
            trigger_weather_stream,
            trigger_air_stream,
            trigger_owm_weather_stream,
            trigger_owm_air_stream,
            trigger_aqicn_air_stream,
            trigger_iqair_air_stream,
        ] >> finalize_stream
