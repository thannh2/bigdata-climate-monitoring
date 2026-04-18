from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import mlflow

from air_quality_ml.settings import BaseSettings, JobConfig


def configure_mlflow(settings: BaseSettings) -> None:
    mlflow.set_tracking_uri(settings.mlflow.tracking_uri)
    if settings.mlflow.registry_uri:
        mlflow.set_registry_uri(settings.mlflow.registry_uri)


def set_experiment(job: JobConfig, settings: BaseSettings) -> str:
    experiment_name = job.experiment_name
    if not experiment_name.startswith(settings.mlflow.experiment_root):
        experiment_name = f"{settings.mlflow.experiment_root}/{experiment_name}"
    mlflow.set_experiment(experiment_name)
    return experiment_name


@contextmanager
def start_training_run(run_name: str, tags: dict[str, str]) -> Iterator[mlflow.ActiveRun]:
    with mlflow.start_run(run_name=run_name) as run:
        if tags:
            mlflow.set_tags(tags)
        yield run
