from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    # Windows: dam bao Spark Python worker dung dung python executable
    # (tranh loi "Python was not found" do alias Microsoft Store).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("air-quality-ml-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
