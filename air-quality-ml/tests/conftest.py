from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = SparkSession.builder.master("local[2]").appName("air-quality-ml-tests").getOrCreate()
    yield spark
    spark.stop()
