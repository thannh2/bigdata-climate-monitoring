from __future__ import annotations

from pyspark.sql import functions as F

from air_quality_ml.processing.build_targets import add_alert_targets, add_pm25_targets


def test_add_pm25_targets_and_alert_targets(spark):
    data = [
        ("s1", "2026-01-01 00:00:00", 20.0),
        ("s1", "2026-01-01 01:00:00", 40.0),
        ("s1", "2026-01-01 02:00:00", 30.0),
    ]
    df = spark.createDataFrame(data, ["station_id", "event_hour", "pm25"]).withColumn("event_hour", F.to_timestamp("event_hour"))

    out = add_pm25_targets(df, horizons=[1])
    out = add_alert_targets(out, horizons=[1], threshold=35.0)

    first = out.orderBy("event_hour").collect()[0]
    assert abs(first["target_pm25_1h"] - 40.0) < 1e-9
    assert first["target_alert_1h"] == 1
