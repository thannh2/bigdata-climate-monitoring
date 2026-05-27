from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from pymongo import MongoClient


st.set_page_config(page_title="Air Quality Monitor", layout="wide")


def _mongo_config() -> tuple[str, str, str]:
    return (
        os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        os.getenv("MONGO_DB", "air_quality"),
        os.getenv("MONGO_COLLECTION", "realtime_predictions"),
    )


@st.cache_data(ttl=10)
def load_realtime(limit: int = 500) -> pd.DataFrame:
    mongo_uri, database, collection = _mongo_config()
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
    try:
        docs = list(
            client[database][collection]
            .find({}, {"_id": 0})
            .sort("prediction_time", -1)
            .limit(int(limit))
        )
    finally:
        client.close()

    if not docs:
        return pd.DataFrame()
    return pd.DataFrame(docs)


@st.cache_data(ttl=30)
def load_parquet_dir(path_value: str) -> pd.DataFrame:
    path = Path(path_value)
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _coerce_time(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        df = df.copy()
        df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def _metric_value(df: pd.DataFrame, column: str, default: Any = "-") -> Any:
    if df.empty or column not in df.columns:
        return default
    value = df[column].dropna()
    if value.empty:
        return default
    return value.iloc[0]


def render_realtime() -> None:
    df = _coerce_time(load_realtime(), "prediction_time")
    left, middle, right = st.columns(3)
    left.metric("Realtime Rows", len(df))
    middle.metric("Latest City", _metric_value(df, "city"))
    right.metric("Latest Prediction", _metric_value(df, "prediction"))

    if df.empty:
        st.info("No realtime predictions in MongoDB yet.")
        return

    display_cols = [
        "prediction_time",
        "city",
        "region",
        "horizon",
        "prediction",
        "pred_prob",
        "pred_alert",
        "alert_level",
        "model_name",
        "model_version",
    ]
    st.dataframe(df[[c for c in display_cols if c in df.columns]], use_container_width=True, hide_index=True)

    chart_cols = [c for c in ["prediction_time", "prediction"] if c in df.columns]
    if len(chart_cols) == 2:
        chart_df = df.dropna(subset=chart_cols).sort_values("prediction_time")
        st.line_chart(chart_df.set_index("prediction_time")["prediction"])

    map_cols = {"latitude", "longitude"}
    if map_cols.issubset(df.columns):
        st.map(df.dropna(subset=["latitude", "longitude"]), latitude="latitude", longitude="longitude")


def render_historical() -> None:
    historical_path = os.getenv("HISTORICAL_PREDICTIONS_PATH", "/workspace/Data/gold/predictions")
    stream_path = os.getenv("STREAM_PREDICTIONS_PATH", "/workspace/Data/gold/predictions_stream")
    historical = load_parquet_dir(historical_path)
    stream = load_parquet_dir(stream_path)

    left, right = st.columns(2)
    left.metric("Batch Prediction Rows", len(historical))
    right.metric("Stream Prediction Rows", len(stream))

    tab_batch, tab_stream = st.tabs(["Batch", "Stream"])
    with tab_batch:
        if historical.empty:
            st.info("No batch prediction parquet found.")
        else:
            st.dataframe(historical.tail(500), use_container_width=True, hide_index=True)
    with tab_stream:
        if stream.empty:
            st.info("No stream prediction parquet found.")
        else:
            st.dataframe(stream.tail(500), use_container_width=True, hide_index=True)


def render_monitoring() -> None:
    monitoring_path = os.getenv("MONITORING_PATH", "/workspace/Data/gold/monitoring")
    df = load_parquet_dir(monitoring_path)
    if df.empty:
        st.info("No monitoring metrics found.")
        return

    st.dataframe(df.tail(500), use_container_width=True, hide_index=True)
    if {"metric_name", "metric_value"}.issubset(df.columns):
        latest = df.groupby("metric_name", as_index=False).tail(1)
        st.bar_chart(latest.set_index("metric_name")["metric_value"])


st.title("Air Quality Monitor")
page = st.sidebar.radio("View", ["Realtime", "Historical", "Monitoring"])

if page == "Realtime":
    render_realtime()
elif page == "Historical":
    render_historical()
else:
    render_monitoring()
