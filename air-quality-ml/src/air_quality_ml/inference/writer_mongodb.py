from __future__ import annotations

from collections.abc import Iterator
import hashlib
import json

from pymongo import MongoClient, UpdateOne
from pyspark.sql import DataFrame


def _build_upsert_filter(document: dict) -> dict:
    key_fields = ["station_id", "timestamp", "horizon", "model_version"]
    filter_doc = {field: document.get(field) for field in key_fields if document.get(field) is not None}
    if len(filter_doc) == len(key_fields):
        return filter_doc

    fallback_fields = ["station_id", "prediction_time", "horizon", "model_version"]
    fallback_doc = {field: document.get(field) for field in fallback_fields if document.get(field) is not None}
    if len(fallback_doc) == len(fallback_fields):
        return fallback_doc

    payload = json.dumps(document, sort_keys=True, default=str)
    return {"_id": hashlib.sha256(payload.encode("utf-8")).hexdigest()}


def _write_partition(rows: Iterator, mongo_uri: str, database: str, collection: str) -> None:
    client = MongoClient(mongo_uri)
    try:
        col = client[database][collection]
        operations = []
        for row in rows:
            document = row.asDict(recursive=True)
            upsert_filter = _build_upsert_filter(document)
            operations.append(UpdateOne(upsert_filter, {"$set": document}, upsert=True))
            if len(operations) >= 1000:
                col.bulk_write(operations, ordered=False)
                operations = []
        if operations:
            col.bulk_write(operations, ordered=False)
    finally:
        client.close()


def write_predictions_to_mongo(df: DataFrame, mongo_uri: str, database: str, collection: str) -> None:
    df.foreachPartition(lambda rows: _write_partition(rows, mongo_uri, database, collection))
