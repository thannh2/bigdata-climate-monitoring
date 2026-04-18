from __future__ import annotations

from collections.abc import Iterator

from pymongo import MongoClient
from pyspark.sql import DataFrame


def _write_partition(rows: Iterator, mongo_uri: str, database: str, collection: str) -> None:
    client = MongoClient(mongo_uri)
    try:
        col = client[database][collection]
        docs = []
        for row in rows:
            docs.append(row.asDict(recursive=True))
            if len(docs) >= 1000:
                col.insert_many(docs, ordered=False)
                docs = []
        if docs:
            col.insert_many(docs, ordered=False)
    finally:
        client.close()


def write_predictions_to_mongo(df: DataFrame, mongo_uri: str, database: str, collection: str) -> None:
    df.foreachPartition(lambda rows: _write_partition(rows, mongo_uri, database, collection))
