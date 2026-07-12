"""MongoDB collection reader — batched cursor extraction for DB→DB migration."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int
    total_rows: int


def _connection_string(cfg: dict[str, Any]) -> str:
    if cfg.get("connection_string"):
        return cfg["connection_string"]
    if cfg.get("username") and cfg.get("password"):
        return (
            f"mongodb://{cfg['username']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg['port'] or 27017}/"
        )
    return f"mongodb://{cfg['host']}:{cfg['port'] or 27017}/"


def _serialize(value: Any) -> str:
    return cell_to_string(value)


def read_collection_batch(
    *,
    cfg: dict[str, Any],
    database: str,
    collection: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
    from pymongo import MongoClient

    client = MongoClient(_connection_string(cfg), serverSelectionTimeoutMS=10000)
    try:
        coll = client[database][collection]
        if known_total_rows is not None:
            total = known_total_rows
        else:
            total = coll.count_documents({})
        cursor = coll.find({}).skip(offset).limit(limit)
        docs = list(cursor)
        if not docs:
            return ReadBatch(headers=columns or [], rows=[], offset=offset, total_rows=total)

        for doc in docs:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])

        if columns:
            headers = columns
        else:
            keys: set[str] = set()
            for doc in docs:
                keys.update(doc.keys())
            headers = sorted(keys)

        rows = [[_serialize(doc.get(h)) for h in headers] for doc in docs]
        return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    finally:
        client.close()


def read_collection_cursor_batch(
    *,
    cfg: dict[str, Any],
    database: str,
    collection: str,
    cursor_column: str,
    cursor_after: str | None = None,
    columns: list[str] | None = None,
    limit: int = 500,
) -> ReadBatch:
    """Read documents where cursor_column > watermark — incremental sync."""
    from pymongo import MongoClient

    client = MongoClient(_connection_string(cfg), serverSelectionTimeoutMS=10000)
    try:
        coll = client[database][collection]
        query: dict[str, Any] = {}
        if cursor_after is not None and cursor_after != "":
            query[cursor_column] = {"$gt": cursor_after}
        total = coll.count_documents(query)
        cursor = coll.find(query).sort(cursor_column, 1).limit(limit)
        docs = list(cursor)
        if not docs:
            return ReadBatch(headers=columns or [], rows=[], offset=0, total_rows=total)

        for doc in docs:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])

        if columns:
            headers = columns
        else:
            keys: set[str] = set()
            for doc in docs:
                keys.update(doc.keys())
            headers = sorted(keys)

        rows = [[_serialize(doc.get(h)) for h in headers] for doc in docs]
        return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=total)
    finally:
        client.close()
