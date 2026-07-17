"""MongoDB collection reader — batched cursor extraction for DB→DB migration."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string

from .mongodb_common import _mongo_client


def _cast_cursor_value(value: str, cursor_type: str | None = None) -> Any:
    """Convert a string cursor value into a BSON-native type for MongoDB queries."""
    from datetime import datetime
    from decimal import InvalidOperation

    from bson.decimal128 import Decimal128

    from services.cdc_engine import WatermarkType, infer_watermark_type

    if not value:
        return value

    ctype = (cursor_type or "").upper()
    if ctype in {"INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "SERIAL", "BIGSERIAL"}:
        try:
            return int(value.replace(",", ""))
        except ValueError:
            return value
    if ctype in {"DECIMAL", "NUMERIC", "NUMBER", "MONEY", "SMALLMONEY"}:
        try:
            return Decimal128(value.replace(",", ""))
        except (InvalidOperation, ValueError):
            return value
    if ctype in {"BOOLEAN", "BOOL"}:
        return value.strip().lower() in {"true", "t", "yes", "y", "1"}
    if ctype in {"DATETIME", "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP_TZ", "TIMESTAMP_LTZ", "DATE"}:
        text = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return value
    if ctype in {"STRING", "VARCHAR", "TEXT", "CHAR"}:
        return value

    wm_type = infer_watermark_type([value])
    if wm_type == WatermarkType.INTEGER:
        try:
            return int(value.replace(",", ""))
        except ValueError:
            return value
    if wm_type == WatermarkType.FLOAT:
        try:
            return Decimal128(value.replace(",", ""))
        except (InvalidOperation, ValueError):
            return value
    if wm_type == WatermarkType.DATETIME:
        text = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return value
    return value


def _connection_string(cfg: dict[str, Any]) -> str:
    from connectors.mongodb_common import normalize_mongodb_connection_string

    return normalize_mongodb_connection_string(
        cfg.get("connection_string", ""),
        database=cfg.get("database", ""),
        host=cfg.get("host", ""),
        port=cfg.get("port", 0),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        ssl=bool(cfg.get("ssl")),
        auth_source=cfg.get("auth_source", ""),
    )


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
    client = _mongo_client(_connection_string(cfg))
    coll = client[database][collection]
    if known_total_rows is not None:
        total = known_total_rows
    else:
        total = coll.count_documents({})
    # Sort by _id so the first batch and any offset-based pagination share the
    # same ordering as cursor/keyset pagination. Without this, the initial
    # batch can come back in insertion order and the keyset cursor will skip
    # every document whose _id is less than an arbitrary high watermark.
    cursor = coll.find({}).sort("_id", 1).skip(offset).limit(limit)
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


def read_collection_cursor_batch(
    *,
    cfg: dict[str, Any],
    database: str,
    collection: str,
    cursor_column: str,
    cursor_after: str | None = None,
    cursor_type: str | None = None,
    columns: list[str] | None = None,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
    """Read documents where cursor_column > watermark — incremental sync."""
    client = _mongo_client(_connection_string(cfg))
    coll = client[database][collection]
    query: dict[str, Any] = {}
    if cursor_after is not None and cursor_after != "":
        from bson.objectid import ObjectId

        casted = _cast_cursor_value(cursor_after, cursor_type)
        # _id is frequently an ObjectId in native MongoDB collections. When the
        # cursor value is a 24-character hex string, compare it as an ObjectId
        # so keyset pagination continues past the first batch.
        if (
            cursor_column == "_id"
            and isinstance(casted, str)
            and len(casted) == 24
            and ObjectId.is_valid(casted)
        ):
            casted = ObjectId(casted)
        query[cursor_column] = {"$gt": casted}
    if known_total_rows is not None:
        total = known_total_rows
    else:
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
