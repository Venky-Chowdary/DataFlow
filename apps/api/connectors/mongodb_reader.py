"""MongoDB collection reader — batched cursor extraction for DB→DB migration."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.header_union import union_attribute_keys

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string
from services.json_intelligence import expand_mongo_documents

from .mongodb_common import _mongo_client


def _cast_cursor_value(value: str, cursor_type: str | None = None) -> Any:
    """Convert a string cursor value into a BSON-native type for MongoDB queries."""
    from datetime import datetime
    from decimal import InvalidOperation, Overflow

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
        except (InvalidOperation, Overflow, ValueError):
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
        except (InvalidOperation, Overflow, ValueError):
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
    # BSON null must stay distinct from missing/empty string on SQL sinks.
    return cell_to_string(value, preserve_sql_null=True)


def _project_doc_row(doc: dict[str, Any], headers: list[str]) -> list[str]:
    """Project a Mongo document onto headers — missing ≠ explicit null."""
    from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL

    row: list[str] = []
    for h in headers:
        if h not in doc:
            row.append(DF_MISSING_SENTINEL)
        elif doc[h] is None:
            row.append(SQL_NULL_SENTINEL)
        else:
            row.append(_serialize(doc[h]))
    return row


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

    docs = expand_mongo_documents(docs, cfg=cfg)

    page_keys: list[str] = []
    seen: set[str] = set()
    for doc in docs:
        for k in doc.keys():
            if k not in seen:
                seen.add(k)
                page_keys.append(k)
    # Always union — sparse fields mid-collection must not vanish when columns frozen.
    headers = union_attribute_keys(columns, page_keys) if columns else page_keys

    rows = [_project_doc_row(doc, headers) for doc in docs]
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
    cursor_primary_key: str | None = None,
) -> ReadBatch:
    """Read documents where cursor_column > watermark — incremental sync.

    When ``cursor_primary_key`` is set, uses lexicographic ``(cursor, pk)`` so
    documents sharing a timestamp watermark are not skipped forever (Airbyte trap).
    """
    from bson.objectid import ObjectId

    client = _mongo_client(_connection_string(cfg))
    coll = client[database][collection]
    query: dict[str, Any] = {}
    sort_spec: list[tuple[str, int]] = [(cursor_column, 1)]
    pk = (cursor_primary_key or "").strip()
    use_composite = bool(pk and pk != cursor_column)

    def _as_mongo_cursor(raw: str, *, as_id: bool = False) -> Any:
        casted = _cast_cursor_value(raw, cursor_type if not as_id else "STRING")
        if (
            as_id
            and isinstance(casted, str)
            and len(casted) == 24
            and ObjectId.is_valid(casted)
        ):
            return ObjectId(casted)
        if (
            not as_id
            and cursor_column == "_id"
            and isinstance(casted, str)
            and len(casted) == 24
            and ObjectId.is_valid(casted)
        ):
            return ObjectId(casted)
        return casted

    if cursor_after is not None and cursor_after != "":
        if use_composite and "|" in str(cursor_after):
            cur_raw, pk_raw = str(cursor_after).split("|", 1)
            casted = _as_mongo_cursor(cur_raw)
            pk_casted = _as_mongo_cursor(pk_raw, as_id=(pk == "_id"))
            query["$or"] = [
                {cursor_column: {"$gt": casted}},
                {cursor_column: casted, pk: {"$gt": pk_casted}},
            ]
            sort_spec = [(cursor_column, 1), (pk, 1)]
        else:
            casted = _as_mongo_cursor(str(cursor_after))
            query[cursor_column] = {"$gt": casted}
            if use_composite:
                sort_spec = [(cursor_column, 1), (pk, 1)]
    elif use_composite:
        sort_spec = [(cursor_column, 1), (pk, 1)]

    if known_total_rows is not None:
        total = known_total_rows
    else:
        total = coll.count_documents(query)
    cursor = coll.find(query).sort(sort_spec).limit(limit)
    docs = list(cursor)
    if not docs:
        return ReadBatch(headers=columns or [], rows=[], offset=0, total_rows=total)

    for doc in docs:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])

    docs = expand_mongo_documents(docs, cfg=cfg)

    page_keys: list[str] = []
    seen: set[str] = set()
    for doc in docs:
        for k in doc.keys():
            if k not in seen:
                seen.add(k)
                page_keys.append(k)
    headers = union_attribute_keys(columns, page_keys) if columns else page_keys

    rows = [_project_doc_row(doc, headers) for doc in docs]
    return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=total)
