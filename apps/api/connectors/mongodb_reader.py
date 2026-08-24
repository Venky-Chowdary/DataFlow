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

from services.json_intelligence import expand_mongo_documents
from services.value_serializer import cell_to_string

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


# BSON compares across types by type order, never by value: a `$gt` on a
# datetime can never match a field whose cells hold ISO strings. A watermark
# cast from the *declared* logical type therefore matches nothing and the sync
# reports "no new rows" forever. The cursor value is aligned to the type the
# collection actually stores instead.
_BSON_KIND_OF_TYPE = {
    "string": "string",
    "date": "date",
    "timestamp": "date",
    "int": "number",
    "long": "number",
    "double": "number",
    "decimal": "number",
    "bool": "bool",
    "objectId": "objectid",
}


def stored_cursor_bson_kind(
    coll: Any, field: str, *, sample_limit: int = 5000
) -> str:
    """The BSON type family the collection actually stores in ``field``.

    Returns ``""`` when nothing is stored (empty collection / absent field), and
    raises when the field mixes families — a mixed cursor field cannot bound an
    incremental read, and guessing one family would silently skip the others.
    """
    if not field:
        return ""
    pipeline = [
        {"$match": {field: {"$exists": True, "$ne": None}}},
        {"$limit": int(sample_limit)},
        {"$group": {"_id": {"$type": f"${field}"}}},
    ]
    try:
        types = {str(d.get("_id") or "") for d in coll.aggregate(pipeline)}
    except Exception:
        return ""
    kinds = {_BSON_KIND_OF_TYPE.get(t, "") for t in types if t}
    kinds.discard("")
    if not kinds:
        return ""
    if len(kinds) > 1:
        raise ValueError(
            f"Cursor field '{field}' stores more than one BSON type "
            f"({', '.join(sorted(types))}) in this collection. MongoDB orders "
            "values by type before value, so no single watermark can bound an "
            "incremental read across them — normalise the field to one type at "
            "the source, or run this sync as full refresh."
        )
    return kinds.pop()


def _align_cursor_to_stored_kind(raw: str, casted: Any, kind: str) -> Any:
    """Re-cast a watermark into the BSON family the collection stores."""
    import datetime as _dt
    from decimal import Decimal, InvalidOperation

    from bson.decimal128 import Decimal128
    from bson.objectid import ObjectId

    if not kind:
        return casted
    if kind == "string":
        return raw if isinstance(casted, (_dt.datetime, _dt.date, bool, int, float)) else casted
    if kind == "date":
        if isinstance(casted, _dt.datetime):
            return casted
        parsed = _cast_cursor_value(raw, "TIMESTAMP")
        if isinstance(parsed, _dt.datetime):
            return parsed
        raise ValueError(
            f"Watermark '{raw}' is not a timestamp, but this collection stores "
            "BSON dates in the cursor field — the incremental bound cannot be "
            "compared. Reset the cursor for this stream."
        )
    if kind == "number":
        if isinstance(casted, (int, float, Decimal128)) and not isinstance(casted, bool):
            return casted
        try:
            text = str(raw).replace(",", "")
            return int(text) if text.lstrip("+-").isdigit() else Decimal128(
                str(Decimal(text))
            )
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError(
                f"Watermark '{raw}' is not numeric, but this collection stores "
                "numbers in the cursor field. Reset the cursor for this stream."
            ) from exc
    if kind == "bool":
        return bool(casted) if isinstance(casted, bool) else str(raw).strip().lower() in {
            "true", "t", "yes", "y", "1",
        }
    if kind == "objectid":
        if isinstance(casted, ObjectId):
            return casted
        text = str(raw).strip()
        if ObjectId.is_valid(text):
            return ObjectId(text)
        raise ValueError(
            f"Watermark '{raw}' is not an ObjectId, but this collection stores "
            "ObjectIds in the cursor field. Reset the cursor for this stream."
        )
    return casted


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


def _page_docs_to_batch(
    docs: list[dict[str, Any]],
    *,
    cfg: dict[str, Any],
    columns: list[str] | None,
    offset: int,
    total: int | None,
) -> ReadBatch:
    """Shared projection for batch / cursor / held-scan pages."""
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
    headers = union_attribute_keys(columns, page_keys) if columns else page_keys
    rows = [_project_doc_row(doc, headers) for doc in docs]
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)


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
    # Legacy OFFSET path — .skip(n) is O(n²) and drifts under concurrent
    # inserts. Fresh dumps use read_collection_scan_batch; resume uses
    # read_collection_cursor_batch on ``_id``.
    cursor = coll.find({}).sort("_id", 1).skip(offset).limit(limit)
    docs = list(cursor)
    return _page_docs_to_batch(
        docs, cfg=cfg, columns=columns, offset=offset, total=total
    )


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

    from services.keyset_pagination import split_cursor_bookmark

    client = _mongo_client(_connection_string(cfg))
    coll = client[database][collection]
    query: dict[str, Any] = {}
    sort_spec: list[tuple[str, int]] = [(cursor_column, 1)]
    pk = (cursor_primary_key or "").strip()
    use_composite = bool(pk and pk != cursor_column)
    # The stored family, not the declared one, decides the comparison.
    cursor_kind = stored_cursor_bson_kind(coll, cursor_column)
    pk_kind = stored_cursor_bson_kind(coll, pk) if use_composite else ""

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
        return _align_cursor_to_stored_kind(
            raw, casted, pk_kind if as_id else cursor_kind
        )

    if cursor_after is not None and cursor_after != "":
        cur_raw, pk_raw = split_cursor_bookmark(
            cursor_after, has_tiebreak=use_composite
        )
        if use_composite and pk_raw != "":
            casted = _as_mongo_cursor(cur_raw)
            pk_casted = _as_mongo_cursor(pk_raw, as_id=(pk == "_id"))
            query["$or"] = [
                {cursor_column: {"$gt": casted}},
                {cursor_column: casted, pk: {"$gt": pk_casted}},
            ]
            sort_spec = [(cursor_column, 1), (pk, 1)]
        else:
            casted = _as_mongo_cursor(cur_raw)
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
    return _page_docs_to_batch(
        docs, cfg=cfg, columns=columns, offset=0, total=total
    )


def read_collection_scan_batch(
    *,
    cfg: dict[str, Any],
    database: str,
    collection: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
    scan_state: dict[str, Any] | None = None,
) -> ReadBatch:
    """Page one ``find().sort(_id)`` cursor with getmore — no ``.skip()``.

    Debezium/Fivetran snapshot the collection on a held ``_id``-ordered cursor.
    ``skip(offset)`` walks the prefix on every page (O(n²)) and, under concurrent
    inserts, can skip or duplicate documents. Mid-run resume must not call this
    from a non-zero offset — stream.py keeps the held cursor or seeks ``_id``.
    """
    from connectors.sql_snapshot_scan import close_table_scan

    state = scan_state if scan_state is not None else {}
    if not state.get("started"):
        client = _mongo_client(_connection_string(cfg))
        coll = client[database][collection]
        if known_total_rows is not None:
            total = known_total_rows
        else:
            total = coll.count_documents({})
        cursor = coll.find({}).sort("_id", 1)
        try:
            cursor = cursor.batch_size(max(1, int(limit)))
        except Exception:
            pass
        state.update(
            started=True,
            client=client,
            cur=cursor,
            total=total,
            headers=list(columns or []),
            cfg=cfg,
        )
    cursor = state["cur"]
    page: list[dict[str, Any]] = []
    n = max(1, int(limit))
    try:
        for _ in range(n):
            page.append(dict(next(cursor)))
    except StopIteration:
        pass
    if not page:
        headers = list(state.get("headers") or columns or [])
        total = state.get("total")
        close_table_scan(state)
        return ReadBatch(headers=headers, rows=[], offset=offset, total_rows=total)
    batch = _page_docs_to_batch(
        page,
        cfg=state.get("cfg") or cfg,
        columns=state.get("headers") or columns,
        offset=offset,
        total=state.get("total"),
    )
    state["headers"] = list(batch.headers)
    return batch
