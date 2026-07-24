"""DynamoDB table reader — paginated Scan with zero silent attribute loss.

Contract (honest vs Airbyte/Fivetran document sources)
----------------------------------------------------
1. **Union keys** across describe + every Scan page — sparse attrs never vanish.
2. **Binary sets (BS)** serialize as base64 lists — never crash the batch.
3. **Explicit Dynamo NULL** vs missing attribute stay distinguishable on the wire
   (``__DF_DDB_NULL__`` sentinel → SQL NULL; missing stays empty until mapped).
4. Nested **M/L** stay as JSON blobs by default; Map STRUCT policy / flatten can
   expand them (same path as Mongo). Sets keep a typed envelope when possible.
"""

from __future__ import annotations

import base64
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

from connectors.aws_common import boto3_client
from connectors.base import ReadBatch
from connectors.header_union import union_attribute_keys

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string

# Explicit DynamoDB NULL AttributeValue — not the same as a missing attribute.
DDB_EXPLICIT_NULL = object()
DDB_NULL_SENTINEL = "__DF_DDB_NULL__"
# Typed set envelopes so SS/NS/BS are not silently equated with L.
_SET_KIND_KEY = "_df_ddb_set"
SET_KIND_KEY = _SET_KIND_KEY  # public alias for json_intelligence


def is_ddb_null_sentinel(value: Any) -> bool:
    return value is DDB_EXPLICIT_NULL or value == DDB_NULL_SENTINEL


def list_tables(cfg: dict[str, Any]) -> list[str]:
    client = boto3_client("dynamodb", cfg)
    tables: list[str] = []
    token = None
    while True:
        kwargs: dict[str, Any] = {}
        if token:
            kwargs["ExclusiveStartTableName"] = token
        resp = client.list_tables(**kwargs)
        tables.extend(resp.get("TableNames") or [])
        token = resp.get("LastEvaluatedTableName")
        if not token:
            break
    return tables


def _ddb_attr_type(attr_type: str) -> str:
    mapping = {
        "S": "VARCHAR",
        "N": "DECIMAL",
        "B": "BINARY",
    }
    return mapping.get((attr_type or "S").upper(), "VARCHAR")


def _encode_binaryish(value: Any) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    cls = value.__class__.__name__
    if cls == "Binary":
        raw = getattr(value, "value", None)
        if raw is None:
            raw = bytes(value)
        return base64.b64encode(bytes(raw)).decode("ascii")
    return cell_to_string(value)


def _deserialize(value: Any, *, set_kind: str | None = None) -> Any:
    """Recursively deserialize DynamoDB values; never crash on Binary sets."""
    if value is DDB_EXPLICIT_NULL:
        return DDB_EXPLICIT_NULL
    if isinstance(value, set):
        items: list[Any] = []
        for v in value:
            items.append(_deserialize(v))
        # Sort only when comparable; Binary/base64 → stable string sort.
        try:
            items_sorted = sorted(items, key=lambda x: json.dumps(x, default=str, sort_keys=True))
        except TypeError:
            items_sorted = [_encode_binaryish(x) if not isinstance(x, (str, int, float, bool, type(None))) else x for x in items]
            items_sorted = sorted(items_sorted, key=lambda x: str(x))
        if set_kind:
            return {_SET_KIND_KEY: set_kind, "v": items_sorted}
        return items_sorted
    if isinstance(value, dict):
        if _SET_KIND_KEY in value:
            return value
        return {k: _deserialize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_deserialize(v) for v in value]
    cls = value.__class__.__name__
    if cls == "Binary":
        return _encode_binaryish(value)
    return value


def _item_to_record(item: dict) -> dict[str, Any]:
    """Typed AttributeValue → Python, preserving explicit NULL vs nested structure."""
    from boto3.dynamodb.types import TypeDeserializer

    deser = TypeDeserializer()
    out: dict[str, Any] = {}
    for key, av in item.items():
        if not isinstance(av, dict):
            out[key] = av
            continue
        if "NULL" in av:
            out[key] = DDB_EXPLICIT_NULL
            continue
        set_kind = None
        if "SS" in av:
            set_kind = "SS"
        elif "NS" in av:
            set_kind = "NS"
        elif "BS" in av:
            set_kind = "BS"
        try:
            raw = deser.deserialize(av)
        except Exception:
            # Fail closed into a quarantine-friendly string — never drop the attribute.
            out[key] = json.dumps({"_df_ddb_error": True, "raw": str(av)}, separators=(",", ":"))
            continue
        if set_kind == "BS":
            # TypeDeserializer may yield set[Binary] — encode without sorted(Binary).
            encoded = [_encode_binaryish(v) for v in (raw if isinstance(raw, (set, list)) else [raw])]
            out[key] = {_SET_KIND_KEY: "BS", "v": sorted(encoded)}
        else:
            out[key] = _deserialize(raw, set_kind=set_kind)
    return out


def infer_logical_from_native(value: Any) -> str | None:
    """Classify a deserialized Dynamo value before stringification.

    Explicit NULL / missing Python None do not cast a vote — returning ``None``
    keeps null-only attributes from inventing VARCHAR.
    """
    if value is DDB_EXPLICIT_NULL or value is None:
        return None
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int) and not isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, Decimal):
        # Whole numbers stay INTEGER when within bigint; else DECIMAL (never invent FLOAT).
        try:
            if value == value.to_integral_value() and abs(value) <= Decimal("9223372036854775807"):
                return "INTEGER"
        except Exception:
            pass
        return "DECIMAL"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, (bytes, bytearray)) or value.__class__.__name__ == "Binary":
        return "BINARY"
    if isinstance(value, dict):
        if _SET_KIND_KEY in value:
            kind = value.get(_SET_KIND_KEY)
            if kind == "BS":
                return "BINARY"
            return "ARRAY"
        return "JSON"
    if isinstance(value, list):
        return "ARRAY"
    if isinstance(value, str):
        return "VARCHAR" if len(value) <= 255 else "TEXT"
    return "VARCHAR"


def _cell(value: Any) -> str:
    if value is DDB_EXPLICIT_NULL:
        return DDB_NULL_SENTINEL
    if value is None:
        return ""
    return cell_to_string(value)


def estimate_item_count(cfg: dict[str, Any], table: str) -> int:
    """Approximate row count from DescribeTable (updated ~6h)."""
    client = boto3_client("dynamodb", cfg)
    resp = client.describe_table(TableName=table)
    return int(resp.get("Table", {}).get("ItemCount", 0) or 0)


def describe_table_schema(cfg: dict[str, Any], table: str) -> tuple[list[str], dict[str, str]]:
    """Return column names and types from KeySchema + AttrDefs + **sample union**."""
    client = boto3_client("dynamodb", cfg)
    resp = client.describe_table(TableName=table)
    table_info = resp.get("Table") or {}
    columns: list[str] = []
    types: dict[str, str] = {}

    for key in table_info.get("KeySchema") or []:
        name = key.get("AttributeName")
        if name and name not in columns:
            columns.append(name)
            types[name] = "VARCHAR"

    attr_defs = {
        a.get("AttributeName"): a.get("AttributeType", "S")
        for a in (table_info.get("AttributeDefinitions") or [])
        if a.get("AttributeName")
    }
    for name, attr_type in attr_defs.items():
        if name not in columns:
            columns.append(name)
        types[name] = _ddb_attr_type(attr_type)

    # Always sample — Dynamo is schemaless beyond keys; sparse attrs must surface.
    try:
        sample, _ = read_table_batch(cfg=cfg, table=table, limit=50, columns=None)
        for header in sample.headers:
            if header not in columns:
                columns.append(header)
        # Native-type majority from the same sample batch (typed records path).
        typed = getattr(sample, "meta", None) or {}
        native_types = typed.get("native_types") if isinstance(typed, dict) else None
        if isinstance(native_types, dict):
            for name, lt in native_types.items():
                if name in types and types[name] in {"VARCHAR", "TEXT"}:
                    types[name] = str(lt)
                else:
                    types.setdefault(name, str(lt))
        else:
            for header in sample.headers:
                types.setdefault(header, "VARCHAR")
    except Exception:
        pass

    return columns, types


def read_table_batch(
    *,
    cfg: dict[str, Any],
    table: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 500,
    exclusive_start_key: dict | None = None,
    total_rows: int | None = None,
    expand_nested: bool | None = None,
) -> tuple[ReadBatch, dict | None]:
    """Scan one page. When ``columns`` is set, still **union** any new keys found
    on this page into headers so sparse attributes are never silently dropped.
    """
    from services.json_intelligence import expand_dynamo_documents, mongo_flatten_enabled

    client = boto3_client("dynamodb", cfg)
    scan_kwargs: dict[str, Any] = {"TableName": table, "Limit": limit}
    if exclusive_start_key:
        scan_kwargs["ExclusiveStartKey"] = exclusive_start_key

    resp = client.scan(**scan_kwargs)
    items = resp.get("Items") or []
    records = [_item_to_record(it) for it in items]

    do_expand = mongo_flatten_enabled(cfg) if expand_nested is None else bool(expand_nested)
    if do_expand and records:
        # Flatten nested M leaves (keep parent blob) — same contract as Mongo.
        records = expand_dynamo_documents(records, cfg=cfg)

    page_keys: list[str] = []
    seen_page: set[str] = set()
    for rec in records:
        for k in rec.keys():
            if k not in seen_page:
                seen_page.add(k)
                page_keys.append(k)

    if columns:
        headers = union_attribute_keys(columns, page_keys)
    else:
        headers = page_keys

    # Majority native types for this page (introspect / Map seed).
    native_votes: dict[str, dict[str, int]] = {h: {} for h in headers}
    for rec in records:
        for h in headers:
            if h not in rec:
                continue
            lt = infer_logical_from_native(rec[h])
            if not lt:
                continue
            native_votes[h][lt] = native_votes[h].get(lt, 0) + 1
    native_types = {
        h: max(votes, key=votes.get) if votes else "VARCHAR"
        for h, votes in native_votes.items()
    }

    rows = []
    for rec in records:
        row: list[str] = []
        for h in headers:
            if h not in rec:
                row.append("")
            else:
                row.append(_cell(rec[h]))
        rows.append(row)

    if total_rows is None:
        try:
            approx = estimate_item_count(cfg, table)
        except Exception:
            approx = None
    else:
        approx = total_rows
    next_key = resp.get("LastEvaluatedKey")
    # DescribeTable.ItemCount is approximate — never use it as a hard Scan bound.
    # LastEvaluatedKey is the sole completion authority for multi-page reads.
    meta: dict[str, Any] = {"native_types": native_types}
    if approx is not None:
        meta["approx_item_count"] = int(approx)
    batch = ReadBatch(
        headers=headers,
        rows=rows,
        offset=offset,
        total_rows=None,
        meta=meta,
    )
    return batch, next_key


def read_all_paginated(cfg: dict[str, Any], table: str, limit: int = 100_000) -> ReadBatch:
    """Full table read up to limit — headers grow as sparse attrs appear."""
    all_rows: list[list[str]] = []
    headers: list[str] = []
    next_key = None
    offset = 0
    total_estimate: int | None = None
    native_acc: dict[str, dict[str, int]] = {}

    while len(all_rows) < limit:
        batch, next_key = read_table_batch(
            cfg=cfg,
            table=table,
            columns=headers or None,
            offset=offset,
            limit=min(500, limit - len(all_rows)),
            exclusive_start_key=next_key,
            total_rows=total_estimate,
        )
        if total_estimate is None:
            total_estimate = batch.total_rows

        # Grow header union; backfill prior rows with "" for new columns (no data loss).
        if batch.headers:
            new_headers = union_attribute_keys(headers, batch.headers)
            if new_headers != headers:
                old_index = {h: i for i, h in enumerate(headers)}
                rebuilt: list[list[str]] = []
                for row in all_rows:
                    rebuilt.append([row[old_index[h]] if h in old_index and old_index[h] < len(row) else "" for h in new_headers])
                all_rows = rebuilt
                headers = new_headers

            batch_index = {h: i for i, h in enumerate(batch.headers)}
            for row in batch.rows:
                all_rows.append([row[batch_index[h]] if h in batch_index and batch_index[h] < len(row) else "" for h in headers])

            meta = getattr(batch, "meta", None) or {}
            for name, lt in (meta.get("native_types") or {}).items():
                votes = native_acc.setdefault(name, {})
                votes[lt] = votes.get(lt, 0) + 1

        offset += len(batch.rows)
        if not next_key or not batch.rows:
            break

    native_types = {
        h: max(votes, key=votes.get) if votes else "VARCHAR"
        for h, votes in native_acc.items()
    }
    out = ReadBatch(
        headers=headers,
        rows=all_rows,
        offset=0,
        total_rows=max(len(all_rows), total_estimate or len(all_rows)),
        meta={"native_types": native_types},
    )
    return out
