"""DynamoDB table writer — BatchWriteItem loads."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from connectors.aws_common import boto3_client
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    build_mapped_rows_with_details,
    resolve_target_columns,
    row_checksum,
    transform_error_policy,
)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "boto3"


def _to_dynamo_value(value: Any, source_type: str) -> Any:
    """Convert transform-engine values to DynamoDB-serializable native types."""
    if value is None:
        return None
    upper = source_type.upper()
    if upper in {"DECIMAL", "NUMERIC"}:
        try:
            return Decimal(value)
        except Exception:
            return value
    if upper in {"JSON", "OBJECT", "ARRAY", "VARIANT"}:
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value, parse_float=Decimal, parse_constant=lambda v: None)
            except json.JSONDecodeError:
                return value
        return value
    if upper in {"BINARY", "BLOB", "BYTEA", "VARBINARY"}:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            try:
                return base64.b64decode(value, validate=True)
            except Exception:
                return value.encode("utf-8")
        return value
    return value


def _to_attr(value: Any, source_type: str) -> dict:
    from boto3.dynamodb.types import TypeSerializer

    ser = TypeSerializer()
    return ser.serialize(_to_dynamo_value(value, source_type))


def write_mapped_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    backfill_new_fields: bool = False,
    endpoint_url: str = "",
    conflict_columns: list[str] | None = None,
    **_kwargs: Any,
) -> WriteResult:
    del schema, ssl, backfill_new_fields
    policy = transform_error_policy(error_policy)
    table = table_name or database
    cfg = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "endpoint_url": endpoint_url,
    }
    target_cols, logical_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    dest_types = {target_cols[i]: logical_types[i] for i in range(len(target_cols))}
    mapped_rows, errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=dest_types,
        preserve_case=True,
        error_policy=policy,
    )

    client = boto3_client("dynamodb", cfg)
    if create_table:
        _ensure_table(
            client,
            table,
            target_cols,
            mappings,
            logical_types,
            conflict_columns=conflict_columns or _kwargs.get("conflict_columns"),
        )

    key_types = _table_key_types(client, table)

    written = 0
    batch_size = 25
    chunks = max(1, (len(mapped_rows) + batch_size - 1) // batch_size)

    try:
        for chunk_idx in range(chunks):
            slice_rows = mapped_rows[chunk_idx * batch_size : (chunk_idx + 1) * batch_size]
            request_items = []
            for row in slice_rows:
                item = {}
                for i, col in enumerate(target_cols):
                    value = row[i]
                    attr_type = key_types.get(col)
                    if attr_type == "S":
                        value = str(value) if value is not None else ""
                    elif attr_type == "N":
                        try:
                            value = Decimal(value) if value is not None else None
                        except Exception:
                            value = value
                    elif attr_type == "B":
                        if isinstance(value, str):
                            value = value.encode("utf-8")
                    item[col] = _to_attr(value, logical_types[i])
                request_items.append({"PutRequest": {"Item": item}})
            _batch_write_with_retry(client, table, request_items)
            written += len(slice_rows)
            if on_checkpoint:
                on_checkpoint(chunk_idx + 1, chunks, written)

        from connectors.aws_common import resolve_endpoint_url, resolve_region

        region = resolve_region(cfg)
        endpoint = resolve_endpoint_url(cfg)
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=table,
            target_schema=endpoint or region,
            checksum=row_checksum(mapped_rows, target_cols),
            chunks_completed=chunks,
            warnings=errors[:10],
            rejected_rows=len({d["row"] for d in rejected_details}),
            rejected_details=rejected_details[:100],
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=written, table_name=table, target_schema=host or "",
            checksum="", chunks_completed=0, error=str(exc),
        )


def _table_key_types(client, table: str) -> dict[str, str]:
    """Return key attribute names -> DynamoDB type ('S', 'N', 'B') for an existing table."""
    from botocore.exceptions import ClientError
    try:
        info = client.describe_table(TableName=table)["Table"]
        attrs = {a["AttributeName"]: a["AttributeType"] for a in info.get("AttributeDefinitions", [])}
        keys = {}
        for ks in info.get("KeySchema", []):
            name = ks["AttributeName"]
            if name in attrs:
                keys[name] = attrs[name]
        return keys
    except ClientError:
        return {}


def _attr_type_for_logical(logical: str) -> str:
    upper = (logical or "").upper()
    if upper.startswith("DECIMAL") or upper in {
        "INTEGER", "NUMERIC", "FLOAT", "DOUBLE", "LONG", "BIGINT", "NUMBER",
    }:
        return "N"
    if upper in {"BINARY", "BLOB", "BYTEA", "VARBINARY"}:
        return "B"
    return "S"


def _resolve_key_schema(
    target_cols: list[str],
    mappings: list[dict],
    *,
    conflict_columns: list[str] | None,
    source_types: list[str] | None,
) -> list[tuple[str, str, str]]:
    """Return [(name, KeyType, AttrType), ...] for create-table.

    Prefer explicit conflict_columns (HASH, optional RANGE). Refuse inventing a
    key from an arbitrary first column when no identity metadata is available.
    """
    conflict = [c for c in (conflict_columns or []) if c and c in target_cols]
    if not conflict:
        # Legacy soft path only when a clear identity name exists.
        preferred = {"id", "_id", "pk", "sk", "uuid", "key"}
        lower_map = {c.lower(): c for c in target_cols}
        for name in preferred:
            if name in lower_map:
                conflict = [lower_map[name]]
                break
        if not conflict:
            for c in target_cols:
                if c.lower().endswith("_id"):
                    conflict = [c]
                    break
    if not conflict:
        raise ValueError(
            "DynamoDB create-table requires conflict_columns (HASH[, RANGE]) "
            "or a clear identity column (id/_id/*_id); refusing to invent a key "
            "from the first mapped column"
        )
    keys: list[tuple[str, str, str]] = []
    for i, col in enumerate(conflict[:2]):
        logical = ""
        if source_types and col in target_cols:
            logical = source_types[target_cols.index(col)] or ""
        keys.append((col, "HASH" if i == 0 else "RANGE", _attr_type_for_logical(logical)))
    return keys


def _ensure_table(
    client,
    table: str,
    target_cols: list[str],
    mappings: list[dict],
    source_types: list[str] | None = None,
    *,
    conflict_columns: list[str] | None = None,
) -> None:
    from botocore.exceptions import ClientError

    try:
        client.describe_table(TableName=table)
        return
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise
    if not target_cols:
        raise ValueError(f"DynamoDB table `{table}` does not exist and no columns were provided to create it.")
    key_schema = _resolve_key_schema(
        target_cols,
        mappings,
        conflict_columns=conflict_columns,
        source_types=source_types,
    )
    # Deduplicate attribute definitions (HASH/RANGE may share names only once).
    attr_defs = []
    seen: set[str] = set()
    for name, _kt, at in key_schema:
        if name in seen:
            continue
        seen.add(name)
        attr_defs.append({"AttributeName": name, "AttributeType": at})
    client.create_table(
        TableName=table,
        AttributeDefinitions=attr_defs,
        KeySchema=[{"AttributeName": name, "KeyType": kt} for name, kt, _at in key_schema],
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = client.get_waiter("table_exists")
    waiter.wait(TableName=table)


def _batch_write_with_retry(client, table: str, request_items: list[dict], retries: int = 5) -> None:
    pending = {table: request_items}
    for _ in range(retries):
        if not pending.get(table):
            return
        resp = client.batch_write_item(RequestItems=pending)
        pending = resp.get("UnprocessedItems") or {}
    if pending.get(table):
        raise RuntimeError(f"DynamoDB batch write left {len(pending[table])} unprocessed items")
