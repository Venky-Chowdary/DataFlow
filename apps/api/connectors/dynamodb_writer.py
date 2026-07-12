"""DynamoDB table writer — BatchWriteItem loads."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

from connectors.aws_common import boto3_client
from connectors.writer_common import build_mapped_rows, resolve_target_columns, row_checksum


@dataclass
class WriteResult:
    ok: bool
    rows_written: int
    table_name: str
    target_schema: str
    checksum: str
    chunks_completed: int
    error: str | None = None
    driver: str = "boto3"
    rejected_rows: int = 0
    warnings: list[str] = field(default_factory=list)


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
                return json.loads(value)
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
) -> WriteResult:
    del schema, ssl, error_policy
    table = table_name or database
    cfg = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "connection_string": connection_string,
    }
    target_cols, source_types = resolve_target_columns(mappings, column_types)
    mapped_rows, errors = build_mapped_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
    )

    client = boto3_client("dynamodb", cfg)
    if create_table:
        _ensure_table(client, table, target_cols, mappings)

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
                    item[col] = _to_attr(value, source_types[i])
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
            checksum=row_checksum(mapped_rows),
            chunks_completed=chunks,
            warnings=errors[:10],
            rejected_rows=len(errors),
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


def _pick_hash_key(target_cols: list[str], mappings: list[dict]) -> str:
    preferred = {"id", "_id", "pk", "sk", "uuid", "key"}
    lower_map = {c.lower(): c for c in target_cols}
    for name in preferred:
        if name in lower_map:
            return lower_map[name]
    for c in target_cols:
        lc = c.lower()
        if lc.endswith("_id"):
            return c
    for mapping in mappings:
        target = (mapping.get("target") or "").strip()
        if target and target.lower() in preferred:
            return target
    return target_cols[0] if target_cols else "id"


def _ensure_table(client, table: str, target_cols: list[str], mappings: list[dict]) -> None:
    from botocore.exceptions import ClientError

    try:
        client.describe_table(TableName=table)
        return
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise
    if not target_cols:
        raise ValueError(f"DynamoDB table `{table}` does not exist and no columns were provided to create it.")
    hash_key = _pick_hash_key(target_cols, mappings)
    attr_type = "N" if hash_key.lower().endswith("_id") and hash_key.lower() != "uuid" else "S"
    client.create_table(
        TableName=table,
        AttributeDefinitions=[{"AttributeName": hash_key, "AttributeType": attr_type}],
        KeySchema=[{"AttributeName": hash_key, "KeyType": "HASH"}],
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
