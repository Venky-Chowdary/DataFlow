"""DynamoDB table reader — paginated Scan extraction."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from connectors.aws_common import boto3_client
from connectors.base import ReadBatch

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


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


def describe_table_schema(cfg: dict[str, Any], table: str) -> tuple[list[str], dict[str, str]]:
    """Return column names and inferred types from table key schema + sample items."""
    client = boto3_client("dynamodb", cfg)
    resp = client.describe_table(TableName=table)
    table_info = resp.get("Table") or {}
    columns: list[str] = []
    types: dict[str, str] = {}

    for key in table_info.get("KeySchema") or []:
        name = key.get("AttributeName")
        if name:
            columns.append(name)
            key_type = key.get("KeyType", "")
            types[name] = "TEXT" if key_type == "HASH" else "TEXT"

    attr_defs = {
        a.get("AttributeName"): a.get("AttributeType", "S")
        for a in (table_info.get("AttributeDefinitions") or [])
        if a.get("AttributeName")
    }
    for name, attr_type in attr_defs.items():
        if name not in columns:
            columns.append(name)
        types[name] = _ddb_attr_type(attr_type)

    if len(columns) < 3:
        sample, _ = read_table_batch(cfg=cfg, table=table, limit=10)
        for header in sample.headers:
            if header not in columns:
                columns.append(header)
                types.setdefault(header, "TEXT")

    return columns, types


def _ddb_attr_type(attr_type: str) -> str:
    mapping = {"S": "TEXT", "N": "DECIMAL", "B": "TEXT"}
    return mapping.get((attr_type or "S").upper(), "TEXT")


def _deserialize(value: Any) -> Any:
    """Recursively deserialize DynamoDB set/list/map values while preserving Decimal."""
    if isinstance(value, set):
        return sorted(_deserialize(v) for v in value)
    if isinstance(value, dict):
        return {k: _deserialize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_deserialize(v) for v in value]
    return value


def _item_to_record(item: dict) -> dict[str, Any]:
    from boto3.dynamodb.types import TypeDeserializer

    deser = TypeDeserializer()
    return {k: _deserialize(deser.deserialize(v)) for k, v in item.items()}


def _cell(value: Any) -> str:
    return cell_to_string(value)


def estimate_item_count(cfg: dict[str, Any], table: str) -> int:
    """Approximate row count from DescribeTable (updated ~6h)."""
    client = boto3_client("dynamodb", cfg)
    resp = client.describe_table(TableName=table)
    return int(resp.get("Table", {}).get("ItemCount", 0) or 0)


def read_table_batch(
    *,
    cfg: dict[str, Any],
    table: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 500,
    exclusive_start_key: dict | None = None,
    total_rows: int | None = None,
) -> tuple[ReadBatch, dict | None]:
    client = boto3_client("dynamodb", cfg)
    scan_kwargs: dict[str, Any] = {"TableName": table, "Limit": limit}
    if exclusive_start_key:
        scan_kwargs["ExclusiveStartKey"] = exclusive_start_key

    resp = client.scan(**scan_kwargs)
    items = resp.get("Items") or []
    records = [_item_to_record(it) for it in items]

    if columns:
        headers = columns
    else:
        keys: set[str] = set()
        for rec in records:
            keys.update(rec.keys())
        headers = sorted(keys)

    rows = [[_cell(r.get(h)) for h in headers] for r in records]
    if total_rows is None:
        try:
            total_rows = estimate_item_count(cfg, table)
        except Exception:
            total_rows = offset + len(rows)
    next_key = resp.get("LastEvaluatedKey")
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total_rows), next_key


def read_all_paginated(cfg: dict[str, Any], table: str, limit: int = 100_000) -> ReadBatch:
    """Full table read up to limit (for non-streaming path)."""
    all_rows: list[list[str]] = []
    headers: list[str] = []
    next_key = None
    offset = 0
    total_estimate: int | None = None
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
        if batch.headers and not headers:
            headers = batch.headers
        all_rows.extend(batch.rows)
        offset += len(batch.rows)
        if not next_key or not batch.rows:
            break
    return ReadBatch(
        headers=headers,
        rows=all_rows,
        offset=0,
        total_rows=max(len(all_rows), total_estimate or len(all_rows)),
    )
