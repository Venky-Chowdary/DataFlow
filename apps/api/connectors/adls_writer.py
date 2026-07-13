"""Azure Blob Storage / ADLS Gen2 object writer."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

from connectors.adls_common import blob_service_client
from connectors.writer_common import (
    build_mapped_rows,
    resolve_target_columns,
    row_checksum,
    sanitize_identifier,
    transform_error_policy,
)
from services.value_serializer import cell_to_string, json_default


@dataclass
class WriteResult:
    ok: bool
    rows_written: int
    table_name: str
    target_schema: str
    checksum: str
    chunks_completed: int
    error: str | None = None
    driver: str = "azure-storage-blob"
    rejected_rows: int = 0
    warnings: list[str] = field(default_factory=list)


def _to_json_value(value: Any, col: str, column_types: dict[str, str]) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return value
        try:
            from services.type_system import normalize_logical_type
        except Exception:
            normalize_logical_type = lambda x: str(x or "").lower()
        ctype = normalize_logical_type(column_types.get(col, "")) if column_types else ""
        if ctype in {"json", "array", "object", "struct"}:
            try:
                return json.loads(text, parse_float=Decimal, parse_constant=lambda v: None)
            except json.JSONDecodeError:
                return value
        if ctype in {"text", "string", "varchar", "uuid", "binary", "date", "datetime", "time"}:
            return value
        try:
            return json.loads(text, parse_float=Decimal, parse_constant=lambda v: None)
        except json.JSONDecodeError:
            return value
    return value


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
    warehouse: str = "",
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    error_policy: str | None = None,
    backfill_new_fields: bool = False,
    create_table: bool = True,
    **_kwargs: Any,
) -> WriteResult:
    del warehouse, backfill_new_fields, create_table
    container = database
    if not container:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema="",
            checksum="", chunks_completed=0,
            error="Azure container is required (set the Database field).",
        )

    key = table_name or schema or "exports/dataflow_export.json"
    if not key.endswith((".json", ".jsonl", ".csv")):
        key = f"{key.rstrip('/')}/export.json"

    cfg = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "database": container,
    }

    target_cols, _ = resolve_target_columns(mappings, column_types, preserve_case=True)
    mapped_rows, errors = build_mapped_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        preserve_case=True,
    )

    records = [{c: _to_json_value(v, c, column_types) for c, v in zip(target_cols, row)} for row in mapped_rows]

    if key.endswith(".csv"):
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=target_cols, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow({k: cell_to_string(v) for k, v in record.items()})
        body = buf.getvalue().encode("utf-8")
        content_type = "text/csv"
    elif key.endswith(".jsonl"):
        body = "\n".join(json.dumps(r, default=json_default, ensure_ascii=False, allow_nan=False) for r in records).encode("utf-8")
        content_type = "application/x-ndjson"
    else:
        body = json.dumps(records, indent=2, default=json_default, ensure_ascii=False, allow_nan=False).encode("utf-8")
        content_type = "application/json"

    try:
        client = blob_service_client(cfg)
        container_client = client.get_container_client(container)
        if not container_client.exists():
            container_client.create_container()
        blob = client.get_blob_client(container, key)
        blob.upload_blob(body, overwrite=True, content_type=content_type)
        checksum = row_checksum(mapped_rows)
        if on_checkpoint:
            on_checkpoint(1, 1, len(records))
        return WriteResult(
            ok=True,
            rows_written=len(records),
            table_name=key,
            target_schema=container,
            checksum=checksum,
            chunks_completed=1,
            warnings=errors[:10],
            rejected_rows=len(errors),
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=key, target_schema=container,
            checksum="", chunks_completed=0, error=str(exc),
        )
