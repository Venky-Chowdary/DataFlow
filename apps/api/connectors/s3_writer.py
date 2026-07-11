"""S3 object writer — upload JSON/JSONL/CSV exports."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
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
    del create_table, error_policy
    bucket = database
    if not bucket:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="S3 bucket is required (set the Database field).",
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
        "ssl": ssl,
        "database": database,
    }
    target_cols, _ = resolve_target_columns(mappings, column_types)
    mapped_rows, errors = build_mapped_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
    )

    def _to_json_value(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return value
            try:
                parsed = json.loads(text)
                return parsed
            except json.JSONDecodeError:
                return value
        return value

    records = [{c: _to_json_value(v) for c, v in zip(target_cols, row)} for row in mapped_rows]

    if key.endswith(".csv"):
        def _csv_cell(value: Any) -> str:
            if value is None:
                return ""
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            return str(value)

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=target_cols, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow({k: _csv_cell(v) for k, v in record.items()})
        body = buf.getvalue().encode("utf-8")
        content_type = "text/csv"
    elif key.endswith(".jsonl"):
        body = "\n".join(json.dumps(r, default=str, ensure_ascii=False) for r in records).encode("utf-8")
        content_type = "application/x-ndjson"
    else:
        body = json.dumps(records, indent=2, default=str, ensure_ascii=False).encode("utf-8")
        content_type = "application/json"

    try:
        client = boto3_client("s3", cfg)
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
        checksum = row_checksum(mapped_rows)
        if on_checkpoint:
            on_checkpoint(1, 1, len(records))
        return WriteResult(
            ok=True,
            rows_written=len(records),
            table_name=key,
            target_schema=bucket,
            checksum=checksum,
            chunks_completed=1,
            warnings=errors[:10],
            rejected_rows=len(errors),
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=key, target_schema=bucket,
            checksum="", chunks_completed=0, error=str(exc),
        )
