"""GCS object writer — upload JSON/JSONL/CSV exports."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from typing import Any, Callable

from connectors.gcs_common import gcs_client
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
    driver: str = "google-cloud-storage"
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
    del port, ssl, create_table, error_policy, username
    bucket = database or connection_string
    key = table_name or schema or "exports/dataflow_export.json"
    if not key.endswith((".json", ".jsonl", ".csv")):
        key = f"{key.rstrip('/')}/export.json"

    cfg = {"host": host, "connection_string": connection_string or password, "password": password}
    target_cols = resolve_target_columns(mappings, headers)
    mapped_rows, errors = build_mapped_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
    )
    records = [dict(zip(target_cols, row)) for row in mapped_rows]

    if key.endswith(".csv"):
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=target_cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
        body = buf.getvalue().encode("utf-8")
        content_type = "text/csv"
    elif key.endswith(".jsonl"):
        body = "\n".join(json.dumps(r, default=str) for r in records).encode("utf-8")
        content_type = "application/x-ndjson"
    else:
        body = json.dumps(records, indent=2, default=str).encode("utf-8")
        content_type = "application/json"

    try:
        client = gcs_client(cfg)
        blob = client.bucket(bucket).blob(key)
        blob.upload_from_string(body, content_type=content_type)
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
