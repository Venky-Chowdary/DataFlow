"""GCS object writer — upload JSON/JSONL/CSV exports."""

from __future__ import annotations

import csv
import io
import json
import sys
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from connectors.gcs_common import gcs_client
from connectors.writer_common import build_mapped_rows, resolve_target_columns, row_checksum

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

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
    backfill_new_fields: bool = False,
    **_kwargs: Any,
) -> WriteResult:
    del ssl, create_table, error_policy, username, backfill_new_fields
    bucket = database
    if not bucket:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="GCS bucket is required (set the Database field).",
        )
    key = table_name or schema or "exports/dataflow_export.json"
    if not key.endswith((".json", ".jsonl", ".csv")):
        key = f"{key.rstrip('/')}/export.json"

    cfg = {
        "host": host,
        "port": port,
        "connection_string": connection_string,
        "password": password,
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

    def _to_json_value(value: Any, col: str) -> Any:
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
            # Structural types are parsed as JSON objects/arrays.
            if ctype in {"json", "array", "object", "struct"}:
                try:
                    return json.loads(text, parse_float=Decimal, parse_constant=lambda v: None)
                except json.JSONDecodeError:
                    return value
            # Text, dates, UUID, binary and other non-numeric types must stay as strings.
            if ctype in {"text", "string", "varchar", "uuid", "binary", "date", "datetime", "time"}:
                return value
            # Numeric / boolean types: allow JSON scalar parsing where valid.
            try:
                return json.loads(text, parse_float=Decimal, parse_constant=lambda v: None)
            except json.JSONDecodeError:
                return value
        return value

    records = [{c: _to_json_value(v, c) for c, v in zip(target_cols, row)} for row in mapped_rows]

    if key.endswith(".csv"):
        def _csv_cell(value: Any) -> str:
            return cell_to_string(value)

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=target_cols, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow({k: _csv_cell(v) for k, v in record.items()})
        body = buf.getvalue().encode("utf-8")
        content_type = "text/csv"
    elif key.endswith(".jsonl"):
        body = "\n".join(json.dumps(r, default=json_default, ensure_ascii=False, allow_nan=False) for r in records).encode("utf-8")
        content_type = "application/x-ndjson"
    else:
        body = json.dumps(records, indent=2, default=json_default, ensure_ascii=False, allow_nan=False).encode("utf-8")
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
