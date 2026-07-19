"""S3 object writer — upload JSON/JSONL/CSV exports."""

from __future__ import annotations

import csv
import io
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from connectors.aws_common import boto3_client, is_local_endpoint, resolve_region
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    build_mapped_rows_with_details,
    resolve_target_columns,
    row_checksum,
    to_json_value,
    transform_error_policy,
)

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string, json_default


@dataclass
class WriteResult(_WriteResult):
    driver: str = "boto3"


def _ensure_bucket(client, bucket: str, cfg: dict[str, Any]) -> None:
    """Create the S3 bucket if it does not already exist."""
    try:
        client.head_bucket(Bucket=bucket)
        return
    except Exception:
        pass
    try:
        if is_local_endpoint(cfg):
            client.create_bucket(Bucket=bucket)
        else:
            region = resolve_region(cfg)
            if region and region != "us-east-1":
                client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": region},
                )
            else:
                client.create_bucket(Bucket=bucket)
    except Exception as exc:
        error_code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        if error_code not in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
            raise


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
    path_style: bool = False,
    **_kwargs: Any,
) -> WriteResult:
    del create_table, backfill_new_fields
    policy = transform_error_policy(error_policy)
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
        "endpoint_url": endpoint_url,
        "path_style": path_style,
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
        error_policy=policy,
        preserve_case=True,
    )

    records = [{c: to_json_value(v, c, dest_types) for c, v in zip(target_cols, row)} for row in mapped_rows]

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
        client = boto3_client("s3", cfg)
        _ensure_bucket(client, bucket, cfg)
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
        checksum = row_checksum(mapped_rows, target_cols)
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
            rejected_rows=len({d["row"] for d in rejected_details}) or max(0, len(data_rows) - len(mapped_rows)),
            rejected_details=rejected_details[:200],
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=key, target_schema=bucket,
            checksum="", chunks_completed=0, error=str(exc),
            rejected_details=rejected_details[:200],
        )
