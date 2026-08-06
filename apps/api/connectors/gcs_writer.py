"""GCS object writer — upload JSON/JSONL/CSV exports."""

from __future__ import annotations

import csv
import io
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from connectors.gcs_common import gcs_client
from connectors.object_store_common import (
    purge_object_store_parts,
    resolve_object_write_layout,
)
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    apply_write_quarantine_matrix,
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
    driver: str = "google-cloud-storage"


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
    service_account: str = "",
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
    del ssl, username, backfill_new_fields
    policy = transform_error_policy(error_policy)
    sync_mode = str(_kwargs.pop("sync_mode", "") or "")
    file_batch_idx = int(_kwargs.pop("file_batch_idx", 0) or 0)
    total_chunks = int(_kwargs.pop("total_chunks", 1) or 1)
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
    try:
        layout = resolve_object_write_layout(
            table_name=table_name,
            schema=schema,
            sync_mode=sync_mode,
            file_batch_idx=file_batch_idx,
            total_chunks=total_chunks,
            job_id=str(_kwargs.pop("job_id", "") or ""),
        )
    except ValueError as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema=bucket,
            checksum="", chunks_completed=0, error=str(exc),
        )
    key = layout.write_key

    cfg = {
        "host": host,
        "port": port,
        "service_account": service_account,
        "connection_string": connection_string,
        "password": password,
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
        dest_kind="gcs",
        destination_pk_columns=None,
    )
    tgt_types = [str(dest_types.get(c, "") or "") for c in target_cols]
    mapped_rows = apply_write_quarantine_matrix(
        mapped_rows, target_cols, tgt_types, rejected_details, policy, dialect_label="GCS",
        mappings=mappings,
    )
    from connectors.writer_common import reject_on_strict_policy

    _map_abort = reject_on_strict_policy(policy, rejected_details, "GCS")
    if _map_abort or (errors and policy == "fail"):
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=key,
            target_schema=database,
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(errors[:3])}",
            warnings=errors[:10],
            rejected_rows=len({d.get("row") for d in rejected_details if d.get("row") is not None}),
            rejected_details=list(rejected_details),
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
        client = gcs_client(cfg)
        bucket_obj = client.bucket(bucket)
        try:
            if not bucket_obj.exists():
                if not create_table:
                    raise RuntimeError(
                        f"GCS bucket {bucket!r} is missing and create_table is disabled"
                    )
                bucket_obj.create()
        except RuntimeError:
            raise
        except Exception as exc:
            # Only treat true NotFound as create-trigger; auth/transient must surface.
            try:
                from google.api_core.exceptions import Forbidden, NotFound
            except ImportError:
                NotFound = type(None)  # type: ignore[misc,assignment]
                Forbidden = type(None)  # type: ignore[misc,assignment]
            if isinstance(exc, Forbidden) or "403" in str(exc) or "Forbidden" in type(exc).__name__:
                raise RuntimeError(
                    f"Cannot verify GCS bucket {bucket!r}: permission denied"
                ) from exc
            if isinstance(exc, NotFound) or "404" in str(exc) or "NotFound" in type(exc).__name__:
                if not create_table:
                    raise RuntimeError(
                        f"GCS bucket {bucket!r} is missing and create_table is disabled"
                    ) from exc
                try:
                    bucket_obj.create()
                except Exception as create_exc:
                    raise RuntimeError(
                        f"Cannot create GCS bucket {bucket!r}: {create_exc}"
                    ) from create_exc
            else:
                raise RuntimeError(
                    f"Cannot verify GCS bucket {bucket!r}: {exc}"
                ) from exc
        if layout.should_purge:
            from connectors.gcs_reader import list_objects

            def _delete_gcs(k: str) -> None:
                bucket_obj.blob(k).delete()

            purge_object_store_parts(
                list_keys=lambda prefix: list_objects(cfg, bucket, prefix),
                delete_key=_delete_gcs,
                parts_prefix=layout.purge_prefix,
                legacy_base_key=layout.purge_legacy_key,
            )
        blob = bucket_obj.blob(key)
        blob.upload_from_string(body, content_type=content_type)
        checksum = row_checksum(mapped_rows, target_cols, dest_db_type="gcs")
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
            rejected_rows=len({d["row"] for d in rejected_details}),
            rejected_details=list(rejected_details),
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=key, target_schema=bucket,
            checksum="", chunks_completed=0, error=str(exc),
            rejected_details=list(rejected_details) if "rejected_details" in locals() else [],
            rejected_rows=len(rejected_details) if "rejected_details" in locals() else 0,
        )
