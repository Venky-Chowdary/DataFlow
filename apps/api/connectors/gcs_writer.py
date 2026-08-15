"""GCS object writer — upload JSON/JSONL/CSV/Parquet exports."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from connectors.gcs_common import gcs_client
from connectors.object_store_common import (
    object_staging_key,
    purge_object_store_parts,
    resolve_object_store_write_dest_types,
    resolve_object_write_layout,
    serialize_object_store_body,
)
from connectors.object_store_multipart import (
    resolve_multipart_limits,
    upload_object_store_bytes,
)
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    apply_write_quarantine_matrix,
    build_mapped_rows_with_details,
    _coerced_null_row_count,
    resolve_target_columns,
    row_checksum,
    transform_error_policy,
)

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))



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
    dest_types, cov_err = resolve_object_store_write_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=logical_types,
        destination_column_types=_kwargs.get("destination_column_types"),
    )
    if cov_err:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=key,
            target_schema=bucket,
            checksum="",
            chunks_completed=0,
            error=cov_err,
        )
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

    _map_abort = reject_on_strict_policy(policy, rejected_details, "GCS", errors)
    if _map_abort:
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

    try:
        body, content_type = serialize_object_store_body(
            key=key,
            mapped_rows=mapped_rows,
            target_cols=target_cols,
            dest_types=dest_types,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=key,
            target_schema=bucket,
            checksum="",
            chunks_completed=0,
            error=f"GCS serialize failed: {exc}",
            rejected_details=list(rejected_details),
        )
    written = len(mapped_rows)

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
        # Staging→live before any purge: failed upload must not wipe the prior export.
        staging_key = object_staging_key(key)
        extra = _kwargs.get("dest_extra") if isinstance(_kwargs.get("dest_extra"), dict) else {}
        threshold, part_size = resolve_multipart_limits(extra)
        upload_kw = dict(
            dialect="gcs",
            bucket_obj=bucket_obj,
            body=body,
            content_type=content_type,
            threshold=threshold,
            part_size=part_size,
        )
        upload_object_store_bytes(key=staging_key, **upload_kw)
        upload_object_store_bytes(key=key, **upload_kw)
        try:
            bucket_obj.blob(staging_key).delete()
        except Exception:
            pass
        # Purge after promote is best-effort — never fail a committed live write.
        purge_warnings: list[str] = []
        if layout.should_purge:
            from connectors.gcs_reader import list_objects

            def _delete_gcs(k: str) -> None:
                bucket_obj.blob(k).delete()

            try:
                purge_object_store_parts(
                    list_keys=lambda prefix: list_objects(cfg, bucket, prefix),
                    delete_key=_delete_gcs,
                    parts_prefix=layout.purge_prefix,
                    legacy_base_key=layout.purge_legacy_key,
                    keep_part_count=layout.keep_part_count,
                    keep_keys=[key, staging_key],
                )
            except Exception as purge_exc:
                purge_warnings.append(
                    f"GCS post-promote purge deferred (write committed): {purge_exc}"
                )
        checksum = row_checksum(mapped_rows, target_cols, dest_db_type="gcs")
        if on_checkpoint:
            on_checkpoint(1, 1, written)
        warn_out = (errors[:10] + purge_warnings)[:20]
        _final_abort = reject_on_strict_policy(policy, rejected_details, "GCS")
        if _final_abort:
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=key,
                target_schema=bucket,
                checksum=checksum,
                chunks_completed=1,
                error=_final_abort,
                warnings=warn_out,
                rejected_rows=len({d["row"] for d in rejected_details}),
                rejected_details=list(rejected_details),
            )
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=key,
            target_schema=bucket,
            checksum=checksum,
            chunks_completed=1,
            warnings=warn_out,
            rejected_rows=len({d["row"] for d in rejected_details}),
            rejected_details=list(rejected_details),
            coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=key, target_schema=bucket,
            checksum="", chunks_completed=0, error=str(exc),
            rejected_details=list(rejected_details) if "rejected_details" in locals() else [],
            rejected_rows=len(rejected_details) if "rejected_details" in locals() else 0,
        )
