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
    refuse_empty_object_replace,
    resolve_object_store_write_dest_types,
    resolve_object_write_layout,
)
from connectors.object_store_incremental import (
    ObjectMergeRefused,
    prepare_incremental_export,
)
from connectors.object_store_leftover import object_append_backup_ops
from connectors.object_store_materialize import (
    materialize_object_store_export,
    resolve_materialize_batch,
    source_from_writer,
)
from connectors.object_store_multipart import (
    land_object_store_export,
    resolve_multipart_limits,
    resolve_spill_max,
)
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    _coerced_null_row_count,
    resolve_target_columns,
    transform_error_policy,
)

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))



@dataclass
class WriteResult(_WriteResult):
    driver: str = "google-cloud-storage"


def _gcs_key_absent(bucket_obj, key: str) -> bool:
    try:
        return not bool(bucket_obj.blob(key).exists())
    except Exception:
        return False


def _delete_gcs_key(bucket_obj, key: str) -> None:
    try:
        bucket_obj.blob(key).delete()
    except Exception:
        return


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
    protect_landed = bool(_kwargs.pop("protect_landed_object", False))
    allow_empty_replace = bool(_kwargs.pop("allow_empty_replace", False))
    conflict_columns = list(_kwargs.pop("conflict_columns", None) or [])
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
            sync_mode="" if protect_landed else sync_mode,
            file_batch_idx=file_batch_idx,
            total_chunks=total_chunks,
            job_id=str(_kwargs.pop("job_id", "") or ""),
            file_format=str(_kwargs.pop("file_format", "") or ""),
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
    # A new object is create-new. Leaving table_exists unset dropped every
    # pending_dest_schema mapping and the parquet file was 0 columns (DEF-C-043).
    target_cols, logical_types = resolve_target_columns(
        mappings, column_types, preserve_case=True, table_exists=False
    )
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
    extra = _kwargs.get("dest_extra") if isinstance(_kwargs.get("dest_extra"), dict) else {}
    try:
        mat = materialize_object_store_export(
            key=key,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            dest_types=dest_types,
            error_policy=policy,
            dest_kind="gcs",
            dialect_label="GCS",
            spill_max_size=resolve_spill_max(extra),
            batch_size=resolve_materialize_batch(extra),
            dest_db_type="gcs",
            empty_cells_as_null=bool(_kwargs.get("empty_cells_as_null")),
            **source_from_writer(_kwargs, extra),
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
        )
    errors = mat.transform_errors
    rejected_details = mat.rejected_details
    if mat.abort_error:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=key,
            target_schema=database,
            checksum="",
            chunks_completed=0,
            error=mat.abort_error or f"Transform errors: {'; '.join(errors[:3])}",
            warnings=errors[:10],
            rejected_rows=mat.rejected_rows,
            rejected_details=list(rejected_details),
        )
    export = mat.export
    written = mat.rows_written

    try:
        client = gcs_client(cfg)
        from connectors.gcs_common import gcs_emulator_kwargs

        probe_kw = gcs_emulator_kwargs(cfg)
        bucket_obj = client.bucket(bucket)
        try:
            if not bucket_obj.exists(**probe_kw):
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
        created_here = _gcs_key_absent(bucket_obj, key)
        blocked = refuse_empty_object_replace(
            rows_written=written,
            live_exists=not created_here,
            sync_mode=sync_mode,
            key=key,
            allow_empty_replace=allow_empty_replace,
            protect_landed_object=protect_landed,
        )
        if blocked:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=key,
                target_schema=bucket,
                checksum="",
                chunks_completed=0,
                error=blocked,
                warnings=errors[:10],
                rejected_rows=mat.rejected_rows,
                rejected_details=list(rejected_details),
            )
        try:
            # Same D116 rule as S3: clear any stale pre-append image, then
            # snapshot the prior object before the merged file lands.
            _backup_key, _backup_put, _backup_clear = object_append_backup_ops(
                "gcs", cfg, bucket, key, content_type=export.content_type,
                client=bucket_obj,
            )
            _backup_clear()
            export = prepare_incremental_export(
                key=key,
                export=export,
                fetch=lambda: bucket_obj.blob(key).download_as_bytes(),
                sync_mode=sync_mode,
                total_chunks=total_chunks,
                protect_landed=protect_landed,
                created_here=created_here,
                target_cols=target_cols,
                dest_types=dest_types,
                conflict_columns=conflict_columns,
                spill_max_size=resolve_spill_max(extra),
                backup_put=_backup_put,
            )
        except ObjectMergeRefused as exc:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=key,
                target_schema=bucket,
                checksum="",
                chunks_completed=0,
                error=str(exc),
                warnings=errors[:10],
                rejected_rows=mat.rejected_rows,
                rejected_details=list(rejected_details),
            )
        staging_key = object_staging_key(key)
        threshold, part_size = resolve_multipart_limits(extra)
        land_object_store_export(
            "gcs",
            export=export,
            staging_key=staging_key,
            live_key=key,
            bucket_obj=bucket_obj,
            content_type=export.content_type,
            threshold=threshold,
            part_size=part_size,
        )
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
        checksum = mat.checksum
        if on_checkpoint:
            on_checkpoint(1, 1, written)
        warn_out = (errors[:10] + purge_warnings)[:20]
        from connectors.writer_common import reject_on_strict_policy

        _final_abort = reject_on_strict_policy(policy, rejected_details, "GCS")
        if _final_abort:
            if created_here and not protect_landed:
                _delete_gcs_key(bucket_obj, key)
                _delete_gcs_key(bucket_obj, staging_key)
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
            meta=mat.meta,
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=key, target_schema=bucket,
            checksum="", chunks_completed=0, error=str(exc),
            rejected_details=list(rejected_details) if "rejected_details" in locals() else [],
            rejected_rows=len(rejected_details) if "rejected_details" in locals() else 0,
        )
    finally:
        export.close()
