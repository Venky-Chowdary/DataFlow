"""Azure Blob Storage / ADLS Gen2 object writer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from connectors.adls_common import blob_service_client
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
from connectors.writer_common import (
    WriteResult as _WriteResult,
)
from connectors.writer_common import (
    _coerced_null_row_count,
    resolve_target_columns,
    transform_error_policy,
)


@dataclass
class WriteResult(_WriteResult):
    driver: str = "azure-storage-blob"


def _adls_key_absent(client, container: str, key: str) -> bool:
    try:
        return not bool(client.get_blob_client(container, key).exists())
    except Exception:
        return False


def _delete_adls_key(client, container: str, key: str) -> None:
    try:
        client.get_blob_client(container, key).delete_blob()
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
    service_account: str = "",
    **_kwargs: Any,
) -> WriteResult:
    del warehouse, backfill_new_fields
    sync_mode = str(_kwargs.pop("sync_mode", "") or "")
    protect_landed = bool(_kwargs.pop("protect_landed_object", False))
    allow_empty_replace = bool(_kwargs.pop("allow_empty_replace", False))
    conflict_columns = list(_kwargs.pop("conflict_columns", None) or [])
    file_batch_idx = int(_kwargs.pop("file_batch_idx", 0) or 0)
    total_chunks = int(_kwargs.pop("total_chunks", 1) or 1)
    container = database
    if not container:
        return WriteResult(
            ok=False, rows_written=0, table_name=table_name, target_schema="",
            checksum="", chunks_completed=0,
            error="Azure container is required (set the Database field).",
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
            ok=False, rows_written=0, table_name=table_name, target_schema=container,
            checksum="", chunks_completed=0, error=str(exc),
        )
    key = layout.write_key

    cfg = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "database": container,
        "service_account": service_account,
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
            target_schema=container,
            checksum="",
            chunks_completed=0,
            error=cov_err,
        )
    policy = transform_error_policy(error_policy)
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
            dest_kind="adls",
            dialect_label="ADLS",
            spill_max_size=resolve_spill_max(extra),
            batch_size=resolve_materialize_batch(extra),
            dest_db_type="adls",
            empty_cells_as_null=bool(_kwargs.get("empty_cells_as_null")),
            **source_from_writer(_kwargs, extra),
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=key,
            target_schema=container,
            checksum="",
            chunks_completed=0,
            error=f"ADLS serialize failed: {exc}",
        )
    errors = mat.transform_errors
    rejected_details = mat.rejected_details
    if mat.abort_error:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=key,
            target_schema=container,
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
        client = blob_service_client(cfg)
        container_client = client.get_container_client(container)
        if not container_client.exists():
            if not create_table:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=key,
                    target_schema=container,
                    checksum="",
                    chunks_completed=0,
                    error=f"ADLS container {container!r} is missing and create_table is disabled",
                )
            container_client.create_container()
        created_here = _adls_key_absent(client, container, key)
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
                target_schema=container,
                checksum="",
                chunks_completed=0,
                error=blocked,
                warnings=errors[:10],
                rejected_rows=mat.rejected_rows,
                rejected_details=list(rejected_details),
            )
        try:
            # Same D116 rule as S3/GCS: clear any stale pre-append image,
            # then snapshot the prior object before the merged file lands.
            _backup_key, _backup_put, _backup_clear = object_append_backup_ops(
                "adls", cfg, container, key, content_type=export.content_type,
                client=client,
            )
            _backup_clear()
            export = prepare_incremental_export(
                key=key,
                export=export,
                fetch=lambda: client.get_blob_client(container, key).download_blob().readall(),
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
                target_schema=container,
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
            "adls",
            export=export,
            staging_key=staging_key,
            live_key=key,
            blob_client_factory=lambda k, _c=client, _n=container: _c.get_blob_client(_n, k),
            content_type=export.content_type,
            threshold=threshold,
            part_size=part_size,
        )
        staging_blob = client.get_blob_client(container, staging_key)
        try:
            staging_blob.delete_blob()
        except Exception:
            pass
        # Purge after promote is best-effort — never fail a committed live write.
        purge_warnings: list[str] = []
        if layout.should_purge:
            from connectors.adls_reader import list_objects

            def _delete_adls(k: str) -> None:
                client.get_blob_client(container, k).delete_blob()

            try:
                purge_object_store_parts(
                    list_keys=lambda prefix: list_objects(cfg, container, prefix),
                    delete_key=_delete_adls,
                    parts_prefix=layout.purge_prefix,
                    legacy_base_key=layout.purge_legacy_key,
                    keep_part_count=layout.keep_part_count,
                    keep_keys=[key, staging_key],
                )
            except Exception as purge_exc:
                purge_warnings.append(
                    f"ADLS post-promote purge deferred (write committed): {purge_exc}"
                )
        checksum = mat.checksum
        if on_checkpoint:
            on_checkpoint(1, 1, written)
        warn_out = (errors[:10] + purge_warnings)[:20]
        from connectors.writer_common import reject_on_strict_policy

        _final_abort = reject_on_strict_policy(policy, rejected_details, "ADLS")
        if _final_abort:
            if created_here and not protect_landed:
                _delete_adls_key(client, container, key)
                _delete_adls_key(client, container, staging_key)
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=key,
                target_schema=container,
                checksum=checksum,
                chunks_completed=1,
                error=_final_abort,
                warnings=warn_out,
                rejected_rows=mat.rejected_rows,
                rejected_details=rejected_details,
            )
        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=key,
            target_schema=container,
            checksum=checksum,
            chunks_completed=1,
            warnings=warn_out,
            rejected_rows=mat.rejected_rows,
            rejected_details=rejected_details,
            coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
            meta=mat.meta,
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=key, target_schema=container,
            checksum="", chunks_completed=0, error=str(exc),
            rejected_details=rejected_details if "rejected_details" in locals() else [],
        )
    finally:
        export.close()
