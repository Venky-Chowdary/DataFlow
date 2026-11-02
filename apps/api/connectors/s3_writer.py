"""S3 object writer — upload JSON/JSONL/CSV/Parquet exports."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from connectors.aws_common import boto3_client, is_local_endpoint, resolve_region
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
    driver: str = "boto3"


def _s3_key_absent(client, bucket: str, key: str) -> bool:
    """True only on a proven 404. Access denied stays False so we do not delete."""
    from botocore.exceptions import ClientError

    try:
        client.head_object(Bucket=bucket, Key=key)
        return False
    except ClientError as exc:
        code = str((exc.response or {}).get("Error", {}).get("Code") or "")
        return code in {"404", "NoSuchKey", "NotFound"}
    except Exception:
        return False


def _delete_s3_key(client, bucket: str, key: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return


def _ensure_bucket(client, bucket: str, cfg: dict[str, Any]) -> None:
    """Create the S3 bucket if it does not already exist.

    Auth / network failures must not be treated as "bucket missing" — that would
    hide the real operator action behind a false create attempt.
    """
    from botocore.exceptions import ClientError

    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError as exc:
        code = str((exc.response or {}).get("Error", {}).get("Code") or "")
        http = str((exc.response or {}).get("ResponseMetadata", {}).get("HTTPStatusCode") or "")
        if code not in {"404", "NoSuchBucket", "NotFound"} and http != "404":
            raise RuntimeError(
                f"Cannot verify S3 bucket {bucket!r}: {code or http or exc}"
            ) from exc
    except Exception as exc:
        raise RuntimeError(f"Cannot verify S3 bucket {bucket!r}: {exc}") from exc
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
    del backfill_new_fields
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
            error="S3 bucket is required (set the Database field).",
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
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "ssl": ssl,
        "database": database,
        "endpoint_url": endpoint_url,
        "path_style": path_style,
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
            dest_kind="s3",
            dialect_label="S3",
            spill_max_size=resolve_spill_max(extra),
            batch_size=resolve_materialize_batch(extra),
            dest_db_type="s3",
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
            error=f"S3 serialize failed: {exc}",
        )
    errors = mat.transform_errors
    rejected_details = mat.rejected_details
    if mat.abort_error:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=key,
            target_schema=bucket,
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
        client = boto3_client("s3", cfg)
        if create_table:
            _ensure_bucket(client, bucket, cfg)
        else:
            try:
                client.head_bucket(Bucket=bucket)
            except Exception as exc:
                raise RuntimeError(
                    f"S3 bucket {bucket!r} is missing or inaccessible and create_table is disabled"
                ) from exc
        # A key this run creates is deleted if strict policy aborts after promote.
        # An object that already existed is left in place: the promote already
        # replaced it, and deleting it would drop the previous generation too.
        created_here = _s3_key_absent(client, bucket, key)
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
            # A stale pre-append image must die on every write: an overwrite
            # after an append would otherwise leave a backup the undo could
            # wrongly restore over the newer content (D116).
            _backup_key, _backup_put, _backup_clear = object_append_backup_ops(
                "s3", cfg, bucket, key, content_type=export.content_type,
                client=client,
            )
            _backup_clear()
            export = prepare_incremental_export(
                key=key,
                export=export,
                fetch=lambda: client.get_object(Bucket=bucket, Key=key)["Body"].read(),
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
        # Staging is verified and copied onto the live key. A failed promote
        # deletes staging and does not replace an object that already landed.
        staging_key = object_staging_key(key)
        threshold, part_size = resolve_multipart_limits(extra)
        land_object_store_export(
            "s3",
            export=export,
            staging_key=staging_key,
            live_key=key,
            client=client,
            bucket=bucket,
            content_type=export.content_type,
            threshold=threshold,
            part_size=part_size,
        )
        try:
            client.delete_object(Bucket=bucket, Key=staging_key)
        except Exception:
            pass
        # Full-refresh overwrite: clear stale parts once on the *last* chunk after
        # promote. Purge must never fail-closed the committed write (Bugbot) —
        # live object already holds the payload; orphan cleanup is best-effort.
        purge_warnings: list[str] = []
        if layout.should_purge:
            from connectors.s3_reader import list_objects

            try:
                purge_object_store_parts(
                    list_keys=lambda prefix: list_objects(cfg, bucket, prefix),
                    delete_key=lambda k: client.delete_object(Bucket=bucket, Key=k),
                    parts_prefix=layout.purge_prefix,
                    legacy_base_key=layout.purge_legacy_key,
                    keep_part_count=layout.keep_part_count,
                    keep_keys=[key, staging_key],
                )
            except Exception as purge_exc:
                purge_warnings.append(
                    f"S3 post-promote purge deferred (write committed): {purge_exc}"
                )
        checksum = mat.checksum
        if on_checkpoint:
            on_checkpoint(1, 1, written)
        warn_out = (errors[:10] + purge_warnings)[:20]
        from connectors.writer_common import reject_on_strict_policy

        _final_abort = reject_on_strict_policy(policy, rejected_details, "S3")
        if _final_abort:
            # A key this run created can be removed. A key that already held
            # rows, and any quarantine replay, is never deleted (R11-001).
            if created_here and not protect_landed:
                _delete_s3_key(client, bucket, key)
                _delete_s3_key(client, bucket, staging_key)
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=key,
                target_schema=bucket,
                checksum=checksum,
                chunks_completed=1,
                error=_final_abort,
                warnings=warn_out,
                rejected_rows=len({d["row"] for d in rejected_details}) or max(0, len(data_rows) - written),
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
            rejected_rows=len({d["row"] for d in rejected_details}) or max(0, len(data_rows) - written),
            rejected_details=list(rejected_details),
            coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
            meta=mat.meta,
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=key, target_schema=bucket,
            checksum="", chunks_completed=0, error=str(exc),
            rejected_details=list(rejected_details),
        )
    finally:
        export.close()
