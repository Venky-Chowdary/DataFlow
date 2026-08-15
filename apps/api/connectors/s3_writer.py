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
    driver: str = "boto3"


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
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "ssl": ssl,
        "database": database,
        "endpoint_url": endpoint_url,
        "path_style": path_style,
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
        error_policy=policy,
        preserve_case=True,
        dest_kind="s3",
        destination_pk_columns=None,
    )
    # Object-store exports still honor typed carriers from Map (DECIMAL/BINARY/
    # VARCHAR(n)) — refuse silent invent / overflow before JSON/CSV serialize.
    tgt_types = [str(dest_types.get(c, "") or "") for c in target_cols]
    mapped_rows = apply_write_quarantine_matrix(
        mapped_rows, target_cols, tgt_types, rejected_details, policy, dialect_label="S3",
        mappings=mappings,
    )
    from connectors.writer_common import reject_on_strict_policy

    _map_abort = reject_on_strict_policy(policy, rejected_details, "S3", errors)
    if _map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=key,
            target_schema=bucket,
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
            error=f"S3 serialize failed: {exc}",
            rejected_details=list(rejected_details),
        )
    written = len(mapped_rows)

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
        # Staging→live before any purge: failed put must not wipe the prior export.
        staging_key = object_staging_key(key)
        extra = _kwargs.get("dest_extra") if isinstance(_kwargs.get("dest_extra"), dict) else {}
        threshold, part_size = resolve_multipart_limits(extra)
        upload_kw = dict(
            dialect="s3",
            client=client,
            bucket=bucket,
            body=body,
            content_type=content_type,
            threshold=threshold,
            part_size=part_size,
        )
        upload_object_store_bytes(key=staging_key, **upload_kw)
        upload_object_store_bytes(key=key, **upload_kw)
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
        checksum = row_checksum(mapped_rows, target_cols, dest_db_type="s3")
        if on_checkpoint:
            on_checkpoint(1, 1, written)
        warn_out = (errors[:10] + purge_warnings)[:20]
        _final_abort = reject_on_strict_policy(policy, rejected_details, "S3")
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
                rejected_rows=len({d["row"] for d in rejected_details}) or max(0, len(data_rows) - len(mapped_rows)),
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
            rejected_rows=len({d["row"] for d in rejected_details}) or max(0, len(data_rows) - len(mapped_rows)),
            rejected_details=list(rejected_details),
            coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
        )
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=key, target_schema=bucket,
            checksum="", chunks_completed=0, error=str(exc),
            rejected_details=list(rejected_details),
        )
