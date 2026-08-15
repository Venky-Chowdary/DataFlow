"""SFTP object writer — upload JSON/JSONL/CSV/Parquet exports."""

from __future__ import annotations

import logging
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from connectors.object_store_common import resolve_object_store_write_dest_types
from connectors.object_store_materialize import (
    materialize_object_store_export,
    resolve_materialize_batch,
    source_from_writer,
)
from connectors.object_store_multipart import resolve_multipart_limits, resolve_spill_max
from connectors.sftp_common import (
    connect_sftp,
    host_key_settings,
    parse_sftp_config,
    split_remote_path,
)
from connectors.writer_common import reject_on_strict_policy, WriteResult as _WriteResult
from connectors.writer_common import (
    _coerced_null_row_count,
    resolve_target_columns,
    transform_error_policy,
)

logger = logging.getLogger(__name__)

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@dataclass
class WriteResult(_WriteResult):
    driver: str = "paramiko"


def _replace_remote(sftp: Any, temp_path: str, final_path: str) -> None:
    """Move the staged upload onto the final path, atomically where possible.

    ``posix-rename@openssh.com`` replaces the target in one step, so a consumer
    polling the directory never sees a truncated file and never sees it vanish.
    Only OpenSSH-derived servers implement it: paramiko always exposes the
    client method, so testing ``hasattr`` asked the wrong side and every managed
    file-transfer appliance without the extension failed the write outright
    with "Operation unsupported" after the bytes had already landed.

    The fallback is the portable two-step. It is not atomic — there is a window
    where the destination does not exist — so it is only taken when the server
    actually refused the atomic form. Any other failure (permission, quota,
    missing directory) must propagate: retrying it as delete-then-rename would
    destroy the operator's existing file first and then fail the same way.

    paramiko maps only a few SFTP status codes onto errno (``ENOENT`` for
    missing, ``EACCES`` for denied) and raises a bare ``IOError(text)`` for
    everything else, including ``SFTP_OP_UNSUPPORTED``. So "the server will not
    do this" is identified by errno *and* message, and an unrecognised error
    with no errno stays fatal rather than being read as permission to fall back.
    """
    import errno

    try:
        sftp.posix_rename(temp_path, final_path)
        return
    except (AttributeError, OSError) as exc:
        if isinstance(exc, OSError) and not _is_unsupported_operation(exc):
            raise
        logger.info(
            "SFTP posix_rename unavailable (%s); falling back to remove+rename", exc
        )

    try:
        sftp.remove(final_path)
    except OSError as exc:
        logger.debug("No existing %s to remove before rename: %s", final_path, exc)
    sftp.rename(temp_path, final_path)


def _is_unsupported_operation(exc: OSError) -> bool:
    """True only when the server said it does not implement the request."""
    import errno

    code = getattr(exc, "errno", None)
    if code in (errno.EOPNOTSUPP, errno.ENOSYS):
        return True
    if code is not None:
        return False
    text = str(exc).lower()
    return "unsupported" in text or "not supported" in text or "not implemented" in text


def write_mapped_rows(
    *,
    connection_string: str = "",
    host: str = "",
    port: int = 0,
    database: str = "",
    username: str = "",
    password: str = "",
    schema: str = "",
    table_name: str = "",
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Any | None = None,
    **_kwargs: Any,
) -> WriteResult:
    """Upload mapped rows as a CSV/JSON/Parquet file to an SFTP server.

    Map+quarantine+serialize uses the shared object-store bundle algorithm.
    Accepted mapped_rows are not retained. Still at-least-once.
    """
    cfg = parse_sftp_config(
        connection_string=connection_string,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        table=table_name,
        private_key=str(_kwargs.get("private_key") or ""),
        **host_key_settings(_kwargs),
    )
    if not cfg.host:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="SFTP host is required. Use an sftp:// URL or the host/port fields.",
        )
    if not cfg.path:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="SFTP remote path is required. Provide it via the connection_string or database/table fields.",
        )

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
            table_name=table_name or cfg.path,
            target_schema=cfg.host or "",
            checksum="",
            chunks_completed=0,
            error=cov_err,
        )
    policy = transform_error_policy(_kwargs.get("error_policy"))
    directory, filename = split_remote_path(cfg.path)
    ext = (filename.rsplit(".", 1)[-1] if "." in filename else "").lower()
    if ext in ("csv", "jsonl", "json", "tsv", "parquet"):
        fmt = ext
    else:
        fmt = "csv"
        if not filename.endswith(".csv"):
            filename = f"{filename.rstrip('/')}.csv"
            cfg.path = f"{directory.rstrip('/')}/{filename}" if directory else f"/{filename}"
    extra = _kwargs.get("dest_extra") if isinstance(_kwargs.get("dest_extra"), dict) else {}
    spill_max = resolve_spill_max(extra)
    _, stream_chunk = resolve_multipart_limits(extra)
    if extra.get("sftp_stream_chunk"):
        stream_chunk = max(1, int(extra["sftp_stream_chunk"]))
    serialize_key = filename if filename.lower().endswith(f".{fmt}") else f"export.{fmt}"
    try:
        mat = materialize_object_store_export(
            key=serialize_key,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            dest_types=dest_types,
            error_policy=policy,
            dest_kind="sftp",
            dialect_label="SFTP",
            spill_max_size=spill_max,
            batch_size=resolve_materialize_batch(extra),
            dest_db_type="sftp",
            **source_from_writer(_kwargs, extra),
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=cfg.host,
            checksum="",
            chunks_completed=0,
            error=f"SFTP serialize failed: {exc}",
        )
    transform_errors = mat.transform_errors
    rejected_details = mat.rejected_details
    if mat.abort_error:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=cfg.host,
            checksum="",
            chunks_completed=0,
            error=mat.abort_error or f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=list(rejected_details),
            rejected_rows=mat.rejected_rows,
        )
    export = mat.export
    written = mat.rows_written
    rejected_rows = mat.rejected_rows

    try:
        transport, sftp = connect_sftp(cfg)
        try:
            if directory:
                try:
                    sftp.stat(directory)
                except Exception:
                    # Best-effort create directory chain
                    parts = [p for p in directory.split("/") if p]
                    current = ""
                    for part in parts:
                        current += f"/{part}"
                        try:
                            sftp.stat(current)
                        except Exception:
                            sftp.mkdir(current)
            # Atomic replace: write temp then rename so consumers never see a
            # truncated artifact after a mid-upload disconnect.
            temp_path = f"{cfg.path}.dataflow-{uuid.uuid4().hex}.tmp"
            try:
                with sftp.file(temp_path, "wb") as f:
                    export.copy_to(f, chunk_size=stream_chunk)
                    f.flush()
                _replace_remote(sftp, temp_path, cfg.path)
            except Exception:
                try:
                    sftp.remove(temp_path)
                except Exception as exc:
                    logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                raise
        finally:
            sftp.close()
            transport.close()

        if on_checkpoint:
            on_checkpoint(1, 1, written)

        _final_abort = reject_on_strict_policy(policy, rejected_details, "SFTP")
        if _final_abort:
            return WriteResult(
                ok=False,
                rows_written=written,
                table_name=filename,
                target_schema=cfg.host,
                checksum="",
                chunks_completed=1,
                error=_final_abort,
                warnings=transform_errors[:10],
                rejected_rows=rejected_rows,
                rejected_details=rejected_details,
            )

        return WriteResult(
            ok=True,
            rows_written=written,
            table_name=filename,
            target_schema=cfg.host,
            checksum=mat.checksum,
            chunks_completed=1,
            warnings=transform_errors[:10],
            rejected_rows=rejected_rows,
            rejected_details=rejected_details,
            coerced_null_rows=_coerced_null_row_count(rejected_details, policy),
            meta=mat.meta,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=filename if "filename" in locals() else table_name,
            target_schema=cfg.host,
            checksum="",
            chunks_completed=0,
            error=f"SFTP write failed: {exc}",
            rejected_details=rejected_details if "rejected_details" in locals() else [],
        )
    finally:
        export.close()
