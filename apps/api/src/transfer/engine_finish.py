"""Run completion: success status, verified finish and overwrite-backup settlement (re-exported by src.transfer.engine)."""

from __future__ import annotations

from .models import EndpointConfig
from .models import TransferResult
from typing import Any
import logging

logger = logging.getLogger("src.transfer.engine")


def _checkpoint_has_progress(checkpoint: Any) -> bool:
    """True when the checkpoint has durable resume tokens (parity with Module 14).

    Must match ``job_has_durable_progress`` / ``evaluate_resume_safety`` — cursor /
    file_offset alone are enough to resume. Narrow row-only checks wiped those
    tokens on reclaim and restarted append from zero (silent duplicates).
    """
    if not checkpoint:
        return False
    try:
        return bool(
            int(getattr(checkpoint, "chunk_index", 0) or 0) > 0
            or int(getattr(checkpoint, "offset", 0) or 0) > 0
            or int(getattr(checkpoint, "rows_processed", 0) or 0) > 0
            or int(getattr(checkpoint, "file_offset", 0) or 0) > 0
            or getattr(checkpoint, "cursor_value", None) is not None
            or getattr(checkpoint, "dynamodb_cursor", None)
            or getattr(checkpoint, "kafka_cursor", None)
            or getattr(checkpoint, "es_search_after", None) is not None
        )
    except (TypeError, ValueError):
        return False


def _raise_if_job_cancelled(
    mongo: Any, job_id: str, *, rows_written: int | None = None
) -> None:
    """Stop the writer when the operator or a lost lease asked it to stop.

    Progress writes that the cancel fence refuses are the same signal: the
    loop used to ignore a False return and keep inserting. ``rows_written``
    is whatever this chunk already committed, so the cancelled job does not
    report 0 rows for a table that now holds them.
    """
    from services.cancel_check import raise_if_job_cancelled

    raise_if_job_cancelled(mongo, job_id, rows_written=rows_written)


def _progress_write_or_cancel(mongo: Any, job_id: str, **update: Any) -> None:
    from src.transfer.engine import (  # lazy: honours monkeypatches on the original module
        _raise_if_job_cancelled,
    )
    accepted = mongo.update_job_status(job_id, "running", **update)
    if accepted is False:
        _raise_if_job_cancelled(
            mongo,
            job_id,
            rows_written=update.get("records_processed"),
        )


def _write_success_status(mongo: Any, job_id: str, status: str, **fields: Any) -> Any:
    """Write the success terminal status, or record the commit on a cancelled job.

    Cancel is sticky, so a success write landing after Cancel is refused —
    but by then the rows are committed. Leaving the cancelled job at its
    last heartbeat hid them (QA MXD10).
    """
    from services.job_status import cancel_outcome_for

    written = mongo.update_job_status(job_id, status, **fields)
    if written is not False:
        return written
    try:
        job = mongo.get_job(job_id) or {}
    except Exception as exc:  # noqa: BLE001 - the refusal itself is returned
        logger.error(
            "Job %s: success write refused and the job could not be read: %s",
            job_id,
            exc,
            exc_info=exc,
        )
        return written
    if not (job.get("cancel_requested") or str(job.get("status") or "") == "cancelled"):
        return written
    outcome = cancel_outcome_for(fields.get("records_processed"))
    logger.warning(
        "Job %s: cancel landed after %s row(s) committed; recording them on the cancelled job",
        job_id,
        outcome["rows_committed"],
    )
    kept = {
        key: fields[key]
        for key in (
            "records_processed",
            "rejected_rows",
            "coerced_null_rows",
            "rejected_details",
            "destination_summary",
            "reconciliation",
            "destination_database",
            "destination_collection",
        )
        if key in fields
    }
    mongo.update_job_status(
        job_id,
        "cancelled",
        phase="cancelled",
        message=outcome["message"],
        cancel_outcome=outcome,
        **kept,
    )
    return written


def _finish_verified_run(
    mongo: Any,
    job_id: str,
    exc: BaseException,
    *,
    request: Any,
    rows_written: int,
    dest_summary: dict[str, Any] | None,
    recon: dict[str, Any] | None,
    total_rows: int | None = None,
) -> TransferResult:
    """A step after Gate-8 proved the load raised. The run still succeeded.

    QA MX2-15: 120,000 rows landed with matching checksums and the job read
    *failed*, because bookkeeping after the proof (status write under a slow
    control plane, lineage emit, contract finalize, slot release) shared the
    ``try`` whose handler fails the job — and, for MySQL/Mongo rename-aside
    overwrite, *restored the previous table* over the verified rows.

    Here the backup is discarded (the new rows are proven), the terminal
    status is written with bounded retry, and the failing step is recorded as
    a ``post_commit_warning`` — visible, never a status flip.
    """
    from src.transfer.engine import (  # lazy: honours monkeypatches on the original module
        _TERMINAL_WRITE_ATTEMPTS,
        _settle_overwrite_backup,
        _write_success_status,
    )
    import time as _time
    from datetime import datetime, timezone

    from services.job_status import terminal_status_for

    summary = dict(dest_summary or {})
    warning = {
        "step": "post_commit",
        "error": f"{type(exc).__name__}: {exc}"[:500],
        "at": datetime.now(timezone.utc).isoformat(),
    }
    summary["post_commit_warnings"] = list(summary.get("post_commit_warnings") or []) + [
        warning
    ]
    logger.error(
        "Job %s: post-commit step failed after Gate-8 passed; the load stands: %s",
        job_id,
        exc,
        exc_info=exc,
    )
    _settle_overwrite_backup(request.destination, restore=False)
    terminal = terminal_status_for(
        summary.get("rejected_rows", 0), summary.get("coerced_null_rows", 0)
    )
    message = (recon or {}).get(
        "message", f"Transferred {rows_written:,} rows successfully"
    )
    for attempt in range(_TERMINAL_WRITE_ATTEMPTS):
        try:
            _write_success_status(
                mongo,
                job_id,
                terminal,
                records_processed=rows_written,
                progress_pct=100,
                phase="completed",
                message=message,
                # Gate-8 already passed on this path — the verified 100, same
                # as the normal success sites (M1).
                reconcile_pct=100,
                rejected_rows=int(summary.get("rejected_rows", 0) or 0),
                coerced_null_rows=int(summary.get("coerced_null_rows", 0) or 0),
                destination_summary=summary,
                reconciliation=recon or {},
                post_commit_warnings=summary["post_commit_warnings"],
            )
            break
        except Exception as write_exc:  # noqa: BLE001
            logger.warning(
                "Job %s terminal status write attempt %s failed: %s",
                job_id,
                attempt + 1,
                write_exc,
            )
            if attempt + 1 < _TERMINAL_WRITE_ATTEMPTS:
                _time.sleep(min(8.0, 0.5 * (2**attempt)))
    return TransferResult(
        success=True,
        job_id=job_id,
        records_transferred=rows_written,
        operation=request.operation,
        source_summary={"rows": total_rows} if total_rows is not None else {},
        destination_summary=summary,
        reconciliation=recon or {},
    )


def _settle_backup_after_error(destination: EndpointConfig, exc: BaseException) -> bool:
    """Settle the pre-run backup after a run ended on ``exc``.

    D134 — a cancel is not a failed batch (D102): the committed prefix stays,
    the job reports it, and the checkpoint and route watermark already point
    past it. Restoring the pre-run backup removed those rows behind the
    resume, which then skipped them as landed and still verified.
    """
    from src.transfer.engine import (  # lazy: honours monkeypatches on the original module
        TransferCancelled,
        _settle_overwrite_backup,
    )
    return _settle_overwrite_backup(
        destination, restore=not isinstance(exc, TransferCancelled)
    )


def _settle_overwrite_backup(destination: EndpointConfig, *, restore: bool) -> bool:
    """Drop a rename-aside backup after success, or put it back after failure.

    MySQL and Mongo leave the previous object under ``__df_bak``. A failed
    load renames that backup back onto the live name, so a refused overwrite
    does not publish an empty table. Other relational engines empty in place
    inside a transaction the writer commits once.
    """
    extra = dict(getattr(destination, "extra", None) or {})
    # Routed replace-mode runs rename aside each occupied route table
    # (``<t>__dfbak_<job>``) inside the write transaction. A failed post-write
    # proof restores every route table from its backup; a successful run drops
    # the backups. Recorded on ``route_backups`` by the stream.
    route_backups = dict(extra.get("route_backups") or {})
    if route_backups:
        ok = False
        try:
            from .adapters import resolve_connector_config
            from connectors.table_manager import (
                discard_pg_route_backups,
                restore_pg_route_backups,
            )

            cfg = resolve_connector_config(destination)
            if restore:
                restored = restore_pg_route_backups(cfg, route_backups)
                ok = len(restored) == len(route_backups)
            else:
                discard_pg_route_backups(cfg, route_backups)
                ok = True
        except Exception as exc:  # noqa: BLE001 - backup settle must not hide the transfer result
            logger.error(
                "Route-table backup %s failed for %s: %s",
                "restore" if restore else "discard",
                sorted(route_backups),
                exc,
                exc_info=exc,
            )
            ok = False
        finally:
            extra.pop("route_backups", None)
            destination.extra = extra
        return ok
    backup = extra.get("overwrite_backup")
    engine_name = str(extra.get("overwrite_backup_engine") or "")
    if not backup or engine_name not in {"mysql", "mongodb"}:
        return False
    from .adapters import resolve_connector_config, resolve_dest_table
    from .connector_capabilities import resolve_driver_type

    ok = False
    try:
        db_type = resolve_driver_type(destination.format)
        cfg = resolve_connector_config(destination)
        table_name = resolve_dest_table(db_type, destination)
        if engine_name == "mongodb":
            from connectors.table_manager import (
                discard_mongodb_overwrite,
                restore_mongodb_overwrite,
            )

            if restore:
                restore_mongodb_overwrite(cfg, table_name, str(backup))
            else:
                discard_mongodb_overwrite(cfg, str(backup))
        elif restore:
            from connectors.table_manager import restore_mysql_overwrite

            restore_mysql_overwrite(cfg, table_name, str(backup))
        else:
            from connectors.table_manager import discard_mysql_overwrite

            discard_mysql_overwrite(cfg, str(backup))
        ok = True
    except Exception as exc:  # noqa: BLE001 - backup settle must not hide the transfer result
        logger.error(
            "Overwrite backup %s failed for %s: %s",
            "restore" if restore else "discard",
            backup,
            exc,
            exc_info=exc,
        )
        ok = False
    finally:
        extra.pop("overwrite_backup", None)
        extra.pop("overwrite_backup_engine", None)
        destination.extra = extra
    return ok
