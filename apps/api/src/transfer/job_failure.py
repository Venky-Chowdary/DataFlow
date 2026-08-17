"""Failure and CDC-field stamping for transfer jobs.

Extracted from :mod:`src.transfer.engine` (Phase F8 size freeze) with no
behaviour change. One place decides what a failed run records — classification,
human message, quarantine durability, CDC lag/health promotion — so a failure
can never be reported as a partial success. ``engine`` re-exports these names.
"""

from __future__ import annotations

import logging
from typing import Any

from services import lineage_telemetry as lineage  # noqa: F401 — used by callers
from services.error_handling import TransferCancelled, classify_error

from .job_quarantine import _persist_job_quarantine

logger = logging.getLogger(__name__)

_CDC_JOB_FIELDS = (
    "cdc_lag_seconds",
    "cdc_lag_basis",
    "cdc_heartbeat_age_sec",
    "cdc_freshness_severity",
    "cdc_lag_unknown_reason",
    "replication_lag_bytes",
    "cdc_confirmed_flush_lsn",
    "cdc_restart_lsn",
    "cdc_min_lsn",
    "cdc_max_lsn",
    "cdc_max_lsn_time",
    "cdc_capture_instance",
    "cdc_capture_stall",
    "cdc_capture_stall_reason",
    "cdc_capture_stall_unknown",
    "cdc_capture_latency_seconds",
    "cdc_slot_active",
    "cdc_slot_exists",
    "cdc_wal_status",
    "cdc_heartbeat_at",
    "cdc_last_ddl_at",
    "cdc_plugin",
    "cdc_slot_name",
    "cdc_delivery",
    "exactly_once_active",
    "exactly_once_claimed_platform",
    "exactly_once_algorithm",
    "exactly_once_protocol",
    "delivery_semantics",
    "eos_committed_lsn",
    "eos_fence_epoch",
    "eos_dest_authoritative",
    "cdc_lease_holder",
    "cdc_lease_resource",
    "cdc_lease_stale",
    "cdc_lease_heartbeat_age_sec",
    "cdc_lease_backend",
    "cdc_lease_generation",
    "cdc_lease_cursor_key",
    "cdc_lease_conflict",
    "cdc_cursor_gap",
    "cdc_cursor_gap_code",
    "cdc_cursor_gap_dialect",
    "cdc_cursor_gap_resume",
    "cdc_cursor_gap_retained",
    "cdc_append_only_sink",
    "cdc_row_filter",
    "source_ha_role",
    "source_ha_topology",
    "source_ha_enabled",
    "source_ha_group",
    "source_ha_replica",
    "source_ha_open_mode",
    "source_ha_message",
    "cdc_retention_status",
    "cdc_retention_resume",
    "cdc_retention_retained",
    "cdc_retention_message",
    "cdc_retention_dialect",
    "watermark",
    "cdc_shared_reader",
    "snapshot_mode",
    "snapshot_plan",
)


def _promote_cdc_job_fields(checkpoint: dict[str, Any], update: dict[str, Any]) -> None:
    """Copy CDC lag/health fields onto the job document for SSE + UI tiles."""
    if not isinstance(checkpoint, dict):
        return
    for key in _CDC_JOB_FIELDS:
        if key in checkpoint and key not in update:
            update[key] = checkpoint.get(key)
    cdc_meta = checkpoint.get("cdc") or {}
    if isinstance(cdc_meta, dict):
        for key in _CDC_JOB_FIELDS:
            if key in cdc_meta and key not in update:
                update[key] = cdc_meta.get(key)
    streams = checkpoint.get("streams")
    if isinstance(streams, list) and streams:
        update["streams"] = streams
    summary_streams = (checkpoint.get("destination_summary") or {}).get("streams")
    if (
        isinstance(summary_streams, list)
        and summary_streams
        and "streams" not in update
    ):
        update["streams"] = summary_streams


def _job_failure_fields(exc: Exception) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build error_details + top-level job fields for a failed transfer."""
    from services.error_handling import humanize_transfer_failure

    classification = classify_error(exc)
    human = humanize_transfer_failure(exc)
    details: dict[str, Any] = {
        "retriable": classification.get("retriable"),
        "evidence": classification.get("evidence"),
        "raw": human.get("raw") or str(exc),
        "code": human.get("code"),
        "title": human.get("title"),
        "fix": human.get("fix"),
        "category": human.get("category"),
        "message": human.get("message"),
        "confidence": human.get("confidence"),
    }
    # Prefer operator-facing message for job.error / SSE while keeping raw in details.
    extras: dict[str, Any] = {
        "error_code": human.get("code"),
        "error_title": human.get("title"),
        "error_fix": human.get("fix"),
        "error_confidence": human.get("confidence"),
        "operator_error": human.get("message"),
    }
    try:
        from services.cdc_lease import CdcLeaseConflict, LeaseStoreError
        from services.cdc_toast import CdcToastIncompleteError
        from services.cdc_transaction_buffer import CdcTxnBufferOverflow

        if isinstance(exc, CdcLeaseConflict):
            details.update(exc.to_dict())
            details["retriable"] = False
            extras.update(
                {
                    "cdc_lease_conflict": True,
                    "cdc_lease_holder": exc.holder_id or None,
                    "cdc_lease_resource": exc.resource or None,
                    "cdc_lease_cursor_key": exc.cursor_key or None,
                }
            )
        elif isinstance(exc, LeaseStoreError):
            details["code"] = "cdc_lease_store_unavailable"
            details["retriable"] = True  # Redis blip — safe to retry once store is back
            extras["cdc_lease_backend"] = "unavailable"
        elif isinstance(exc, CdcTxnBufferOverflow):
            details.update(exc.to_dict())
            details["retriable"] = False
            extras.update(
                {
                    "cdc_txn_buffer_overflow": True,
                    "cdc_txn_xid": exc.xid or None,
                    "cdc_txn_max_events": exc.max_events or None,
                }
            )
        elif isinstance(exc, CdcToastIncompleteError):
            details.update(exc.to_dict())
            details["retriable"] = False
            extras.update(
                {
                    "cdc_toast_incomplete": True,
                    "cdc_toast_table": exc.table or None,
                }
            )
    except Exception as exc:
        logger.debug("cdc toast classification skipped: %s", exc, exc_info=exc)
    try:
        from services.cdc_cursor_gap import CdcCursorGapError

        if isinstance(exc, CdcCursorGapError):
            details.update(exc.to_dict())
            details["retriable"] = False
            extras.update(
                {
                    "cdc_cursor_gap": True,
                    "cdc_cursor_gap_code": exc.code,
                    "cdc_cursor_gap_dialect": exc.dialect or None,
                    "cdc_cursor_gap_resume": exc.resume or None,
                    "cdc_cursor_gap_retained": exc.retained or None,
                    "cdc_lease_cursor_key": exc.cursor_key
                    or extras.get("cdc_lease_cursor_key"),
                }
            )
            if exc.snapshot_plan:
                extras["snapshot_plan"] = dict(exc.snapshot_plan)
                mode = exc.snapshot_plan.get("snapshot_mode")
                if mode:
                    extras["snapshot_mode"] = mode
    except Exception as exc:
        logger.debug("cdc cursor gap classification skipped: %s", exc, exc_info=exc)
    try:
        from services.cdc_effectively_once import CdcAppendOnlySinkError

        if isinstance(exc, CdcAppendOnlySinkError):
            details["code"] = "cdc_append_only_sink"
            details["retriable"] = False
            extras["cdc_append_only_sink"] = True
    except Exception as exc:
        logger.debug(
            "cdc append-only sink classification skipped: %s", exc, exc_info=exc
        )
    return details, extras


def _fail_runtime_job(
    mongo: Any,
    job_id: str,
    exc: Exception,
    *,
    lineage: Any = None,
    request: Any = None,
    already_persisted: list[int] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Persist a runtime failure with operator-facing message + failed_at_phase.

    When the exception carries ``rejected_details`` (WriteBatchBlocked or a
    connection-lost error stamped by ``_raise_write_failure``), persist DLQ
    before marking the job failed so quarantine cannot disappear.
    """
    stamped_details = list(getattr(exc, "rejected_details", None) or [])
    if stamped_details:
        summary = dict(getattr(exc, "dest_summary", None) or {})
        summary["rejected_details"] = stamped_details
        summary["rejected_rows"] = int(
            getattr(exc, "rejected_rows", 0) or len(stamped_details)
        )
        summary["rows_written"] = int(getattr(exc, "rows_written", 0) or 0)
        summary["ok"] = False
        summary["error"] = str(exc)
        try:
            _persist_job_quarantine(
                job_id,
                summary,
                request,
                already_persisted=already_persisted,
            )
        except Exception as qexc:
            logger.warning(
                "quarantine persist on runtime failure for %s: %s",
                job_id,
                qexc,
                exc_info=qexc,
            )
    cancelled = isinstance(exc, TransferCancelled)
    status = "cancelled" if cancelled else "failed"
    error_details, lease_extras = _job_failure_fields(exc)
    prev = {}
    try:
        prev = mongo.get_job(job_id) or {}
    except Exception as load_exc:
        logger.warning(
            "failed to load prior job state for %s: %s", job_id, load_exc, exc_info=load_exc
        )
        prev = {}
    prev_phase = str(prev.get("phase") or "").strip().lower()
    failed_at_phase = (
        prev_phase
        if prev_phase and prev_phase not in {"failed", "cancelled", "queued", ""}
        else "load"
    )
    operator_msg = str(
        lease_extras.pop("operator_error", None) or error_details.get("message") or exc
    )
    display = str(exc) if cancelled else operator_msg
    status_kwargs: dict[str, Any] = {
        "error": display,
        "phase": status,
        "failed_at_phase": failed_at_phase,
        "progress_pct": 0,
        "message": display,
        "error_details": error_details,
        **lease_extras,
    }
    if stamped_details:
        from services.job_document_budget import slim_rejected_details

        preview, total, truncated = slim_rejected_details(stamped_details)
        status_kwargs["rejected_rows"] = int(
            getattr(exc, "rejected_rows", 0) or total
        )
        status_kwargs["rejected_details"] = preview
        status_kwargs["rejected_details_total"] = total
        status_kwargs["rejected_details_truncated"] = truncated
        status_kwargs["records_processed"] = int(getattr(exc, "rows_written", 0) or 0)
    mongo.update_job_status(
        job_id,
        status,
        **status_kwargs,
    )
    if lineage is not None and not cancelled:
        lineage.emit_run_failed(
            run_id=job_id,
            job_id=job_id,
            error=display,
            error_details=error_details,
            retriable=bool(error_details.get("retriable", False)),
        )
    return display, error_details


def _cdc_fields_from_summary(dest_summary: dict[str, Any] | None) -> dict[str, Any]:
    """Top-level job fields from a CDC destination summary."""
    if not isinstance(dest_summary, dict):
        return {}
    out: dict[str, Any] = {}
    for key in _CDC_JOB_FIELDS:
        if key in dest_summary:
            out[key] = dest_summary.get(key)
    cdc_meta = dest_summary.get("cdc") or {}
    if isinstance(cdc_meta, dict):
        for key in _CDC_JOB_FIELDS:
            if key in cdc_meta and key not in out:
                out[key] = cdc_meta.get(key)
    streams = dest_summary.get("streams")
    if isinstance(streams, list) and streams:
        out["streams"] = streams
    return out
