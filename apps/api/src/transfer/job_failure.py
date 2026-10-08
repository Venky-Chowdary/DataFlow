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
    "cdc_publication_name",
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


def summary_streams(payload: Any) -> list[Any] | None:
    """Non-empty per-stream health.

    Writers stamp this list on ``destination_summary`` (and CDC checkpoints).
    An empty list is not a stream set — callers must not wipe a real one.
    """
    if not isinstance(payload, dict):
        return None
    streams = payload.get("streams")
    if isinstance(streams, list) and streams:
        return list(streams)
    return None


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
    # Checkpoint list is the live page. The summary list is the fallback when
    # the page only nested health under destination_summary.
    streams = summary_streams(checkpoint) or summary_streams(
        checkpoint.get("destination_summary")
    )
    if streams is not None:
        update["streams"] = streams


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
        from services.cdc_catchup import CdcStreamBehind
        from services.cdc_lease import CdcLeaseConflict, LeaseStoreError
        from services.cdc_toast import CdcToastIncompleteError
        from services.cdc_transaction_buffer import CdcTxnBufferOverflow

        if isinstance(exc, CdcStreamBehind):
            details["code"] = "cdc_stream_behind"
            details["retriable"] = True
            extras["cdc_stream_behind"] = True
            if exc.slot_name:
                extras["cdc_slot_name"] = exc.slot_name
        elif isinstance(exc, CdcLeaseConflict):
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


def _cdc_dest_release_args(request: Any) -> tuple[str, dict[str, Any] | None]:
    """Destination identity for retiring a dropped slot's exactly-once LSN."""
    if request is None:
        return "", None
    destination = getattr(request, "destination", None)
    if destination is None:
        return "", None
    try:
        from src.transfer.adapters import resolve_connector_config

        cfg = resolve_connector_config(destination)
    except Exception as exc:
        logger.debug("CDC slot release dest config unread: %s", exc)
        return "", None
    dest_type = str(
        getattr(destination, "format", "") or getattr(destination, "type", "") or ""
    )
    return dest_type, cfg if isinstance(cfg, dict) else None


def _release_failed_cdc_slot(
    mongo: Any,
    job_id: str,
    request: Any,
    *,
    job: dict[str, Any] | None = None,
) -> None:
    """Drop a one-shot Postgres slot after a non-retriable CDC failure.

    The reader closes in its own finally before this runs, so the slot is
    inactive unless a schedule still owns the route. A retriable failure
    never reaches this helper. Historical slots are not enumerated here.
    """
    try:
        loaded = job if isinstance(job, dict) and job else (mongo.get_job(job_id) or {})
    except Exception as exc:
        logger.warning("CDC slot release skipped; job %s unread: %s", job_id, exc)
        return
    schedule_id = ""
    source_cfg = None
    if request is not None:
        schedule_id = str(getattr(request, "schedule_id", "") or "")
        source = getattr(request, "source", None)
        if source is not None:
            try:
                from src.transfer.adapters import resolve_connector_config

                source_cfg = resolve_connector_config(source)
            except Exception as exc:
                logger.debug("CDC failure source config unread: %s", exc)
                source_cfg = None
    dest_type, dest_cfg = _cdc_dest_release_args(request)
    try:
        from services.cdc_catchup import release_finished_cdc_slot

        release_finished_cdc_slot(
            loaded,
            reason="failed",
            schedule_id=schedule_id,
            source_cfg=source_cfg,
            job_id=job_id,
            worker_closed=True,
            retriable=False,
            dest_type=dest_type,
            dest_cfg=dest_cfg,
        )
    except Exception as exc:
        logger.warning("CDC slot release after failure failed for %s: %s", job_id, exc)


def _release_cancelled_cdc_slot(mongo: Any, job_id: str, request: Any) -> None:
    """Drop a one-shot Postgres slot once the cancelled worker has closed it.

    A schedule that still owns the route keeps the slot. Failures here are
    logged; cancel itself must still be recorded.
    """
    try:
        job = mongo.get_job(job_id) or {}
    except Exception as exc:
        logger.warning("CDC slot release skipped; job %s unread: %s", job_id, exc)
        return
    schedule_id = ""
    source_cfg = None
    if request is not None:
        schedule_id = str(getattr(request, "schedule_id", "") or "")
        source = getattr(request, "source", None)
        if source is not None:
            try:
                from src.transfer.adapters import resolve_connector_config

                source_cfg = resolve_connector_config(source)
            except Exception as exc:
                logger.debug("CDC cancel source config unread: %s", exc)
                source_cfg = None
    dest_type, dest_cfg = _cdc_dest_release_args(request)
    try:
        from services.cdc_catchup import release_finished_cdc_slot

        release_finished_cdc_slot(
            job,
            reason="cancelled",
            schedule_id=schedule_id,
            source_cfg=source_cfg,
            job_id=job_id,
            worker_closed=True,
            dest_type=dest_type,
            dest_cfg=dest_cfg,
        )
    except Exception as exc:
        logger.warning("CDC slot release after cancel failed for %s: %s", job_id, exc)


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
    if cancelled:
        _release_cancelled_cdc_slot(mongo, job_id, request)
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
    if not cancelled and not bool(error_details.get("retriable")):
        _release_failed_cdc_slot(mongo, job_id, request, job=prev)
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
    streams = summary_streams(dest_summary)
    if streams is not None:
        out["streams"] = streams
    return out


def _validation_plan_for_result(pf: dict | None) -> dict:
    """Checklist plus live gate outcomes so operators see float→decimal etc. warnings."""
    if not pf:
        return {}
    plan = dict(pf.get("validation_plan") or {})
    if pf.get("gates") is not None:
        plan["gates"] = pf.get("gates") or []
    if "passed" in pf:
        plan["passed"] = pf.get("passed")
    if pf.get("warnings") is not None:
        plan["warnings"] = pf.get("warnings") or []
    if pf.get("blockers") is not None:
        plan["blockers"] = pf.get("blockers") or []
    if pf.get("readiness_score") is not None:
        plan["readiness_score"] = pf.get("readiness_score")
    return plan


def _blank_cells_as_null_from_preflight(pf: dict) -> int:
    """Highest Gate-8 count of spreadsheet blanks stored as SQL NULL.

    Several gates can echo the same sample. Summing them would double-count
    one blank cell. The max is the disposition the write path recorded.
    """
    highest = 0
    for gate in pf.get("gates") or []:
        if not isinstance(gate, dict):
            continue
        details = gate.get("details")
        if not isinstance(details, dict):
            continue
        raw = details.get("file_blank_null_count")
        if raw is None:
            continue
        try:
            highest = max(highest, int(raw))
        except (TypeError, ValueError):
            continue
    return highest


def _fail_job_preflight(
    mongo,
    job_id: str,
    pf: dict,
    *,
    lineage,
    rows_read: int | None = None,
    sync_mode: str = "",
) -> tuple[str, dict]:
    """Mark job failed at preflight and persist inspectable quarantine rows.

    ``rows_read`` is the count already taken (file peek or in-memory batch).
    When it is present the job ledger is write-refused: measured read, zero
    writes, dest COUNT(*) not taken. Omitting it leaves the read unmeasured,
    which is only honest when nothing was counted.
    """
    from services.quarantine_from_preflight import quarantine_rows_from_preflight

    decision = (pf.get("proof_bundle") or {}).get("transfer_decision", {}) or {}
    blocker_reasons = [
        b.get("message") for b in pf.get("blockers", []) if isinstance(b, dict)
    ]
    qrows = quarantine_rows_from_preflight(pf)
    row_ids = {d.get("row") for d in qrows if d.get("row") is not None}
    rejected_rows = len(row_ids) if row_ids else len(qrows)
    error_details = {
        "reason": "Preflight blocked transfer",
        "blockers": blocker_reasons,
        "guidance": [
            {
                "gate": b.get("id"),
                "message": b.get("message"),
                "why": (b.get("guidance") or {}).get("why", ""),
                "fix": (b.get("guidance") or {}).get("fix", ""),
            }
            for b in pf.get("blockers", [])
            if isinstance(b, dict) and b.get("guidance")
        ],
        "proof_bundle": {
            "decision": decision.get("decision"),
            "reason": decision.get("reason"),
            "semantic_mapping_score": pf.get("proof_bundle", {}).get(
                "semantic_mapping_score"
            ),
            "min_confidence": pf.get("proof_bundle", {}).get("min_confidence"),
            "quality_score": pf.get("proof_bundle", {}).get("quality_score"),
            "compliance_risk": (pf.get("proof_bundle", {}).get("compliance") or {}).get(
                "risk_score"
            ),
        },
        "readiness_score": pf.get("readiness_score"),
        "validation_plan": _validation_plan_for_result(pf),
        "payload_shape": pf.get("payload_shape"),
        "quarantine_issue_count": len(qrows),
        "quarantine_row_count": rejected_rows,
    }
    ledger_dict: dict | None = None
    if rows_read is not None:
        from services.row_conservation import write_refused_ledger

        ledger_dict = write_refused_ledger(
            rows_read=int(rows_read),
            quarantined_rows=rejected_rows,
            blank_cells_as_null=_blank_cells_as_null_from_preflight(pf),
            sync_mode=sync_mode,
        ).to_dict()
        error_details["row_accounting"] = ledger_dict
    error_message = (
        decision.get("reason")
        or "; ".join(str(x) for x in blocker_reasons if x)
        or "Preflight blocked transfer"
    )
    lineage.emit_preflight_completed(
        run_id=job_id,
        passed=False,
        readiness_score=pf.get("readiness_score", 0),
        blockers=pf.get("blockers", []),
        validation_plan=_validation_plan_for_result(pf),
    )
    lineage.emit_run_failed(
        run_id=job_id,
        job_id=job_id,
        error=error_message,
        error_details=error_details,
    )
    status_fields: dict = {
        "error": error_message,
        "phase": "failed",
        "progress_pct": 0,
        "error_details": error_details,
        "preflight": pf,
        "rejected_details": qrows,
        "rejected_rows": rejected_rows,
    }
    if ledger_dict is not None:
        status_fields["row_accounting"] = ledger_dict
        status_fields["sync_mode"] = str(sync_mode or "")
    mongo.update_job_status(job_id, "failed", **status_fields)
    return error_message, error_details
