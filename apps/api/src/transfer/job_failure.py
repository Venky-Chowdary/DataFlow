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
    stamped = getattr(exc, "failure_details", None)
    if isinstance(stamped, dict):
        details.update(stamped)
    # Prefer operator-facing message for job.error / SSE while keeping raw in details.
    extras: dict[str, Any] = {
        "error_code": human.get("code"),
        "error_title": human.get("title"),
        "error_fix": human.get("fix"),
        "error_confidence": human.get("confidence"),
        "operator_error": human.get("message"),
    }
    dropped = str(getattr(exc, "destination_table_dropped", "") or "")
    if dropped:
        from services.error_handling import ROWS_STAY_NOTE

        # D315: a failed first CDC run drops the table it created when no row
        # landed; the generic "rows stay" tail is untrue for it.
        message = str(human.get("message") or exc).replace(ROWS_STAY_NOTE, "").strip()
        message += (
            f" Nothing was kept: this run created the destination table {dropped} "
            "and dropped it again because no row had landed."
        )
        details["message"] = message
        details["destination_table_dropped"] = dropped
        extras["operator_error"] = message
    from services.cdc_drift_repair import (
        CdcDriftRequiresFencedRepair,
        cdc_drift_operator_message,
    )

    if isinstance(exc, CdcDriftRequiresFencedRepair):
        message = cdc_drift_operator_message(exc.evidence)
        details.update(
            {
                "code": "cdc_drift_requires_fenced_repair",
                "retriable": False,
                "message": message,
                "evidence": exc.evidence,
                "raw": message,
                "title": "CDC destination drift requires confirmed repair",
            }
        )
        extras.update(
            {
                "error_code": "cdc_drift_requires_fenced_repair",
                "operator_error": message,
                "cdc_drift_repair": exc.evidence,
                "cdc_lease_cursor_key": str(exc.evidence.get("cursor_key") or ""),
            }
        )
    # A destination that is short of the source image still has the rows on
    # the source. Dropping the replication slot here makes the next resume
    # report slot_missing and the unapplied inserts cannot be read back.
    short_image = "short of the live source image" in str(exc).lower()
    if short_image or "short of the live source image" in str(details.get("message") or "").lower():
        details["retriable"] = True
        details["code"] = "cdc_image_short"
    try:
        from services.cdc_catchup import CdcStreamBehind
        from services.cdc_lease import CdcLeaseConflict, LeaseStoreError
        from services.cdc_toast import CdcToastIncompleteError
        from services.cdc_transaction_buffer import CdcTxnBufferOverflow
        from connectors.oracle_logminer import (
            OracleBufferOffRacUnsupported,
            OracleInstanceDetectionUnavailable,
            OracleOpenTxnDetectionUnavailable,
        )

        oracle_buffer_off_errors = (
            OracleOpenTxnDetectionUnavailable,
            OracleInstanceDetectionUnavailable,
            OracleBufferOffRacUnsupported,
        )
        if isinstance(exc, oracle_buffer_off_errors):
            code = exc.code
            details.update(
                {
                    "code": code,
                    "retriable": False,
                    "raw": str(exc),
                    "message": str(exc),
                }
            )
            extras.update(
                {
                    "error_code": code,
                    "operator_error": str(exc),
                }
            )
            if isinstance(exc, OracleOpenTxnDetectionUnavailable):
                extras["oracle_open_txn_detection_unavailable"] = True
            elif isinstance(exc, OracleInstanceDetectionUnavailable):
                extras["oracle_instance_detection_unavailable"] = True
            elif isinstance(exc, OracleBufferOffRacUnsupported):
                extras["oracle_buffer_off_rac_unsupported"] = True
        elif isinstance(exc, CdcStreamBehind):
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


def _with_capture_identity(
    job: dict[str, Any], identity: dict[str, str] | None
) -> dict[str, Any]:
    """Job copy that names the reader's slot when no checkpoint recorded it."""
    merged = dict(job or {})
    for key, value in (identity or {}).items():
        if value and not str(merged.get(key) or "").strip():
            merged[key] = value
    return merged


def _release_failed_cdc_slot(
    mongo: Any,
    job_id: str,
    request: Any,
    *,
    job: dict[str, Any] | None = None,
    identity: dict[str, str] | None = None,
) -> None:
    """Drop a one-shot Postgres slot after a non-retriable CDC failure.

    The reader closes in its own finally before this runs, so the slot is
    inactive unless a schedule still owns the route. A retriable failure
    never reaches this helper. ``identity`` is the slot the reader named;
    a first-batch failure has no checkpoint that would have put it on the
    job. Historical slots are not enumerated here.
    """
    try:
        loaded = job if isinstance(job, dict) and job else (mongo.get_job(job_id) or {})
    except Exception as exc:
        logger.warning("CDC slot release skipped; job %s unread: %s", job_id, exc)
        return
    loaded = _with_capture_identity(loaded, identity)
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

        release_result = release_finished_cdc_slot(
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
        if release_result.get("dest_resume_blank_failed"):
            fields = {
                "dest_resume_blank_failed": True,
                "dest_resume_blank_error": str(
                    release_result.get("dest_resume_blank_error") or "unknown"
                ),
            }
            update_fields = getattr(mongo, "update_job_fields", None)
            if not callable(update_fields):
                logger.error(
                    "CDC resume blank failure could not be persisted for job %s "
                    "(update_job_fields unavailable)",
                    job_id,
                )
            else:
                try:
                    if not update_fields(job_id, fields):
                        logger.error(
                            "CDC resume blank failure could not be persisted for job %s",
                            job_id,
                        )
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "CDC resume blank failure could not be persisted for job %s "
                        "(%s)",
                        job_id,
                        type(exc).__name__,
                    )
    except Exception as exc:
        logger.warning("CDC slot release after failure failed for %s: %s", job_id, exc)


def _release_cancelled_cdc_slot(
    mongo: Any,
    job_id: str,
    request: Any,
    *,
    identity: dict[str, str] | None = None,
) -> None:
    """Drop a one-shot Postgres slot once the cancelled worker has closed it.

    A schedule that still owns the route keeps the slot. Failures here are
    logged; cancel itself must still be recorded.
    """
    try:
        job = mongo.get_job(job_id) or {}
    except Exception as exc:
        logger.warning("CDC slot release skipped; job %s unread: %s", job_id, exc)
        return
    job = _with_capture_identity(job, identity)
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


def release_cdc_capture_after_failure(
    mongo: Any,
    job_id: str,
    request: Any,
    exc: BaseException,
    *,
    retriable: bool,
    job: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Terminal slot release for every failure branch of a CDC run.

    Returns the slot identity the reader named so the caller can stamp it on
    the job. A retriable failure keeps the slot for the resume; a refused
    batch or other non-retriable failure drops it, and the next run
    snapshots current keys.
    """
    from services.cdc_catchup import capture_identity_from

    identity = capture_identity_from(exc)
    if isinstance(exc, TransferCancelled):
        _release_cancelled_cdc_slot(mongo, job_id, request, identity=identity)
    elif not retriable:
        _release_failed_cdc_slot(
            mongo, job_id, request, job=job, identity=identity
        )
    return identity


def _records_after_failure(prior: Any, incoming: Any) -> int:
    """Keep the larger committed count when a later attempt writes nothing.

    A prefix that already checkpointed must stay visible after the next
    chunk fails with ``rows_written=0``. A negative or unreadable value
    counts as zero.
    """

    def _n(value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    return max(_n(prior), _n(incoming))


def account_blocked_write(mongo: Any, job_id: str, blocked: Any) -> tuple[int, str]:
    """Row count and message for a write that stopped after a committed prefix.

    ``WriteBatchBlocked.rows_written`` is this attempt. A checkpoint already
    stored a larger ``records_processed``. Replacing it with 0 made the job
    say nothing landed while the destination still held the prefix
    (DEF-B2-012). The rows are not deleted.
    """
    prior = None
    try:
        job = mongo.get_job(job_id) or {}
        prior = job.get("records_processed")
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Blocked-write row count could not read job %s: %s", job_id, exc
        )
        prior = None
    kept = _records_after_failure(prior, getattr(blocked, "rows_written", 0))
    message = str(blocked)
    measured = getattr(blocked, "rows_committed_measured", None)
    if isinstance(measured, int) and measured >= 0:
        # The destination was re-counted after the failure (audit H3): that is
        # the run's committed total, and the note that carries it replaces the
        # writer tallies ("N rows were already written") that disagreed with it.
        note = str(getattr(blocked, "failed_batch_undo_note", "") or "")
        if note and note not in message:
            message = f"{message.rstrip()} {note}"
        return measured, message
    try:
        incoming = max(0, int(getattr(blocked, "rows_written", 0) or 0))
    except (TypeError, ValueError):
        incoming = 0
    if kept > incoming:
        note = f"{kept} row(s) were already written and were not removed."
        if note not in message:
            message = f"{message.rstrip()} {note}"
    return kept, message


def _fail_runtime_job(
    mongo: Any,
    job_id: str,
    exc: Exception,
    *,
    lineage: Any = None,  # noqa: F811  # preserve public parameter name
    request: Any = None,
    already_persisted: list[int] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Persist a runtime failure with operator-facing message + failed_at_phase.

    When the exception carries ``rejected_details`` (WriteBatchBlocked or a
    connection-lost error stamped by ``_raise_write_failure``), persist DLQ
    before marking the job failed so quarantine cannot disappear.
    """
    if getattr(exc, "superseded", False):
        # A newer attempt of this job owns the record now (resume): this one
        # stops quietly rather than marking the live attempt cancelled/failed.
        logger.warning("Job %s: attempt superseded by a newer one; stopping", job_id)
        return str(exc), {}
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
    from services.cdc_catchup import capture_identity_from

    capture = capture_identity_from(exc)
    cancelled = isinstance(exc, TransferCancelled)
    status = "cancelled" if cancelled else "failed"
    if cancelled:
        _release_cancelled_cdc_slot(mongo, job_id, request, identity=capture)
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
    if lease_extras:
        status_kwargs["extras"] = {
            **(prev.get("extras") if isinstance(prev.get("extras"), dict) else {}),
            **lease_extras,
        }
    for key, value in capture.items():
        if not str(prev.get(key) or "").strip():
            status_kwargs.setdefault(key, value)
    held_out_ledger = getattr(exc, "row_accounting", None)
    if isinstance(held_out_ledger, dict):
        # The run counted its read and held every row out; the job keeps that
        # ledger instead of an unmeasured rebuild that reads "held out 0".
        error_details["row_accounting"] = held_out_ledger
        status_kwargs["row_accounting"] = held_out_ledger
        status_kwargs.setdefault(
            "rejected_rows", int(held_out_ledger.get("rows_quarantined") or 0)
        )
    if stamped_details:
        from services.job_document_budget import slim_rejected_details

        preview, total, truncated = slim_rejected_details(stamped_details)
        status_kwargs["rejected_rows"] = int(
            getattr(exc, "rejected_rows", 0) or total
        )
        status_kwargs["rejected_details"] = preview
        status_kwargs["rejected_details_total"] = total
        status_kwargs["rejected_details_truncated"] = truncated
        status_kwargs["records_processed"] = _records_after_failure(
            prev.get("records_processed"), getattr(exc, "rows_written", 0)
        )
    elif hasattr(exc, "rows_written"):
        # A duplicate-key abort often has no quarantine rows. Leaving the
        # counter at 0 while the destination already committed a prefix hid
        # the partial write. A later chunk that writes nothing must not
        # erase the count the earlier chunk already stored on this job.
        status_kwargs["records_processed"] = _records_after_failure(
            prev.get("records_processed"), getattr(exc, "rows_written")
        )
    if cancelled:
        from services.job_status import cancel_outcome_for

        # A route that did not count its destination (COPY fast paths,
        # adapters) reports a progress meter, not committed batches.
        unmeasured = getattr(exc, "destination_rows_now", None) is None
        if unmeasured and request is not None:
            _measure_unmeasured_cancel(exc, request)
        outcome = cancel_outcome_for(
            status_kwargs.get("records_processed", prev.get("records_processed")),
            destination_rows_by_run=getattr(exc, "destination_rows_by_run", None),
            destination_rows_now=getattr(exc, "destination_rows_now", None),
            destination_table_exists=getattr(exc, "destination_table_exists", None),
            destination_rows_before=getattr(exc, "destination_rows_before", None),
            write_rolled_back=getattr(exc, "write_rolled_back", None) is True,
            tally_is_commit_evidence=not (unmeasured and request is not None),
        )
        status_kwargs["cancel_outcome"] = outcome
        if outcome.get("rows_committed_source") == "destination_count":
            # D199 — the destination's own count replaces the committed-batch
            # tally wherever the job reports it.
            status_kwargs["records_processed"] = outcome["rows_committed"]
        elif outcome.get("rows_committed_source") == "unknown":
            # Unknown is not zero: a retry must not read the meter's 0 (or
            # its rolled-back rows) as a fact about the destination.
            status_kwargs["records_processed"] = None
        # The error text is the cancel message too: the exception's own
        # "N row(s) were already written and were not removed" is the writer's
        # tally, which a rollback (or a dropped run-created table) makes untrue
        # (C16, MySQL D11). The record states the destination as measured.
        display = outcome["message"]
        status_kwargs["error"] = display
        status_kwargs["message"] = display
        if outcome["partial_write"]:
            logger.warning(
                "Job %s cancelled after %s row(s) committed; nothing was rolled back",
                job_id,
                outcome["rows_committed"],
            )
    mongo.update_job_status(
        job_id,
        status,
        **status_kwargs,
    )
    if not cancelled and not bool(error_details.get("retriable")):
        _release_failed_cdc_slot(
            mongo, job_id, request, job=prev, identity=capture
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


def _measure_unmeasured_cancel(exc: BaseException, request: Any) -> None:
    """Count the destination for a cancelled route that did not (C16, D11).

    The row path measures its own landing (``stream.py``). A COPY fast path or
    an adapter raises the cancel with only the writer's tally — the meter a
    single-transaction COPY moved before it rolled back, or the rows of a
    run-created table its undo dropped. The pre-run facts ride
    ``destination.extra`` (``dest_table_existed_before``, and the pre-write
    count ``cancel_rows_before``), so the same measurement applies here. A
    COPY route's overwrite is one transaction or a staged swap on every SQL
    destination (atomic); a schemaless sink clears first. A rollback is
    claimed only when the route marked it (``mark_write_rolled_back``); keyed
    modes use the shared ``keyed_landing`` predicate.
    """
    try:
        from services.job_status import measure_cancel_landing
        from services.sync_cursor import resolve_effective_sync_mode, resolve_sync_contract

        from .adapters import resolve_connector_config, resolve_dest_table
        from .connector_capabilities import resolve_driver_type
        from .engine_family import is_schemaless_family

        _dst = request.destination
        if str(getattr(_dst, "kind", "") or "") != "database":
            return
        _dtype = resolve_driver_type(str(_dst.format or ""))
        _src_name = str(
            getattr(request.source, "table", "")
            or getattr(request.source, "collection", "")
            or ""
        )
        contract = resolve_sync_contract(getattr(request, "stream_contracts", None))
        sync = str(
            resolve_effective_sync_mode(
                getattr(request, "sync_mode", ""), contract.sync_mode if contract else None
            )
            or ""
        )
        extra = getattr(_dst, "extra", None) or {}
        before = next(
            (
                extra.get(k)
                for k in ("dest_rows_before", "cancel_rows_before", "overwrite_rows_before")
                if isinstance(extra.get(k), int)
            ),
            None,
        )
        measure_cancel_landing(
            exc,
            dest_type=_dtype,
            dest_cfg=resolve_connector_config(_dst),
            table=resolve_dest_table(_dtype, _dst, _src_name),
            sync_mode=sync,
            rows_before=before,
            table_existed_before=extra.get("dest_table_existed_before"),
            write_mode=str(getattr(request, "write_mode", "") or ""),
            atomic_overwrite=not is_schemaless_family(_dtype),
        )
    except Exception as count_exc:  # noqa: BLE001 - the tally still stands
        logger.debug("cancel destination count unavailable: %s", count_exc)


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
    lineage,  # noqa: F811  # preserve public parameter name
    rows_read: int | None = None,
    sync_mode: str = "",
) -> tuple[str, dict]:
    """Mark job failed at preflight and persist inspectable quarantine rows.

    ``rows_read`` is the count already taken (file peek or in-memory batch).
    When it is present the job ledger is write-refused: measured read, zero
    writes, dest COUNT(*) not taken. Omitting it leaves the read unmeasured,
    which is only honest when nothing was counted.
    """
    from services.quarantine_from_preflight import (
        preflight_quarantine_row_count,
        quarantine_rows_from_preflight,
    )

    from services.blocker_titles import collapse_operator_message, is_schema_drift_echo

    decision = (pf.get("proof_bundle") or {}).get("transfer_decision", {}) or {}
    blocker_reasons: list[str] = []
    saw_drift = False
    for blocker in pf.get("blockers", []):
        if not isinstance(blocker, dict):
            continue
        text = str(blocker.get("message") or "").strip()
        if not text or text in blocker_reasons:
            continue
        if is_schema_drift_echo(text):
            if saw_drift:
                continue
            saw_drift = True
        blocker_reasons.append(text)
    qrows = quarantine_rows_from_preflight(pf)
    rejected_rows = preflight_quarantine_row_count(qrows)
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
    error_message = collapse_operator_message(
        decision.get("reason")
        or "; ".join(str(x) for x in blocker_reasons if x)
        or "Preflight blocked transfer"
    )
    from services.root_cause_engine import CAST_FAILURE_ROOT_TITLE, quarantine_retry_fix

    if any(
        isinstance(b, dict)
        and (b.get("details") or {}).get("root_cause")
        and (b.get("details") or {}).get("title") == CAST_FAILURE_ROOT_TITLE
        for b in pf.get("blockers", [])
    ):
        # D170: the run was refused whole; say how to load the valid rows.
        # QA r12 D2: the job is failing, so the way out is a retry — name the
        # tool and REST field that accept the acceptance for a failed job.
        fix = quarantine_retry_fix(job_id)
        error_message = f"{error_message.rstrip('. ')}. {fix}."
        error_details["remediation"] = fix
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
    # A blocking gate that states a stable refusal code (e.g. g24's
    # DESTINATION_CONTINUITY) carries it to the job, exactly as the Execute-time
    # check does, so the retry policy and the schedule park read one code
    # whichever door refused the run (QA C9 review 2).
    coded = next(
        (
            b.get("details") for b in pf.get("blockers", [])
            if isinstance(b, dict) and isinstance(b.get("details"), dict)
            and str((b.get("details") or {}).get("error_code") or "").strip()
        ),
        None,
    )
    if coded:
        status_fields["error_code"] = str(coded["error_code"])
        error_details["error_code"] = str(coded["error_code"])
        if coded.get("mismatch"):
            error_details["mismatch"] = coded["mismatch"]
    from services.drift_approval import (
        build_source_drift_approval,
        gate_codes_from_preflight,
        open_schedule_drift_approval,
        schedule_id_from_job,
        schema_evidence_from_preflight,
    )

    codes = gate_codes_from_preflight(pf)
    approval = build_source_drift_approval(
        message=error_message,
        codes=codes,
        job_id=job_id,
        evidence={"gate_codes": codes, "schema_findings": schema_evidence_from_preflight(pf)},
    )
    schedule_id = ""
    if approval is not None:
        status_fields["approval_request"] = approval
        try:
            schedule_id = schedule_id_from_job(mongo.get_job(job_id) or {})
        except Exception:
            schedule_id = ""
    mongo.update_job_status(job_id, "failed", **status_fields)
    if approval is not None and schedule_id:
        try:
            open_schedule_drift_approval(schedule_id, approval)
        except Exception:
            logger.exception("Manual run %s drift approval was not opened", job_id)
    return error_message, error_details
