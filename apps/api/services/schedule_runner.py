"""Background scheduler — runs due pipeline syncs."""

from __future__ import annotations

import asyncio
import logging
import os
from services.brand_env import getenv_brand
import socket
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

from services.schedule_store import due_schedules
from services.failure_retry_policy import DETERMINISTIC
from services.standing_authorization import (
    CODE_NO_AUTHORIZATION,
    SCOPE_NET_ADDITIVE_DRIFT,
    AuthorizationDecision,
    binding_from_schedule,
    evaluate_authorization,
)

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="schedule-runner")
CHECK_INTERVAL_SECONDS = 60
LOCK_TTL_SECONDS = int(getenv_brand("SCHEDULER_LOCK_TTL", "300"))


def _scheduler_instance_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def _lock_expiry() -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=LOCK_TTL_SECONDS)


def _mongo_backend():
    try:
        from services.mongodb_service import get_mongodb_service
    except ImportError:
        from services.mongodb_service import get_mongodb_service
    try:
        svc = get_mongodb_service()
    except Exception:
        return None
    if type(svc).__name__ == "MemoryMongoDBService":
        return None
    return svc if getattr(svc, "client", None) is not None else None


def _acquire_scheduler_lock() -> bool:
    """Try to acquire a short-lived distributed lock for this scheduler beat.

    When a real MongoDB is shared across replicas this prevents duplicate runs.
    When multi-replica coordination is required and Mongo is unavailable, fail
    closed (return False). Single-instance / memory mode may proceed without a
    shared lock.
    """
    from services.worker_leases import requires_distributed_backend

    svc = _mongo_backend()
    if not svc:
        if requires_distributed_backend():
            logger.error("Scheduler lock unavailable; refuse beat (multi-replica fail-closed)")
            return False
        return True
    db = svc.get_database()
    now = datetime.now(timezone.utc)
    instance = _scheduler_instance_id()
    try:
        result = db["schedule_locks"].find_one_and_update(
            {
                "_id": "global_scheduler_lock",
                "$or": [
                    {"expires_at": {"$lte": now}},
                    {"expires_at": None},
                    {"instance": instance},
                ],
            },
            {
                "$set": {
                    "instance": instance,
                    "acquired_at": now,
                    "expires_at": _lock_expiry(),
                },
                "$setOnInsert": {"_id": "global_scheduler_lock"},
            },
            upsert=True,
            return_document=True,
        )
        return bool(result and result.get("instance") == instance)
    except Exception as exc:
        name = type(exc).__name__
        if "DuplicateKey" in name:
            return False
        logger.exception("Failed to acquire scheduler lock")
        if requires_distributed_backend():
            return False
        return False


def _release_scheduler_lock() -> None:
    svc = _mongo_backend()
    if not svc:
        return
    try:
        svc.get_database()["schedule_locks"].delete_one(
            {"_id": "global_scheduler_lock", "instance": _scheduler_instance_id()}
        )
    except Exception:
        logger.exception("Failed to release scheduler lock")


def _resolve_connector(connector_id: str) -> dict | None:
    """Load connector from file store (UUID) with MongoDB platform fallback."""
    import sys
    from pathlib import Path

    api_root = Path(__file__).resolve().parents[2]
    if str(api_root) not in sys.path:
        sys.path.insert(0, str(api_root))

    try:
        from services.connector_store import get_connector

        conn = get_connector(connector_id)
        if conn:
            data = conn.to_dict()
            data["_id"] = conn.id
            data["id"] = conn.id
            data["type"] = conn.type
            return data
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    try:
        from services.mongodb_service import get_mongodb_service

        return get_mongodb_service().get_connector(connector_id)
    except Exception:
        return None


def _apply_callable_schedule_source(source, sched) -> None:
    """Stamp CALL/SELECT fields onto the endpoint so the reader is not a table scan."""
    mode = str(getattr(sched, "source_read_mode", "") or "").strip().lower()
    call = str(getattr(sched, "procedure_call", "") or "").strip()
    query = str(getattr(sched, "source_query", "") or "").strip()
    params = getattr(sched, "procedure_params", None) or {}
    if mode not in {"procedure", "query"} and not call and not query:
        return
    if mode not in {"procedure", "query"}:
        mode = "procedure" if call else "query"
    extra = dict(getattr(source, "extra", None) or {})
    extra["source_read_mode"] = mode
    if call:
        extra["procedure_call"] = call
    if query:
        extra["source_query"] = query
    if isinstance(params, dict) and params:
        extra["procedure_params"] = {str(k): v for k, v in params.items()}
    source.extra = extra


def probe_schedule_source_schema(source) -> dict[str, Any]:
    """Read the extract shape. CALL/SELECT peeks — never a colliding table."""
    from services.procedure_source import (
        close_callable_spool,
        is_callable_source,
        read_callable_batch,
    )

    if is_callable_source(source):
        from src.transfer.models import endpoint_to_dict

        cfg = endpoint_to_dict(source)
        try:
            batch = read_callable_batch(cfg, offset=0, limit=1, peek=True)
        finally:
            close_callable_spool()
        headers = [str(h) for h in (batch.headers or []) if str(h).strip()]
        meta = getattr(batch, "meta", None) or {}
        native = meta.get("native_types") if isinstance(meta, dict) else {}
        schema: dict[str, str] = {}
        if isinstance(native, dict):
            schema = {str(k): str(v) for k, v in native.items() if str(k).strip()}
        for name in headers:
            schema.setdefault(name, "VARCHAR")
        return {
            "schema": schema,
            "columns": headers or list(schema.keys()),
            "primary_key_columns": [],
        }

    from src.transfer.endpoint_intelligence import introspect_endpoint

    return introspect_endpoint(source) or {}


def _endpoint_from_connector(conn: dict, table: str):
    from src.transfer.models import EndpointConfig

    is_mongo = conn.get("type") == "mongodb"
    connector_id = str(conn.get("_id") or conn.get("id") or "")
    from services.dialect_profiles import schema_from_cfg

    return EndpointConfig(
        kind="database",
        format=conn.get("type", ""),
        connector_id=connector_id or None,
        host=conn.get("host", ""),
        port=int(conn.get("port", 0) or 0),
        database=conn.get("database", ""),
        schema=schema_from_cfg(conn.get("type"), conn),
        table=table if not is_mongo else "",
        collection=table if is_mongo else "",
        username=conn.get("username", ""),
        password=conn.get("password", ""),
        connection_string=conn.get("connection_string", ""),
        warehouse=conn.get("warehouse", ""),
    )


def _normalize_sync_mode(sync_mode: str, primary_key: str) -> str:
    """Map the schedule's coarse sync_mode onto the engine's contract vocabulary."""
    mode = (sync_mode or "full_refresh_overwrite").lower()
    if mode == "incremental":
        return "incremental_deduped" if primary_key else "incremental_append"
    if mode in ("scd2", "mirror"):
        return mode
    return mode


def build_schedule_request(sched, src: dict, dst: dict):
    """Build a :class:`TransferRequest` from a persisted schedule.

    Threads the per-schedule sync mode, validation mode, mappings, and any
    watermark/primary-key contract so scheduled runs can be incremental/CDC
    instead of always full-refresh. Backward compatible: schedules created before
    these fields existed fall back to full_refresh_overwrite / strict.
    """
    from src.transfer.models import TransferRequest

    source = _endpoint_from_connector(src, sched.source_table)
    destination = _endpoint_from_connector(dst, sched.dest_table)
    _apply_callable_schedule_source(source, sched)

    effective_mode = _normalize_sync_mode(sched.sync_mode, sched.primary_key)
    from services.procedure_source import assert_callable_sync_allowed

    assert_callable_sync_allowed(effective_mode, source)
    stream_contracts = list(sched.stream_contracts or [])
    if not stream_contracts and effective_mode not in ("full_refresh_overwrite", "full_refresh_append"):
        stream_contracts = [{
            "selected": True,
            "name": sched.source_table,
            "stream": sched.source_table,
            "sync_mode": effective_mode,
            "cursor_field": sched.cursor_column,
            "primary_key": sched.primary_key,
            "schema_policy": sched.schema_policy,
            "validation_mode": sched.validation_mode,
        }]

    from services.schedule_store import assert_schedule_run_allowed

    bind = assert_schedule_run_allowed(sched)
    contract_id = str(bind.get("contract_id") or "").strip()
    require_signed = bool(bind.get("require_signed_contract", False))

    # Fail-closed: open CDC Map-review for this source pauses scheduled runs.
    sync_l = str(getattr(sched, "sync_mode", "") or "").strip().lower()
    is_cdc_like = sync_l in {"cdc", "incremental", "incremental_append", "incremental_deduped"}
    try:
        from services.cdc_mapping_review import open_review_for_source
        from services.cdc_schema_history import connection_fingerprint
        from services.connector_store import get_connector

        src_conn = get_connector(sched.source_connector_id, workspace_id=sched.workspace_id or None)
        if src_conn is not None:
            cfg = src_conn.to_dict()
            source_keys = [
                connection_fingerprint(cfg, connector_id=src_conn.id),
                connection_fingerprint(cfg),
            ]
            table = (sched.source_table or "").strip()
            seen: set[str] = set()
            for source_key in source_keys:
                if not source_key or source_key in seen:
                    continue
                seen.add(source_key)
                review = open_review_for_source(source_key, table) or open_review_for_source(
                    source_key, ""
                )
                if review:
                    raise ValueError(
                        f"CDC mapping review required before schedule run "
                        f"(review_id={review.get('id')}, table={review.get('table')}). "
                        "Open Map, review fidelity, acknowledge the review, then re-enable."
                    )
    except ValueError:
        raise
    except Exception as exc:
        if is_cdc_like:
            raise ValueError(
                f"CDC mapping-review gate unavailable (fail-closed): {exc}"
            ) from exc
        logger.debug("CDC mapping-review schedule gate skipped", exc_info=True)

    from services.batch_progress import effective_backfill_new_fields

    mappings = list(sched.mappings or [])
    schema_policy = sched.schema_policy or "manual_review"
    # Autopilot: a scheduled run has nobody at the keyboard, so it carries the
    # attestations a named human signed in advance — and only while the plan they
    # signed is still the plan being run. With no valid grant these stay False and
    # every gate decides exactly as it did before.
    decision = authorization_for(sched)
    acks = decision.acknowledgments
    return TransferRequest(
        source=source,
        destination=destination,
        mappings=mappings,
        skip_preflight=False,
        sync_mode=effective_mode,
        schema_policy=schema_policy,
        validation_mode=sched.validation_mode or "strict",
        delivery_guarantee=getattr(sched, "delivery_guarantee", None)
        or "at_least_once",
        backfill_new_fields=effective_backfill_new_fields(
            backfill_new_fields=bool(sched.backfill_new_fields),
            schema_policy=schema_policy,
            mappings=mappings,
        ),
        stream_contracts=stream_contracts,
        workspace_id=sched.workspace_id or "",
        contract_id=contract_id,
        enforce_contract=bool(contract_id),
        require_signed_contract=require_signed,
        compliance_acknowledged=acks.compliance,
        schema_drift_acknowledged=acks.schema_drift,
        fk_risk_acknowledged=acks.fk_risk,
        acknowledgment_actor=acks.actor if acks.any_claimed else "",
        acknowledgment_reason=acks.reason if acks.any_claimed else "",
    )


def authorization_for(sched: Any) -> AuthorizationDecision:
    """What this schedule's standing authorization permits for the run at hand.

    Recomputes the binding from the schedule as it stands now, so a grant signed
    against a different mapping, source shape or policy authorizes nothing. Never
    raises: an unreadable grant is no authority, not an outage.
    """
    try:
        return evaluate_authorization(
            getattr(sched, "standing_authorization", None),
            binding_now=binding_from_schedule(sched),
        )
    except Exception as exc:  # noqa: BLE001 - unreadable authority is no authority
        logger.warning(
            "Schedule %s standing authorization unreadable — treating as absent: %s",
            getattr(sched, "id", ""),
            exc,
        )
        return AuthorizationDecision(
            applies=False,
            code=CODE_NO_AUTHORIZATION,
            reason="The standing authorization could not be read.",
            corrective_action="Re-grant the authorization.",
        )


def _guard_source_schema_drift(sched: Any, request: Any) -> bool:
    """Refuse a scheduled run whose source changed shape since the last one.

    Returns whether the run proceeded *on delegated authority* rather than because
    nothing changed, so the caller can record the authority as exercised.

    Raises ``ApprovalRequired`` (a ``ValueError``) so the caller records it the way
    it records a contract refusal — before a row moves. The damaging drift is the kind that
    would otherwise succeed: a column that keeps its name and changes type loads
    cleanly and writes wrong values, and nothing downstream reports an error.

    A source that cannot be introspected is left alone. Refusing a nightly load
    because a probe timed out is the false alarm that gets the check switched
    off, and an unread schema is not evidence of a change.
    """
    from services.schedule_approvals import KIND_SOURCE_DRIFT, ApprovalRequired
    from services.source_schema_memory import evaluate_source_drift
    from services.standing_authorization import scopes_for_drift_kinds

    previous = dict(getattr(sched, "source_schema", None) or {})
    previous_pk = [
        str(p).strip()
        for p in (getattr(sched, "source_primary_key", None) or [])
        if str(p).strip()
    ]
    try:
        info = probe_schedule_source_schema(request.source) or {}
    except Exception as exc:
        logger.info(
            "Schedule %s source schema probe unavailable: %s",
            getattr(sched, "id", ""),
            exc,
        )
        return False
    current = {
        str(k): str(v)
        for k, v in dict(info.get("schema") or {}).items()
        if not isinstance(v, (dict, list))
    }
    if not current:
        return False
    current_pk = [
        str(p).strip()
        for p in (info.get("primary_key_columns") or [])
        if str(p).strip()
    ]
    cursor = str(getattr(sched, "cursor_column", "") or "").strip()

    verdict = evaluate_source_drift(
        previous_schema=previous,
        current_schema=current,
        current_columns=list(info.get("columns") or current.keys()),
        mappings=list(getattr(request, "mappings", None) or []),
        schema_policy=str(getattr(sched, "schema_policy", "") or "manual_review"),
        dest_db=str(getattr(request.destination, "format", "") or ""),
        previous_primary_key=previous_pk,
        current_primary_key=current_pk,
        cursor_fields=[cursor] if cursor else None,
    )
    if verdict.blocks:
        kinds = [str(e.get("kind") or "") for e in (verdict.breaking or [])]
        scopes = scopes_for_drift_kinds(kinds)
        decision = authorization_for(sched)
        if SCOPE_NET_ADDITIVE_DRIFT in scopes and decision.allows(SCOPE_NET_ADDITIVE_DRIFT):
            # A mapped drop or rename is destination-safe (nothing is dropped
            # there) and the named granter accepted it in advance for exactly this
            # plan. Proceeding is authorized; it is still recorded as authority
            # exercised, never as a check that passed.
            logger.info(
                "Schedule %s net-additive source drift proceeding under "
                "authorization %s: %s",
                getattr(sched, "id", ""),
                decision.grant_id,
                verdict.summary,
            )
            _remember_source_schema(
                sched, current, verdict.fingerprint, primary_key=current_pk
            )
            return True
        raise ApprovalRequired(
            f"{verdict.summary} Review the mapping and re-approve, or set "
            "schema_policy to propagate the change deliberately.",
            kind=KIND_SOURCE_DRIFT,
            code="SOURCE_SCHEMA_DRIFT",
            corrective_action=(
                "Open the schedule's Map, confirm the mapping still holds, then "
                "accept the new source shape as the baseline."
            ),
            scopes=scopes,
            evidence={
                "summary": verdict.summary,
                "compatibility": verdict.compatibility,
                "breaking": list(verdict.breaking or [])[:20],
                "additive": list(verdict.additive or [])[:20],
                "source_schema_fingerprint": verdict.fingerprint,
            },
        )
    _remember_source_schema(
        sched, current, verdict.fingerprint, primary_key=current_pk
    )
    return False


def _remember_source_schema(
    sched: Any,
    schema: dict[str, str],
    fingerprint: str,
    *,
    primary_key: list[str] | None = None,
) -> None:
    """Record the shape this run read, so the next one has something to compare."""
    pk = [str(p).strip() for p in (primary_key or []) if str(p).strip()]
    same_fp = fingerprint == getattr(sched, "source_schema_fingerprint", "")
    same_pk = pk == list(getattr(sched, "source_primary_key", None) or [])
    if not fingerprint or (same_fp and same_pk):
        return
    try:
        from services.schedule_store import update_schedule

        update_schedule(
            str(getattr(sched, "id", "")),
            {
                "source_schema": dict(schema),
                "source_schema_fingerprint": fingerprint,
                "source_schema_observed_at": datetime.now(timezone.utc).isoformat(),
                "source_primary_key": pk,
            },
        )
    except Exception as exc:
        # Losing the baseline costs a later comparison, never this run.
        logger.info("Schedule source schema not recorded: %s", exc)


def _run_entry(job_id: str, status: str, attempt: int, started_at: datetime, job_doc: dict | None) -> dict:
    doc = job_doc or {}
    finished = datetime.now(timezone.utc)
    ledger = doc.get("row_accounting") if isinstance(doc.get("row_accounting"), dict) else {}
    return {
        "job_id": job_id,
        "status": status,
        "attempt": attempt,
        "started_at": started_at.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_seconds": round((finished - started_at).total_seconds(), 3),
        "records_transferred": int(doc.get("records_processed", 0) or 0),
        "rejected_rows": int(doc.get("rejected_rows", 0) or 0),
        "coerced_null_rows": int(doc.get("coerced_null_rows", 0) or 0),
        "error": (doc.get("error") or "")[:500],
        "row_accounting": dict(ledger),
    }


def _notify_schedule(sched, job_id: str, status: str, job_doc: dict | None) -> None:
    """Deliver success/failure notifications honoring per-schedule preferences."""
    success = _is_success(status)
    if success and not sched.notify_on_success:
        return
    if not success and not sched.notify_on_failure:
        return
    try:
        from services.notification_service import (
            build_job_payload,
            log_job_notifications,
            notify_workspace,
        )
        from services.platform_config import public_url, web_url

        doc = job_doc or {}
        payload = build_job_payload(
            job_id=job_id,
            status=status,
            source=sched.source_table or sched.source_connector_id,
            destination=sched.dest_table or sched.dest_connector_id,
            records_transferred=int(doc.get("records_processed", 0) or 0),
            rejected_rows=int(doc.get("rejected_rows", 0) or 0),
            error=doc.get("error") or "",
            retry_url=f"/api/v1/connectors/jobs/{job_id}/resume",
            workspace_id=sched.workspace_id or "",
            base_url=public_url(),
            web_url=web_url(),
        )
        results = notify_workspace(sched.workspace_id or "", payload)
        log_job_notifications(job_id, results)
    except Exception:
        logger.exception("Failed to send schedule notification for %s", sched.id)


def _is_success(status: str | None) -> bool:
    # One vocabulary: a local copy drifts from the canonical statuses the engine
    # actually writes, and a status this set does not know is read as a failure.
    from services.job_status import is_completed

    return is_completed(status)


def _retry_decision(
    status: str | None,
    attempt: int,
    max_retries: int,
    *,
    sync_mode: str | None = None,
    rows_committed: int = 0,
    rows_committed_known: bool = True,
    job_doc: dict | None = None,
) -> dict:
    """Whether to start attempt ``attempt + 1``, and why not when refusing.

    A scheduled retry is a *from-zero* run: it builds a fresh request and reads
    the source from the beginning. That is safe for a convergent sync mode and
    for an attempt that committed nothing — but re-running an append that
    already landed rows writes every one of them again, unattended and at the
    schedule's cadence, so it is refused with the operator pointed at Resume.

    Safe to retry is not the same as worth retrying. A run refused by a
    validation gate wrote nothing, so the duplicate check waves it through, and
    the schedule then replays an identical deterministic verdict until the
    budget is gone. Those are refused here with the corrective action named.
    """
    from services.execution_engine_contract import decide_retry_from_start
    from services.failure_retry_policy import classify_job_failure

    if _is_success(status):
        return {"retry": False, "reason": ""}
    if attempt >= max_retries:
        return {
            "retry": False,
            "reason": f"Retry budget exhausted after {max_retries} attempt(s).",
        }
    classification = classify_job_failure(job_doc)
    if not classification.retryable:
        reason = classification.reason
        if classification.corrective_action:
            reason = f"{reason} {classification.corrective_action}"
        return {
            "retry": False,
            "reason": reason,
            "failure_class": classification.to_dict(),
        }
    decision = decide_retry_from_start(
        status=status,
        sync_mode=sync_mode,
        rows_committed=rows_committed,
        rows_committed_known=rows_committed_known,
    )
    if not decision["allowed"]:
        return {
            "retry": False,
            "reason": decision["reason"],
            "decision": decision,
            "failure_class": classification.to_dict(),
        }
    return {
        "retry": True,
        "reason": "",
        "decision": decision,
        "failure_class": classification.to_dict(),
    }


def _should_retry(
    status: str | None,
    attempt: int,
    max_retries: int,
    *,
    sync_mode: str | None = None,
    rows_committed: int = 0,
    rows_committed_known: bool = True,
    job_doc: dict | None = None,
) -> bool:
    return bool(
        _retry_decision(
            status,
            attempt,
            max_retries,
            sync_mode=sync_mode,
            rows_committed=rows_committed,
            rows_committed_known=rows_committed_known,
            job_doc=job_doc,
        )["retry"]
    )


def _job_doc(job_id: str) -> dict | None:
    try:
        from services.mongodb_service import get_mongodb_service

        return get_mongodb_service().get_job(job_id)
    except Exception:
        return None


def _observe_parallel_run(sched: Any, job_doc: dict | None = None) -> None:
    """Record one Dual Run cycle after a successful overwrite load.

    Never fails the transfer. Append/incremental dests are not the same
    population as the source — auto-running would false-diverge every night
    (the Full Append dest-Δ lesson). On-demand checks still run via the API.

    Column-profile screening runs first; Gate-8 from this job is recorded
    last so the campaign tail is the snapshot compare (Google Dual Run
    output identity), not the KPI screen.
    """
    from services.continuous_fidelity import (
        population_comparable,
        record_gate8_cycle,
        run_and_record_campaign,
    )

    if not population_comparable(getattr(sched, "sync_mode", None)):
        return
    src = _resolve_connector(sched.source_connector_id)
    dst = _resolve_connector(sched.dest_connector_id)
    if not src or not dst:
        return
    campaign = dict(getattr(sched, "fidelity_campaign", None) or {})
    try:
        request = build_schedule_request(sched, src, dst)
        _report, campaign = run_and_record_campaign(
            campaign,
            source=request.source,
            destination=request.destination,
            mappings=list(getattr(request, "mappings", None) or []),
            workspace_id=str(getattr(sched, "workspace_id", "") or ""),
        )
    except Exception as exc:
        logger.info(
            "Schedule %s parallel-run profile skipped: %s",
            getattr(sched, "id", ""),
            exc,
        )
    try:
        recon = (job_doc or {}).get("reconciliation")
        campaign = record_gate8_cycle(campaign, recon if isinstance(recon, dict) else None)
    except Exception as exc:
        logger.info(
            "Schedule %s Gate-8 Dual Run cycle skipped: %s",
            getattr(sched, "id", ""),
            exc,
        )
    try:
        from services.schedule_store import update_schedule

        update_schedule(
            str(getattr(sched, "id", "")),
            {"fidelity_campaign": campaign},
        )
    except Exception as exc:
        logger.info(
            "Schedule %s parallel-run observation skipped: %s",
            getattr(sched, "id", ""),
            exc,
        )


def _finalize_run(schedule_id: str, job_id: str, attempt: int, started_at: datetime) -> None:
    """Handle a finished scheduled run: retry on failure, else record + notify."""
    from services.schedule_store import (
        get_schedule,
        mark_schedule_run,
        schedule_retry,
    )

    sched = get_schedule(schedule_id)
    if not sched:
        return
    job_doc = _job_doc(job_id)
    status = (job_doc or {}).get("status") or "failed"
    entry = _run_entry(job_id, status, attempt, started_at, job_doc)

    from services.execution_engine_contract import committed_rows_of

    rows_committed, rows_known = committed_rows_of(job_doc)
    decision = _retry_decision(
        status,
        attempt,
        sched.max_retries,
        sync_mode=_normalize_sync_mode(sched.sync_mode, sched.primary_key),
        rows_committed=rows_committed,
        rows_committed_known=rows_known,
        job_doc=job_doc,
    )
    if not decision["retry"] and decision["reason"] and not _is_success(status):
        entry["retry_refused"] = decision["reason"]
    failure_class = decision.get("failure_class")
    if failure_class and not _is_success(status):
        entry["failure_class"] = failure_class

    if decision["retry"]:
        delay = max(0, sched.retry_backoff_seconds) * (attempt + 1)
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
        logger.warning(
            "Schedule %s attempt %s failed; retrying at %s",
            schedule_id,
            attempt + 1,
            retry_at.isoformat(),
        )
        schedule_retry(
            schedule_id,
            retry_at=retry_at,
            attempt=attempt + 1,
            run_entry={**entry, "retry_scheduled": True, "retry_at": retry_at.isoformat()},
        )
        return

    cursor_value = None
    if _is_success(status) and sched.cursor_column:
        cursor_value = (job_doc or {}).get("cursor_value")
    mark_schedule_run(
        schedule_id, job_id, status=status, run_entry=entry, cursor_value=cursor_value
    )
    if _is_success(status):
        _observe_parallel_run(sched, job_doc)
    elif failure_class == DETERMINISTIC:
        # A gate said no. It will say the same thing to every later beat, so the
        # schedule is parked on the finding instead of failing nightly forever.
        _open_finding(
            schedule_id,
            str((job_doc or {}).get("error") or decision.get("reason") or "")
            or "The run was refused by a validation gate.",
            attempt=attempt,
            job_id=job_id,
            evidence={"failure_class": failure_class, "phase": (job_doc or {}).get("phase", "")},
        )
    _notify_schedule(sched, job_id, status, job_doc)


def _park_on_decision(
    schedule_id: str,
    exc: BaseException,
    *,
    attempt: int,
    job_id: str = "",
) -> None:
    """Turn a pre-run refusal into a decision a human can act on.

    The run is still recorded as failed — nothing here pretends a refused run
    succeeded — but the schedule is additionally parked on one durable finding, so
    the cadence stops replaying an answer that cannot change by itself.

    Best effort: if the inbox write fails, the failed run is already recorded and
    the schedule behaves exactly as it did before this existed.
    """
    from services.schedule_store import mark_schedule_run

    message = str(exc)
    now = datetime.now(timezone.utc).isoformat()
    mark_schedule_run(
        schedule_id,
        job_id,
        status="failed",
        run_entry={
            "job_id": job_id,
            "status": "failed",
            "attempt": attempt,
            "started_at": now,
            "finished_at": now,
            "duration_seconds": 0,
            "records_transferred": 0,
            "rejected_rows": 0,
            "coerced_null_rows": 0,
            "error": message[:500],
        },
    )
    _open_finding(schedule_id, exc, attempt=attempt, job_id=job_id)


def _open_finding(
    schedule_id: str,
    exc: BaseException | str,
    *,
    attempt: int,
    job_id: str = "",
    evidence: dict[str, Any] | None = None,
) -> None:
    """Park the schedule on one finding, describing it in the operator's terms.

    A structured ``ApprovalRequired`` already knows its code, scopes and corrective
    action. Anything else is classified from its message, and only findings an
    attestation could actually clear are offered as approvable — the rest name the
    configuration change that has to happen instead.
    """
    from services.failure_retry_policy import classify_failure
    from services.schedule_approvals import (
        ApprovalRequired,
        KIND_RUN_REFUSED,
        build_approval_request,
        open_approval_request,
    )
    from services.schedule_store import get_schedule
    from services.standing_authorization import delegable_scopes_for

    message = str(exc)
    try:
        sched = get_schedule(schedule_id)
        if not sched:
            return
        if isinstance(exc, ApprovalRequired):
            kind, code, scopes = exc.kind, exc.code, exc.scopes
            corrective = exc.corrective_action
            found = {**exc.evidence, **(evidence or {})}
        else:
            classification = classify_failure(error=message, phase="validate")
            kind, code = KIND_RUN_REFUSED, "RUN_REFUSED"
            scopes = delegable_scopes_for(message)
            corrective = classification.corrective_action
            found = {"failure_class": classification.kind, **(evidence or {})}
        open_approval_request(
            schedule_id,
            build_approval_request(
                kind=kind,
                code=code,
                finding=message[:1000],
                corrective_action=corrective,
                binding=binding_from_schedule(sched),
                requested_scopes=scopes,
                job_id=job_id,
                run_attempt=attempt,
                evidence=found,
            ),
        )
    except Exception:
        logger.exception("Schedule %s could not be parked on a decision", schedule_id)


def _record_authorization_use(schedule_id: str, *, rebind: bool = False) -> None:
    from services.schedule_approvals import record_authorization_use

    try:
        record_authorization_use(schedule_id, rebind=rebind)
    except Exception:
        # Bookkeeping must never fail an authorized run.
        logger.exception("Schedule %s authorization use not recorded", schedule_id)


def _dispatch_transfer(schedule_id: str, attempt: int = 0) -> str | None:
    """Build and submit the transfer for a schedule attempt (used for retries too)."""
    from src.transfer.background import run_transfer_async
    from src.transfer.engine import get_transfer_engine

    from services.schedule_store import get_schedule, mark_schedule_run

    sched = get_schedule(schedule_id)
    if not sched or not sched.enabled:
        return None
    src = _resolve_connector(sched.source_connector_id)
    dst = _resolve_connector(sched.dest_connector_id)
    if not src or not dst:
        logger.warning("Schedule %s skipped — connector missing", schedule_id)
        now = datetime.now(timezone.utc)
        mark_schedule_run(
            schedule_id,
            "",
            status="failed",
            run_entry={
                "job_id": "",
                "status": "failed",
                "attempt": attempt,
                "started_at": now.isoformat(),
                "finished_at": now.isoformat(),
                "duration_seconds": 0,
                "records_transferred": 0,
                "rejected_rows": 0,
                "coerced_null_rows": 0,
                "error": "Schedule skipped — source or destination connector is missing or unavailable",
            },
        )
        return None

    authorized_drift = False
    try:
        request = build_schedule_request(sched, src, dst)
        authorized_drift = _guard_source_schema_drift(sched, request)
    except ValueError as exc:
        logger.error("Schedule %s refused before any row moved: %s", schedule_id, exc)
        _park_on_decision(schedule_id, exc, attempt=attempt)
        return None
    engine = get_transfer_engine()
    job_id = engine._create_pending_job(request)
    started_at = datetime.now(timezone.utc)
    from services.schedule_store import set_running_job

    # Bind the claim to this job so a long migration keeps it and a crashed one
    # gives it back, instead of both being judged by the same wall clock.
    set_running_job(schedule_id, job_id)
    if request.acknowledgment_actor or authorized_drift:
        # Delegated authority was exercised, and only now that a run actually
        # exists to exercise it: counting earlier would spend a single-use
        # approval on a job that never started.
        _record_authorization_use(schedule_id, rebind=authorized_drift)
    future = run_transfer_async(job_id, request)
    future.add_done_callback(
        lambda _f, sid=schedule_id, jid=job_id, a=attempt, ts=started_at: _finalize_run(sid, jid, a, ts)
    )
    logger.info("Schedule %s started job %s (attempt %s)", schedule_id, job_id, attempt + 1)
    return job_id


def _run_schedule(schedule_id: str) -> str | None:
    from services.schedule_store import (
        clear_schedule_running,
        get_schedule,
        mark_schedule_running,
    )

    sched = get_schedule(schedule_id)
    if not sched or not sched.enabled:
        return None

    # Concurrency guard: refuse to start when this schedule (or another schedule
    # for the same source→dest connector pair) already has a live run in flight.
    if mark_schedule_running(schedule_id, _scheduler_instance_id()) is None:
        logger.info("Schedule %s skipped — a run is already in progress", schedule_id)
        return None

    # A parked retry resumes its own attempt count; the budget is per run, not
    # per beat, or a schedule that fails every time retries forever.
    job_id = _dispatch_transfer(schedule_id, attempt=sched.retry_attempt if sched.retry_at else 0)
    if job_id is None:
        # Fail-closed paths (missing connector / contract) already call
        # mark_schedule_run which clears ``running``. Belt-and-suspenders clear.
        clear_schedule_running(schedule_id)
    return job_id


def _run_due_schedules() -> int:
    if not _acquire_scheduler_lock():
        logger.debug("Scheduler lock held by another instance; skipping this beat")
        return 0
    try:
        started = 0
        for sched in due_schedules():
            try:
                if _run_schedule(sched.id):
                    started += 1
            except Exception:
                logger.exception("Failed to run schedule %s", sched.id)
        return started
    finally:
        _release_scheduler_lock()


def _clear_stale_running_schedules() -> None:
    """On startup, clear running flags left by a previous crashed instance."""
    from services.schedule_store import (
        PipelineSchedule,
        _is_running_stale,
        _load_all,
        _save_all,
    )

    schedules = _load_all()
    changed = False
    for i, s in enumerate(schedules):
        if s.running and _is_running_stale(s):
            schedules[i] = PipelineSchedule.from_dict({
                **s.to_dict(),
                "running": False,
                "running_instance": "",
                "running_started_at": None,
            })
            changed = True
    if changed:
        _save_all(schedules)


async def run_schedule_loop() -> None:
    """Poll for due schedules and enqueue transfers."""
    logger.info("Pipeline scheduler started (interval=%ss)", CHECK_INTERVAL_SECONDS)
    await asyncio.get_event_loop().run_in_executor(_executor, _clear_stale_running_schedules)
    try:
        from services.schedule_store import import_file_schedules_into_mongo

        imported = await asyncio.get_event_loop().run_in_executor(
            _executor, import_file_schedules_into_mongo
        )
        if imported:
            logger.info("Imported %s schedule(s) from schedules.json into MongoDB", imported)
    except Exception:
        logger.exception("Schedule file→Mongo import failed")
    while True:
        try:
            count = await asyncio.get_event_loop().run_in_executor(_executor, _run_due_schedules)
            if count:
                logger.info("Scheduler started %s pipeline run(s)", count)
        except Exception:
            logger.exception("Schedule loop error")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)
