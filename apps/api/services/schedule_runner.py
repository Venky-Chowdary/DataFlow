"""Background scheduler — runs due pipeline syncs."""

from __future__ import annotations

import asyncio
import contextvars
import functools
import logging
import os
import re
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

from services.brand_env import getenv_brand
from services.failure_retry_policy import DETERMINISTIC, TRANSIENT
from services.ops_events import emit_event
from services.schedule_store import due_schedules
from services.standing_authorization import (
    CODE_NO_AUTHORIZATION,
    SCOPE_NET_ADDITIVE_DRIFT,
    SCOPE_SCHEMA_DRIFT,
    AuthorizationDecision,
    binding_from_schedule,
    evaluate_authorization,
)

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="schedule-runner")


class ScheduleStartError(Exception):
    """A manual Run now could not start — carry the operator-visible reason.

    Cadence beats still return ``None`` and stay quiet. The HTTP / Pilot
    trigger must not collapse every refusal into "check connectors".
    """

    def __init__(self, message: str, *, http_status: int = 400, code: str = ""):
        super().__init__(message)
        self.http_status = int(http_status)
        self.code = str(code or "")


def _manual_start_failure_detail(sched: Any) -> str:
    """Prefer the parked finding or last run error over a connector guess."""
    if sched is None:
        return "Schedule not found."
    hist = [
        r for r in (getattr(sched, "run_history", None) or [])
        if isinstance(r, dict) and str(r.get("status") or "") not in ("pending", "running")
    ]
    last_err = ""
    if hist:
        last_err = str(hist[-1].get("error") or hist[-1].get("reason") or "").strip()
    req = getattr(sched, "approval_request", None) or {}
    finding = ""
    if isinstance(req, dict) and str(req.get("status") or "") == "open":
        finding = str(req.get("finding") or req.get("corrective_action") or "").strip()
    return finding or last_err or (
        "Could not start this schedule. Open the schedule for the parked finding, "
        "or confirm the source and destination connectors still resolve."
    )
CHECK_INTERVAL_SECONDS = 60
LOCK_TTL_SECONDS = int(getenv_brand("SCHEDULER_LOCK_TTL", "300"))
# Failed beats in a row before the cadence parks on a finding whatever the
# failure was classified as. Two is the benefit of the doubt; the third is a
# pattern, and every beat after it only costs the operator another identical row.
_CONSECUTIVE_FAILURE_PARK = 3
# A transient fault (connection reset, timeout, 5xx) is never parked on a human
# decision: nothing a person approves changes it, and an approval park nulls
# the visible next run until someone notices (QA r12 Mongo D4). It backs off
# instead — doubling from this base per failed beat in a row, capped — so the
# cadence neither grinds nor stalls.
_TRANSIENT_BACKOFF_BASE_SECONDS = 60
_TRANSIENT_BACKOFF_CAP_SECONDS = 3600


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
            emit_event(logger, "scheduler.lock.acquire", outcome="failed", reason_code="backend_unavailable")
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
        acquired = bool(result and result.get("instance") == instance)
        if not acquired:
            emit_event(logger, "scheduler.lock.acquire", outcome="failed", reason_code="held_by_other")
        return acquired
    except Exception as exc:
        name = type(exc).__name__
        if "DuplicateKey" in name:
            emit_event(logger, "scheduler.lock.acquire", outcome="failed", reason_code="race")
            return False
        emit_event(logger, "scheduler.lock.acquire", outcome="errored", reason_code="lock_error", error=exc)
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


def _resolve_connector_for_schedule(connector_id: str, sched: Any) -> dict | None:
    """Current settings of a connector a schedule uses, inside its workspace (D172).

    A schedule stores connector ids, never hosts, so every fire reads the
    connector as it is now. A connector that belongs to another workspace is
    treated as absent - same rule as ``connector_store.get_connector``
    (matching workspace, or a legacy unscoped connector).
    """
    conn = _resolve_connector(connector_id)
    workspace = str(getattr(sched, "workspace_id", "") or "").strip()
    if conn and workspace:
        owner = str(conn.get("workspace_id") or "").strip() if isinstance(conn, dict) else ""
        if owner and owner != workspace:
            return None
    return conn


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

    extra = dict(conn.get("extra") or {}) if isinstance(conn.get("extra"), dict) else {}
    # SFTP host-key trust lives on the saved connector. Dropping it here
    # made a scheduled beat open an unpinned transport while Studio Test
    # had just verified against the pin.
    from connectors.sftp_common import host_key_settings

    extra.update({k: v for k, v in host_key_settings({**extra, **conn}).items() if v})

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
        private_key=str(conn.get("private_key") or ""),
        extra=extra,
    )


def _normalize_sync_mode(sync_mode: str, primary_key: str) -> str:
    """Map the schedule's coarse sync_mode onto the engine's contract vocabulary."""
    mode = (sync_mode or "full_refresh_overwrite").strip().lower().replace("-", "_").replace(" ", "_")
    # Bare "incremental" is the schedule spelling. A primary key makes the
    # write idempotent; without one the read stays append. Other tokens,
    # including a stored incremental_upsert, go through the sync-mode SSOT.
    if mode == "incremental":
        return "incremental_deduped" if primary_key else "incremental_append"
    from services.sync_cursor import normalize_sync_mode

    return normalize_sync_mode(mode)


def build_schedule_request(
    sched, src: dict, dst: dict, *, resync_mode: str = "", triggered_by: str = ""
):
    """Build a :class:`TransferRequest` from a persisted schedule.

    Threads the per-schedule sync mode, validation mode, mappings, and any
    watermark/primary-key contract so scheduled runs can be incremental/CDC
    instead of always full-refresh. Backward compatible: schedules created before
    these fields existed fall back to full_refresh_overwrite / strict.
    """
    from src.transfer.models import TransferRequest

    source = _endpoint_from_connector(src, sched.source_table)
    destination = _endpoint_from_connector(dst, sched.dest_table)
    src_extra = dict(getattr(source, "extra", None) or {})
    src_extra["file_wait_seconds"] = int(getattr(sched, "file_wait_seconds", 0) or 0)
    from services.duplicate_policy import normalize_duplicate_policy

    policy = normalize_duplicate_policy(
        str(getattr(sched, "duplicate_policy", "") or "")
    )
    schedule_pk = str(getattr(sched, "primary_key", "") or "").strip()
    if not policy and schedule_pk:
        # A keyed schedule defaults to upsert. A one-shot transfer that never
        # sets duplicate_policy still inserts (2N). This default is the schedule.
        policy = "upsert"
    src_extra["duplicate_policy"] = policy
    in_file = str(getattr(sched, "in_file_dedupe", "") or "block").strip().lower()
    src_extra["in_file_dedupe"] = in_file
    sched_id = str(getattr(sched, "id", "") or "")
    if sched_id:
        src_extra["schedule_id"] = sched_id
    source.extra = src_extra
    dst_extra = dict(getattr(destination, "extra", None) or {})
    if policy:
        dst_extra["duplicate_policy"] = policy
    dst_extra["in_file_dedupe"] = in_file
    if sched_id:
        dst_extra["schedule_id"] = sched_id
    if schedule_pk:
        dst_extra["primary_key"] = schedule_pk
    destination.extra = dst_extra
    _apply_callable_schedule_source(source, sched)
    from services.schedule_cdc_extras import apply_cdc_schedule_extras

    apply_cdc_schedule_extras(source, destination, sched)

    effective_mode = _normalize_sync_mode(sched.sync_mode, sched.primary_key)
    from services.procedure_source import assert_callable_sync_allowed

    assert_callable_sync_allowed(effective_mode, source)
    stream_contracts = list(sched.stream_contracts or [])
    snapshot_mode = ""
    if effective_mode == "cdc":
        from services.cdc_snapshot_mode import schedule_snapshot_mode

        snapshot_mode = schedule_snapshot_mode(
            "cdc",
            getattr(sched, "snapshot_mode", "")
            or next(
                (
                    str(c.get("snapshot_mode") or "")
                    for c in stream_contracts
                    if isinstance(c, dict) and c.get("snapshot_mode")
                ),
                "initial",
            ),
        )
    def _declared_key(raw: object) -> str:
        if not isinstance(raw, dict):
            return ""
        value = raw.get("primary_key")
        if value is None or (isinstance(value, str) and not str(value).strip()):
            value = raw.get("primary_keys")
        if isinstance(value, (list, tuple)):
            return ",".join(str(part).strip() for part in value if str(part).strip())
        return str(value or "").strip()

    # The key rides on the contract for every sync mode, including full refresh.
    # Dropping it there made upsert and skip_duplicates insert (60 rows / 50 keys).
    if not stream_contracts and (
        effective_mode not in ("full_refresh_overwrite", "full_refresh_append")
        or schedule_pk
    ):
        stream_contracts = [{
            "selected": True,
            "name": sched.source_table,
            "stream": sched.source_table,
            "sync_mode": effective_mode,
            "cursor_field": sched.cursor_column,
            "cursor_semantics": sched.cursor_semantics,
            "primary_key": schedule_pk,
            "schema_policy": sched.schema_policy,
            "validation_mode": sched.validation_mode,
            **({"snapshot_mode": snapshot_mode} if snapshot_mode else {}),
        }]
    elif stream_contracts and (snapshot_mode or sched.cursor_semantics or schedule_pk):
        stamped: list[dict] = []
        for raw in stream_contracts:
            row = dict(raw) if isinstance(raw, dict) else {}
            if snapshot_mode:
                row.setdefault("snapshot_mode", snapshot_mode)
            # A schedule-level declaration fills streams that did not declare
            # their own; a stream's own word about its cursor is never replaced.
            if sched.cursor_semantics and not str(row.get("cursor_semantics") or "").strip():
                row["cursor_semantics"] = sched.cursor_semantics
            if schedule_pk and not _declared_key(row):
                row["primary_key"] = schedule_pk
            stamped.append(row)
        stream_contracts = stamped
    if policy in {"upsert", "dedupe_skip"} and not (
        schedule_pk or any(_declared_key(row) for row in stream_contracts)
    ):
        from services.duplicate_policy import DuplicatePolicyError

        raise DuplicatePolicyError(
            "Upsert and skip_duplicates need a primary key. "
            "Declare one, or choose append to allow duplicate rows."
        )

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

    from services.schedule_mapping_contract import assert_schedule_mappings_replayable

    mappings = assert_schedule_mappings_replayable(sched.mappings)
    schema_policy = sched.schema_policy or "manual_review"
    sched_shape_recipe = (
        dict(getattr(sched, "shape_recipe", None) or {})
        if isinstance(getattr(sched, "shape_recipe", None), dict)
        else {}
    )
    if (
        str(getattr(sched, "transform_recipe_id", "") or "").strip()
        and str(getattr(sched, "transform_recipe_version_mode", "") or "")
        .strip()
        .lower()
        == "follow"
    ):
        # Follow-latest: resolve the newest library version for this run. If
        # it compiles to a different recipe than Validate approved, the
        # engine's hash gate refuses — the upgrade is visible, never silent.
        from services.transform_binding import resolve_recipe_binding

        bound = resolve_recipe_binding(
            sched.transform_recipe_id,
            params=getattr(sched, "transform_recipe_params", None),
            workspace_id=sched.workspace_id or None,
        )
        sched_shape_recipe = bound.payload
    # Autopilot: a scheduled run has nobody at the keyboard, so it carries the
    # attestations a named human signed in advance — and only while the plan they
    # signed is still the plan being run. With no valid grant these stay False and
    # every gate decides exactly as it did before.
    decision = authorization_for(sched)
    acks = decision.acknowledgments
    request = TransferRequest(
        source=source,
        destination=destination,
        mappings=mappings,
        skip_preflight=False,
        sync_mode=effective_mode,
        schema_policy=schema_policy,
        validation_mode=sched.validation_mode or "strict",
        delivery_guarantee=getattr(sched, "delivery_guarantee", None)
        or "at_least_once",
        # The stored mappings were stamped create_new by the run that created
        # the table; they do not arm ADD COLUMN for this one. Execute derives
        # that from the live destination.
        backfill_new_fields=effective_backfill_new_fields(
            backfill_new_fields=bool(sched.backfill_new_fields),
            schema_policy=schema_policy,
        ),
        write_via_staging=bool(getattr(sched, "write_via_staging", False)),
        priority_column=str(getattr(sched, "priority_column", "") or ""),
        priority_direction=(
            "asc"
            if str(getattr(sched, "priority_direction", "") or "").strip().lower() == "asc"
            else "desc"
        ),
        limit=max(0, int(getattr(sched, "row_limit", 0) or 0)),
        stream_contracts=stream_contracts,
        duplicate_policy=policy,
        date_locale=str(getattr(sched, "date_locale", "") or ""),
        number_locale=str(getattr(sched, "number_locale", "") or ""),
        shape_recipe=sched_shape_recipe,
        approved_shape_recipe_hash=str(
            getattr(sched, "approved_shape_recipe_hash", "") or ""
        ),
        approved_set_hash=str(getattr(sched, "approved_set_hash", "") or ""),
        approved_decision_artifact_hash=str(
            getattr(sched, "approved_decision_artifact_hash", "") or ""
        ),
        approved_ddl_identity_hash=str(
            getattr(sched, "approved_ddl_identity_hash", "") or ""
        ),
        workspace_id=sched.workspace_id or "",
        contract_id=contract_id,
        enforce_contract=bool(contract_id),
        require_signed_contract=require_signed,
        compliance_acknowledged=(
            acks.compliance
            or bool(getattr(sched, "compliance_acknowledged", False))
        ),
        schema_drift_acknowledged=(
            acks.schema_drift
            or bool(getattr(sched, "schema_drift_acknowledged", False))
        ),
        fk_risk_acknowledged=acks.fk_risk,
        acknowledgment_actor=(
            acks.actor
            if acks.any_claimed
            else str(getattr(sched, "acknowledgment_actor", "") or "")
        ),
        acknowledgment_reason=(
            acks.reason
            if acks.any_claimed
            else str(getattr(sched, "acknowledgment_reason", "") or "")
        ),
        # D6: every schedule-fired job names its trigger — a blank
        # triggered_by erases the audit trail.
        triggered_by=(
            (triggered_by or "").strip()
            or f"schedule:{str(getattr(sched, 'name', '') or getattr(sched, 'id', '') or 'unknown')}"
        ),
    )
    if resync_mode in {"sync", "full"}:
        from services.sync_now import apply_resync_to_request

        apply_resync_to_request(request, resync_mode)
    return request


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
    current = _schema_types_from_probe(info)
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
        # Source-vs-source: MySQL FSP 0 must not classify a Snowflake
        # TIMESTAMP_NTZ against itself as narrow_type.
        source_db=str(getattr(request.source, "format", "") or ""),
        dest_db="",
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
                # The shape that was compared — Accept records this, it does
                # not hang a second live introspect on a sleeping warehouse.
                "current_schema": dict(current),
                "current_columns": list(info.get("columns") or current.keys())[:200],
                "current_primary_key": list(current_pk),
            },
        )
    # An unmapped new column is REVIEW, not BLOCK: the nightly load used to
    # record the new shape as the baseline and then fail inside the job.
    # SFTP files hit that path — the pre-run raise above never ran — and the
    # in-job sentence was parked as RUN_REFUSED, or not parked at all on a
    # manual run. Refuse here, for both, and leave the baseline alone.
    unmapped = _unmapped_added_columns(sched, request, verdict)
    if unmapped:
        named = ", ".join(unmapped[:8])
        extra = f" (+{len(unmapped) - 8} more)" if len(unmapped) > 8 else ""
        from services.drift_approval import DRIFT_CORRECTIVE, SOURCE_DRIFT_CODE

        raise ApprovalRequired(
            f"{len(unmapped)} source column(s) are neither mapped nor declared "
            f"omitted: {named}{extra}",
            kind=KIND_SOURCE_DRIFT,
            code=SOURCE_DRIFT_CODE,
            corrective_action=DRIFT_CORRECTIVE,
            scopes=(SCOPE_SCHEMA_DRIFT,),
            evidence={
                "summary": verdict.summary,
                "compatibility": verdict.compatibility,
                "additive": list(verdict.additive or [])[:20],
                "unmapped_columns": unmapped[:50],
                "source_schema_fingerprint": verdict.fingerprint,
                "current_schema": dict(current),
                "current_columns": list(info.get("columns") or current.keys())[:200],
                "current_primary_key": list(current_pk),
            },
        )
    _remember_source_schema(
        sched, current, verdict.fingerprint, primary_key=current_pk
    )
    return False


def _schema_types_from_probe(info: dict[str, Any]) -> dict[str, str]:
    """Flat column→type map from a schedule source probe.

    File probes sometimes nest the type (``{"type": "string"}``). Dropping
    those left the guard with an empty shape, so an SFTP file never compared
    against its baseline. A header list with no types is still a shape:
    VARCHAR is the placeholder the file reader already uses.
    """
    out: dict[str, str] = {}
    for key, value in dict((info or {}).get("schema") or {}).items():
        name = str(key).strip()
        if not name or name.startswith("_"):
            continue
        if isinstance(value, dict):
            inner = value.get("type") or value.get("data_type") or value.get("dataType")
            if inner is None or isinstance(inner, (dict, list)):
                inner = "VARCHAR"
            out[name] = str(inner)
        elif not isinstance(value, list):
            out[name] = str(value)
    if out:
        return out
    return {
        str(column).strip(): "VARCHAR"
        for column in ((info or {}).get("columns") or [])
        if str(column).strip()
    }


def _unmapped_added_columns(sched: Any, request: Any, verdict: Any) -> list[str]:
    """New source columns nobody mapped, under a policy that does not propagate."""
    from services.schema_drift import PROPAGATE_POLICIES, effective_schema_policy
    from services.source_schema_memory import REVIEW, mapped_source_columns

    # A backfill request is a propagate request (see effective_schema_policy).
    policy = effective_schema_policy(
        str(getattr(sched, "schema_policy", "") or "manual_review"),
        backfill_new_fields=bool(getattr(sched, "backfill_new_fields", False)),
    )
    if getattr(verdict, "verdict", "") != REVIEW or policy in PROPAGATE_POLICIES:
        return []
    from services.mapping_constraints import is_intentional_omit

    mappings = list(getattr(request, "mappings", None) or [])
    # A declared omission is the operator's decision, the same as G13 reads it.
    accounted = mapped_source_columns(mappings) | {
        str(m.get("source") or "").strip().lower()
        for m in mappings
        if isinstance(m, dict) and is_intentional_omit(m)
    }
    names: list[str] = []
    for entry in getattr(verdict, "additive", None) or []:
        if not isinstance(entry, dict) or str(entry.get("kind") or "") != "add_column":
            continue
        name = str(entry.get("column") or "").strip()
        if name and name.lower() not in accounted:
            names.append(name)
    return names


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
    hist = doc.get("load_history_report")
    if not isinstance(hist, dict):
        dest = doc.get("destination_summary")
        hist = dest.get("load_history_report") if isinstance(dest, dict) else None
    entry = {
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
    if doc.get("catch_up"):
        entry["catch_up"] = True
    if doc.get("_run_reason_code"):
        entry["reason_code"] = doc["_run_reason_code"]
    elif doc.get("error_code") and not _is_success(status):
        entry["reason_code"] = str(doc["error_code"])
    if _is_success(status) and doc.get("message"):
        entry["reason"] = str(doc.get("message") or "")[:500]
    if isinstance(hist, dict) and hist:
        entry["load_history_report"] = hist
    return entry


def _notify_schedule(sched, job_id: str, status: str, job_doc: dict | None) -> None:
    """Deliver success/failure notifications honoring per-schedule preferences.

    A run that failed before a job existed passes ``{"error": <run reason>}``
    as ``job_doc``: there is no job error to quote.
    """
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

        from services.schedule_run_lifecycle import current_run_id
        from services.schedule_store import get_schedule, has_open_approval

        # Re-read: the caller's copy predates the finding this run may have
        # just opened, and the alert must say whether the pipeline is parked.
        sched = get_schedule(sched.id) or sched
        run_id = current_run_id(sched.id) or next(
            (
                str(r.get("run_id") or "")
                for r in reversed(sched.run_history or [])
                if isinstance(r, dict) and (not job_id or r.get("job_id") == job_id)
            ),
            "",
        )
        request = sched.approval_request if isinstance(sched.approval_request, dict) else {}
        parked = has_open_approval(sched)
        doc = job_doc or {}
        payload = build_job_payload(
            job_id=job_id,
            status=status,
            source=sched.source_table or sched.source_connector_id,
            destination=sched.dest_table or sched.dest_connector_id,
            records_transferred=int(doc.get("records_processed", 0) or 0),
            rejected_rows=int(doc.get("rejected_rows", 0) or 0),
            error=doc.get("error") or "",
            retry_url=(
                f"/api/v1/connectors/jobs/{job_id}/resume"
                if job_id
                else f"/api/v1/schedules/{sched.id}/history"
            ),
            workspace_id=sched.workspace_id or "",
            base_url=public_url(),
            web_url=web_url(),
        )
        # The schedule-aware alert: which pipeline, and whether it is now
        # parked waiting on a person (and on what).
        payload["schedule_id"] = sched.id
        payload["schedule_name"] = sched.name or ""
        payload["parked"] = bool(parked)
        payload["approval_code"] = str(request.get("code") or "") if parked else ""
        payload["title"] = f"Pipeline “{sched.name or sched.id}” run {status}" + (
            " — parked, needs a decision" if parked else ""
        )
        payload["text"] = (
            f"Pipeline: {sched.name or sched.id}\n"
            + (f"Parked: {payload['approval_code'] or 'waiting on a decision'}\n" if parked else "")
            + payload.get("text", "")
        )
        from services.notification_delivery_log import delivery_context

        with delivery_context(
            schedule_id=sched.id,
            run_id=run_id,
            job_id=job_id,
            event=f"schedule.run.{'succeeded' if success else 'failed'}",
        ):
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
    classification = classify_job_failure(job_doc)
    if not classification.retryable:
        # Name the gate before the budget. A schedule with max_retries=0 used
        # to park as "Retry budget exhausted after 0 attempt(s)" even when the
        # job error was a Decision Artifact refuse — that hid the next action.
        reason = classification.reason
        if classification.corrective_action:
            reason = f"{reason} {classification.corrective_action}"
        return {
            "retry": False,
            "reason": reason,
            "failure_class": classification.to_dict(),
        }
    if attempt >= max_retries:
        return {
            "retry": False,
            "reason": f"Retry budget exhausted after {max_retries} attempt(s).",
            "failure_class": classification.to_dict(),
        }
    decision = decide_retry_from_start(
        status=status,
        sync_mode=sync_mode,
        rows_committed=rows_committed,
        rows_committed_known=rows_committed_known,
        source_engine=str((job_doc or {}).get("source_format") or ""),
        destination_engine=str((job_doc or {}).get("destination_type") or ""),
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
    src = _resolve_connector_for_schedule(sched.source_connector_id, sched)
    dst = _resolve_connector_for_schedule(sched.dest_connector_id, sched)
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


def _failure_signature(entry: dict[str, Any] | None) -> str:
    """A comparable identity for *why* a run failed, free of ids and counts."""
    text = str((entry or {}).get("error") or "").strip().lower()
    # Row numbers, job ids and timestamps differ between two identical verdicts.
    text = re.sub(r"\b[0-9a-f]{16,}\b", "", text)
    text = re.sub(r"\d+", "#", text)
    return re.sub(r"\s+", " ", text)[:300]


def _park_reason(sched: Any, decision: dict[str, Any], entry: dict[str, Any]) -> str:
    """Why this failed beat must park instead of waiting for the next cadence.

    Three cases, all of which repeat forever on their own:

    - a gate refused the run (``deterministic``): the same inputs get the same
      verdict on every beat;
    - the retry was refused because the attempt already committed rows a
      from-zero re-run cannot collapse — the *cadence* re-runs from zero too, so
      every later beat appends the same rows again (this is how one failing
      append grew a destination 5 → 25 rows across four beats);
    - an unrecognised failure that has now produced the identical verdict twice
      in a row: one retry was the benefit of the doubt, a second identical
      verdict is evidence.
    """
    failure_class = decision.get("failure_class") or {}
    kind = str(failure_class.get("kind") or "") if isinstance(failure_class, dict) else str(failure_class)
    if kind == DETERMINISTIC:
        return "deterministic_refusal"
    if not decision.get("retry") and _missing_source_table(
        sched, str(entry.get("error") or entry.get("reason") or "")
    ):
        # The table this schedule reads is gone: no later beat can find it, so
        # park on one finding that names it (QA SC5) instead of grinding.
        return "source_table_missing"
    if not decision.get("retry") and (decision.get("decision") or {}).get("allowed") is False:
        return "committed_rows_cannot_be_replayed"
    # Finished runs only: a skipped beat or the record still open for this very
    # run is not a failure in a row.
    history = [
        r for r in (getattr(sched, "run_history", None) or [])
        if isinstance(r, dict)
        and str(r.get("status") or "") not in ("skipped", "pending", "running")
        and (not entry.get("run_id") or r.get("run_id") != entry.get("run_id"))
    ]
    signature = _failure_signature(entry)
    if signature:
        previous = [r for r in history if not _is_success(r.get("status"))]
        if previous and _failure_signature(previous[-1]) == signature:
            return "identical_failure_repeated"
    # A ceiling that does not depend on the wording matching. Two verdicts whose
    # text differs — a connection error naming a different replica, a gate that
    # names a different column each beat — used to grind on the cadence forever.
    consecutive = 1
    for run in reversed(history):
        if _is_success(run.get("status")):
            break
        consecutive += 1
    if consecutive >= _CONSECUTIVE_FAILURE_PARK:
        return "consecutive_failures"
    return ""


def _failure_kind(decision: dict[str, Any]) -> str:
    failure_class = decision.get("failure_class") or {}
    if isinstance(failure_class, dict):
        return str(failure_class.get("kind") or "")
    return str(failure_class or "")


def _back_off_transient(schedule_id: str, sched: Any, park: str) -> None:
    """Push the next beat out instead of parking a transient fault for approval.

    ``next_run_at`` stays set (the API shows when it fires next) and no approval
    request opens; the first success resets the run of failures.
    """
    from services.schedule_store import get_schedule, next_run_for, update_schedule

    current = get_schedule(schedule_id) or sched
    consecutive = 0
    for run in reversed(getattr(current, "run_history", None) or []):
        if not isinstance(run, dict) or str(run.get("status") or "") in (
            "skipped", "pending", "running",
        ):
            continue
        if _is_success(run.get("status")):
            break
        consecutive += 1
    delay = min(
        _TRANSIENT_BACKOFF_CAP_SECONDS,
        _TRANSIENT_BACKOFF_BASE_SECONDS * (2 ** max(0, consecutive - 1)),
    )
    now = datetime.now(timezone.utc)
    slot = next_run_for(current, now)
    backoff_at = now + timedelta(seconds=delay)
    due = slot
    try:
        slot_dt = datetime.fromisoformat(str(slot)) if slot else None
        if slot_dt is not None and slot_dt.tzinfo is None:
            slot_dt = slot_dt.replace(tzinfo=timezone.utc)
        if slot_dt is None or slot_dt < backoff_at:
            due = backoff_at.isoformat()
    except ValueError:
        due = backoff_at.isoformat()
    update_schedule(schedule_id, {"next_run_at": due, "retry_at": None, "retry_attempt": 0})
    emit_event(
        logger,
        "schedule.run.backoff",
        outcome="backing_off",
        schedule_id=schedule_id,
        reason_code=park,
        delay_seconds=delay,
    )


def _append_replay_blocked(sched: Any) -> str | None:
    """Why a from-zero start would duplicate committed append rows, or None.

    Cadence already skips an open finding. Run now must not silently replay
    the same from-zero append the retry contract just refused.
    """
    req = getattr(sched, "approval_request", None) or {}
    if not isinstance(req, dict) or str(req.get("status") or "").strip().lower() != "open":
        return None
    ev = req.get("evidence") if isinstance(req.get("evidence"), dict) else {}
    if str(ev.get("park_reason") or "") != "committed_rows_cannot_be_replayed":
        return None
    return (
        "This schedule already committed rows on an append. "
        "Retry from start would duplicate them — resume from the last "
        "committed checkpoint, or resolve the finding first."
    )


def _settle_if_worker_lost(
    job_id: str, job_doc: dict | None
) -> tuple[str, dict] | None:
    """Finalize was asked about a job that never reached a terminal status.

    Callers only get here when the job is not live (its lease and heartbeat
    expired, or the document is gone). Recording the job's own ``running`` as
    the run's result left ``last_status='running'`` for good. The job is failed
    "worker lost" instead, conditional on the status that was read.

    Returns ``None`` when the job is terminal (nothing to settle), ``("", {})``
    when it became live again, else the settled ``(status, job_doc)``.
    """
    from services.job_status import UNFINISHED_JOB_STATUSES
    from services.schedule_run_lifecycle import WORKER_LOST_CODE, is_open, settle_lost_job

    status = str((job_doc or {}).get("status") or "")
    if job_doc and status not in UNFINISHED_JOB_STATUSES:
        return None
    settled, reason = settle_lost_job(job_id, job_doc)
    if is_open(settled) or settled in UNFINISHED_JOB_STATUSES:
        return "", {}
    doc = dict(_job_doc(job_id) or job_doc or {})
    doc["status"] = settled
    doc["error"] = str(doc.get("error") or reason)
    doc.setdefault("error_code", WORKER_LOST_CODE)
    doc["_run_reason_code"] = WORKER_LOST_CODE if doc.get("error_code") == WORKER_LOST_CODE else ""
    return settled, doc


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
    from services.schedule_run_lifecycle import run_finished

    if job_id and run_finished(sched, job_id, success=_is_success(status)):
        # Already recorded with the same outcome (the done-callback and the
        # beat can both arrive).
        return
    lost = _settle_if_worker_lost(job_id, job_doc)
    if lost is not None:
        status, job_doc = lost
        if not status:
            return  # resumed or live again: the run is still in flight
    if (
        job_id
        and sched.last_job_id == job_id
        and not sched.running
        and _is_success(status) == _is_success(sched.last_status)
    ):
        return
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
        failure_class = decision.get("failure_class") or {}
        emit_event(
            logger,
            "schedule.run.retry",
            outcome="retrying",
            schedule_id=schedule_id,
            attempt=attempt + 1,
            classification=(
                str(failure_class.get("kind") or "other")
                if isinstance(failure_class, dict)
                else str(failure_class or "other")
            ),
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
        try:
            from services.schedule_approvals import (
                close_dest_exists_park_after_success,
                close_finding_after_successful_run,
            )

            close_finding_after_successful_run(schedule_id)
            close_dest_exists_park_after_success(schedule_id)
        except Exception:
            logger.exception(
                "Schedule %s dest-exists park close after success skipped",
                schedule_id,
            )
    else:
        if not _is_success(status):
            failure_class = decision.get("failure_class") or {}
            emit_event(
                logger,
                "schedule.run.give_up",
                outcome="failed",
                schedule_id=schedule_id,
                attempt=attempt + 1,
                classification=(
                    str(failure_class.get("kind") or "other")
                    if isinstance(failure_class, dict)
                    else str(failure_class or "other")
                ),
            )
        park = _park_reason(sched, decision, entry)
        if park and _failure_kind(decision) == TRANSIENT and park in (
            "identical_failure_repeated",
            "consecutive_failures",
        ):
            _back_off_transient(schedule_id, sched, park)
            park = ""
        if park:
            # This beat's verdict will not change by itself, and the next beat
            # would either replay it or — on an append with nothing to collapse
            # a second copy — land the same rows again. Park on one finding.
            from services.drift_approval import (
                gate_codes_from_job,
                unaccounted_sources_from_job,
            )

            _open_finding(
                schedule_id,
                str((job_doc or {}).get("error") or decision.get("reason") or "")
                or "The run was refused by a validation gate.",
                attempt=attempt,
                job_id=job_id,
                evidence={
                    "failure_class": failure_class,
                    "phase": (job_doc or {}).get("phase", ""),
                    "park_reason": park,
                    "gate_codes": gate_codes_from_job(job_doc),
                    "unmapped_columns": unaccounted_sources_from_job(job_doc),
                },
            )
    from services.schedule_run_lifecycle import default_reason

    doc = dict(job_doc or {})
    if not _is_success(status):
        doc["error"] = str(doc.get("error") or default_reason(entry))
    _notify_schedule(sched, job_id, status, doc)


def _park_on_decision(
    schedule_id: str,
    exc: BaseException,
    *,
    attempt: int,
    job_id: str = "",
    reason_code: str = "preflight_refused",
) -> None:
    """Turn a pre-run refusal into a decision a human can act on.

    The run is still recorded as failed — nothing here pretends a refused run
    succeeded — but the schedule is additionally parked on one durable finding, so
    the cadence stops replaying an answer that cannot change by itself.

    Best effort: if the inbox write fails, the failed run is already recorded and
    the schedule behaves exactly as it did before this existed.
    """
    from services.schedule_run_lifecycle import current_run_id
    from services.schedule_store import mark_schedule_run

    message = str(exc)
    now = datetime.now(timezone.utc).isoformat()
    updated = mark_schedule_run(
        schedule_id,
        job_id,
        status="failed",
        run_entry={
            "run_id": current_run_id(schedule_id),
            "reason": message[:500],
            "reason_code": reason_code,
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
    if updated is not None:
        # No job ran, so no job-level notification will ever say this failed.
        _notify_schedule(updated, job_id, "failed", {"error": message})


def _collapse_repeated_finding(message: str) -> str:
    """One drift sentence, even when Execute and the proof bundle both said it."""
    from services.blocker_titles import (
        collapse_operator_message,
        sanitize_operator_finding,
    )

    return collapse_operator_message(sanitize_operator_finding(message))


SOURCE_TABLE_MISSING_CODE = "SOURCE_TABLE_MISSING"


def _missing_source_table(sched: Any, message: str) -> str:
    """The schedule's source table when ``message`` says that table is missing."""
    from connectors.sql_identifiers import split_qualified_table
    from services.failure_retry_policy import missing_relation_name

    named = missing_relation_name(message)
    if named is None:
        return ""
    source = str(getattr(sched, "source_table", "") or "").strip()
    if not source or getattr(sched, "source_read_mode", "") in ("procedure", "query"):
        return ""
    _sch, wanted = split_qualified_table(source)
    if not named:
        return source  # the driver named no table; the run read only this one
    _nsch, got = split_qualified_table(named)
    return source if got.lower() == wanted.lower() else ""


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
    from services.blocker_titles import is_schema_drift_echo
    from services.drift_approval import SOURCE_DRIFT_CODE, open_schedule_drift_approval
    from services.schedule_approvals import (
        ApprovalRequired,
        KIND_RUN_REFUSED,
        KIND_SOURCE_DRIFT,
        build_approval_request,
        open_approval_request,
    )
    from services.schedule_mapping_contract import (
        EMPTY_MAPPING_CODE,
        EMPTY_MAPPING_CORRECTIVE,
        is_empty_mapping_refusal,
    )
    from services.schedule_store import get_schedule
    from services.standing_authorization import delegable_scopes_for

    message = _collapse_repeated_finding(str(exc))
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
            from services.drift_approval import (
                DRIFT_CORRECTIVE,
                BREAKING_CORRECTIVE,
                SOURCE_DRIFT_CODE,
                drift_codes,
                drift_is_breaking,
            )
            from services.standing_authorization import SCOPE_SCHEMA_DRIFT

            supplied = list((evidence or {}).get("gate_codes") or [])
            matched = drift_codes(supplied)
            from services.drift_approval import message_is_unmapped_column_drift

            unmapped_sentence = message_is_unmapped_column_drift(message)
            missing_table = _missing_source_table(sched, message)
            if is_empty_mapping_refusal(message):
                code = EMPTY_MAPPING_CODE
                scopes = ()
                corrective = EMPTY_MAPPING_CORRECTIVE
            elif missing_table:
                # No signature can create a table: name it and the fix (QA SC5).
                code = SOURCE_TABLE_MISSING_CODE
                scopes = ()
                corrective = (
                    f"Source table '{missing_table}' does not exist on the source "
                    "connector. Create it, or PATCH source_table to an existing "
                    "table, then resume the pipeline."
                )
                if missing_table not in message:
                    message = f"Source table '{missing_table}' does not exist. {message}"
            elif matched or unmapped_sentence:
                # The gate id is the classification. The coverage sentence
                # does not say "schema drift", and matching that phrase
                # parked additive columns as RUN_REFUSED.
                kind, code = KIND_SOURCE_DRIFT, SOURCE_DRIFT_CODE
                if drift_is_breaking(matched):
                    scopes = ()
                    corrective = BREAKING_CORRECTIVE
                else:
                    scopes = (SCOPE_SCHEMA_DRIFT,)
                    corrective = DRIFT_CORRECTIVE
            elif is_schema_drift_echo(message):
                kind, code = KIND_SOURCE_DRIFT, SOURCE_DRIFT_CODE
            found = {"failure_class": classification.kind, **(evidence or {})}
        request = build_approval_request(
            kind=kind,
            code=code,
            finding=message[:1000],
            corrective_action=corrective,
            binding=binding_from_schedule(sched),
            requested_scopes=scopes,
            job_id=job_id,
            run_attempt=attempt,
            evidence=found,
        )
        if code == SOURCE_DRIFT_CODE:
            # Preflight may already have parked this job. A second open
            # would count one refusal as two occurrences.
            open_schedule_drift_approval(schedule_id, request)
        else:
            open_approval_request(schedule_id, request)
    except Exception:
        logger.exception("Schedule %s could not be parked on a decision", schedule_id)


def _record_authorization_use(schedule_id: str, *, rebind: bool = False) -> None:
    from services.schedule_approvals import record_authorization_use

    try:
        record_authorization_use(schedule_id, rebind=rebind)
    except Exception:
        # Bookkeeping must never fail an authorized run.
        logger.exception("Schedule %s authorization use not recorded", schedule_id)


def _abandon_unclaimed_schedule_job(job_id: str) -> None:
    """A pending job that lost the schedule claim must not stay runnable."""
    if not str(job_id or "").strip():
        return
    try:
        from services.mongodb_service import get_mongodb_service

        get_mongodb_service().update_job_status(
            job_id,
            "cancelled",
            message="Schedule claim was already held; this attempt was not started.",
        )
    except Exception:
        logger.debug("unclaimed schedule job %s was not cancelled", job_id, exc_info=True)


def _dispatch_transfer(
    schedule_id: str,
    attempt: int = 0,
    *,
    allow_paused: bool = False,
    resync_mode: str = "",
    triggered_by: str = "",
) -> str | None:
    """Build and submit the transfer for a schedule attempt (used for retries too).

    ``enabled`` gates the cadence, not a one-shot Run now. A paused schedule
    still has connectors and mappings — Fivetran/Airbyte let the operator fire
    a paused connection without flipping it back to active.
    """
    from src.transfer.background import run_transfer_async
    from src.transfer.engine import get_transfer_engine

    from services.schedule_store import get_schedule

    sched = get_schedule(schedule_id)
    if not sched:
        return None
    if not sched.enabled and not allow_paused:
        return None
    src = _resolve_connector_for_schedule(sched.source_connector_id, sched)
    dst = _resolve_connector_for_schedule(sched.dest_connector_id, sched)
    if not src or not dst:
        # A connector this schedule cannot resolve does not come back on its own:
        # someone has to re-point the schedule or restore the connection. Every
        # later beat produced another identical failed row — 48 of them in one
        # customer's Jobs list — so this parks on one finding instead.
        logger.warning("Schedule %s skipped — connector missing", schedule_id)
        emit_event(logger, "schedule.run.skip", outcome="skipped", reason_code="connector_missing", schedule_id=schedule_id)
        missing = "source" if not src else "destination"
        missing_id = sched.source_connector_id if not src else sched.dest_connector_id
        _park_on_decision(
            schedule_id,
            RuntimeError(
                f"Schedule cannot run — Connector {missing_id} no longer exists: "
                f"its {missing} connector is missing or unavailable. Re-select "
                "the connection on this schedule, or restore the deleted "
                "connector, then resume."
            ),
            attempt=attempt,
            reason_code="connector_missing",
        )
        return None

    authorized_drift = False
    try:
        request = build_schedule_request(
            sched,
            src,
            dst,
            resync_mode=resync_mode,
            triggered_by=triggered_by,
        )
        authorized_drift = _guard_source_schema_drift(sched, request)
    except ValueError as exc:
        emit_event(logger, "schedule.run.fire", outcome="held", reason_code="preflight_refused", schedule_id=schedule_id, attempt=attempt + 1, error=exc)
        _park_on_decision(schedule_id, exc, attempt=attempt)
        return None
    from services.full_pass_rerun import apply_schedule_full_pass, full_pass_decision

    read_scope = apply_schedule_full_pass(request, sched, attempt=attempt)
    engine = get_transfer_engine()
    job_id = engine._create_pending_job(request)
    if read_scope:
        from services.mongodb_service import get_mongodb_service

        get_mongodb_service().update_job_status(
            job_id, "pending", read_scope=read_scope,
            retry_decision=full_pass_decision({"kind": "schedule_retry"}, read_scope),
        )
    started_at = datetime.now(timezone.utc)
    from services.schedule_store import (
        _CLAIM_LOCK,
        describe_claim_holder,
        get_schedule as _reload_schedule,
        last_claim_refusal,
        mark_schedule_running,
        set_running_job,
    )

    # The job id is part of the claim write. A follow-up update left a window
    # where the job existed and Run now still said "no job bound yet" (R14-001).
    with _CLAIM_LOCK:
        current = _reload_schedule(schedule_id)
        already = str(getattr(current, "running_job_id", "") or "").strip() if current else ""
        running = bool(getattr(current, "running", False)) if current else False
        if running and already and already != job_id:
            _abandon_unclaimed_schedule_job(job_id)
            holder = last_claim_refusal(schedule_id) or {
                "kind": "this_schedule",
                "schedule_id": schedule_id,
                "schedule_name": getattr(current, "name", "") or schedule_id,
                "job_id": already,
                "running_since": current.running_started_at,
            }
            if not str(holder.get("job_id") or ""):
                holder = {**holder, "job_id": already}
            raise ScheduleStartError(
                describe_claim_holder(holder),
                http_status=409,
                code="already_running",
            )
        if running and not already:
            # This process already took the claim, then created the job. Bind
            # that id in this same locked section.
            set_running_job(schedule_id, job_id)
        elif not (running and already == job_id):
            claimed = mark_schedule_running(
                schedule_id, _scheduler_instance_id(), job_id
            )
            if claimed is not None and getattr(claimed, "claimed_catch_up", False):
                # The claim consumed an operator-approved catch-up window —
                # stamp the job so the run record can say it was a replay.
                try:
                    from services.mongodb_service import get_mongodb_service

                    get_mongodb_service().update_job_fields(
                        job_id, {"catch_up": True}, touch_updated_at=False
                    )
                except Exception:
                    logger.debug(
                        "could not tag catch-up job %s", job_id, exc_info=True
                    )
            if claimed is None:
                fresh = _reload_schedule(schedule_id)
                bound = str(getattr(fresh, "running_job_id", "") or "") if fresh else ""
                fresh_running = bool(getattr(fresh, "running", False)) if fresh else False
                if fresh_running and bound and bound != job_id:
                    _abandon_unclaimed_schedule_job(job_id)
                    holder = last_claim_refusal(schedule_id) or {}
                    if not str(holder.get("job_id") or ""):
                        holder = {
                            **holder,
                            "kind": holder.get("kind") or "this_schedule",
                            "schedule_id": schedule_id,
                            "schedule_name": getattr(fresh, "name", "") or schedule_id,
                            "job_id": bound,
                            "running_since": getattr(fresh, "running_started_at", None),
                        }
                    raise ScheduleStartError(
                        describe_claim_holder(holder),
                        http_status=409,
                        code="already_running",
                    )
                # No durable schedule (direct dispatch tests) or the claim was
                # not stored. Keep the job the caller just created.
                set_running_job(schedule_id, job_id)
    from services.schedule_run_lifecycle import current_run_id, mark_running

    mark_running(schedule_id, current_run_id(schedule_id), job_id)
    if request.acknowledgment_actor or authorized_drift:
        # Delegated authority was exercised, and only now that a run actually
        # exists to exercise it: counting earlier would spend a single-use
        # approval on a job that never started.
        _record_authorization_use(schedule_id, rebind=authorized_drift)
    future = run_transfer_async(job_id, request)
    # Claim-queue enqueue returns an already-completed future. Treating that
    # as "the transfer finished" recorded the run while the job was still
    # pending, or left the claim held when the callback raced the worker.
    # The beat finalizes once the job document is actually terminal.
    if future.done():
        logger.info(
            "Schedule %s enqueued job %s; cadence records it when the job ends",
            schedule_id,
            job_id,
        )
        emit_event(logger, "schedule.run.fire", outcome="started", schedule_id=schedule_id, attempt=attempt + 1, job_id=job_id)
        return job_id
    future.add_done_callback(
        lambda _f, sid=schedule_id, jid=job_id, a=attempt, ts=started_at: _finalize_run(sid, jid, a, ts)
    )
    logger.info("Schedule %s started job %s (attempt %s)", schedule_id, job_id, attempt + 1)
    emit_event(logger, "schedule.run.fire", outcome="started", schedule_id=schedule_id, attempt=attempt + 1, job_id=job_id)
    return job_id


#: A blocked schedule stays due, and the beat wakes every second for an
#: already-due schedule. Without backoff the same "already in progress" line
#: was logged every beat, forever (QA MX3-24 log). Retry 5s → 60s.
_CLAIM_BACKOFF_MIN_S = 5.0
_CLAIM_BACKOFF_MAX_S = 60.0
_claim_backoff: dict[str, tuple[float, float, str]] = {}
_claim_backoff_lock = threading.Lock()


def _note_claim_blocked(schedule_id: str, holder: dict | None, detail: str) -> None:
    import time as _time

    key = "|".join(
        str((holder or {}).get(k) or "") for k in ("kind", "schedule_id", "job_id")
    )
    now = _time.monotonic()
    with _claim_backoff_lock:
        _until, delay, prior_key = _claim_backoff.get(schedule_id, (0.0, 0.0, ""))
        delay = (
            min(_CLAIM_BACKOFF_MAX_S, max(_CLAIM_BACKOFF_MIN_S, delay * 2))
            if prior_key == key
            else _CLAIM_BACKOFF_MIN_S
        )
        _claim_backoff[schedule_id] = (now + delay, delay, key)
    if prior_key != key:
        logger.info("Schedule %s skipped — %s", schedule_id, detail)
        emit_event(logger, "schedule.run.skip", outcome="skipped", reason_code="already_leased", schedule_id=schedule_id)
    else:
        logger.debug("Schedule %s still blocked — %s", schedule_id, detail)


def _clear_claim_backoff(schedule_id: str) -> None:
    with _claim_backoff_lock:
        _claim_backoff.pop(schedule_id, None)


def _claim_backed_off(schedule_id: str) -> bool:
    import time as _time

    with _claim_backoff_lock:
        entry = _claim_backoff.get(schedule_id)
    return bool(entry and _time.monotonic() < entry[0])


def _run_schedule(
    schedule_id: str,
    *,
    manual: bool = False,
    resync_mode: str = "",
    triggered_by: str = "",
) -> str | None:
    """Start one scheduled attempt. Its run record exists before any work.

    Every way out of here leaves that record in a persisted state: ``running``
    with the job bound, ``skipped`` with why, or ``failed`` with why. An
    exception used to be a log line and nothing else (QA round 5).
    """
    from services import schedule_run_lifecycle as life
    from services.schedule_store import get_schedule

    sched = get_schedule(schedule_id)
    if not sched:
        if manual:
            raise ScheduleStartError("Schedule not found.", http_status=404, code="not_found")
        return None
    if not sched.enabled and not manual:
        return None
    retrying = bool(getattr(sched, "retry_at", None))
    attempt = int(getattr(sched, "retry_attempt", 0) or 0) if retrying else 0
    trigger = (
        life.TRIGGER_MANUAL if manual
        else life.TRIGGER_RETRY if retrying
        else life.TRIGGER_CADENCE
    )
    run_id = life.begin_run(schedule_id, trigger=trigger, attempt=attempt)
    with life.bound_run(schedule_id, run_id):
        try:
            job_id = _start_schedule_run(
                schedule_id,
                manual=manual,
                resync_mode=resync_mode,
                triggered_by=triggered_by,
            )
        except ScheduleStartError as exc:
            life.finish_run(
                schedule_id, run_id, life.SKIPPED,
                reason=str(exc), reason_code=exc.code or "start_refused",
            )
            raise
        except PermissionError as exc:
            if manual:
                life.finish_run(
                    schedule_id, run_id, life.SKIPPED,
                    reason=str(exc), reason_code="permission_refused",
                )
            else:
                _fail_unstarted_run(schedule_id, run_id, exc, attempt=attempt, manual=False)
            raise
        except Exception as exc:
            _fail_unstarted_run(schedule_id, run_id, exc, attempt=attempt, manual=manual)
            raise
    if job_id is None:
        # A refusal that parked already failed this record; anything else
        # (paused between the beat and the dispatch) is a skip with its reason.
        life.finish_run(
            schedule_id, run_id, life.SKIPPED,
            reason="The schedule was not started: it was paused or removed before dispatch.",
            reason_code="not_started",
        )
    return job_id


def _fail_unstarted_run(
    schedule_id: str,
    run_id: str,
    exc: BaseException,
    *,
    attempt: int,
    manual: bool,
) -> None:
    """An exception before (or while) the job was submitted: a failed run.

    A cadence attempt consumes its slot like any failed run — otherwise the
    same crash re-fires on every beat — and is notified, since no job exists
    to notify for it. A manual one only records the failure: the person who
    pressed Run now is told by the HTTP answer, and the cadence is untouched.
    """
    from services import schedule_run_lifecycle as life
    from services.schedule_store import get_schedule, mark_schedule_run

    reason = f"The scheduler could not start this run: {type(exc).__name__}: {exc}"[:500]
    try:
        before = get_schedule(schedule_id)
        record = next(
            (r for r in (before.run_history if before else []) if r.get("run_id") == run_id),
            {},
        )
        if not life.is_open(record.get("status")):
            return
        job_id = str(record.get("job_id") or "")
        if job_id:
            life.settle_lost_job(job_id, _job_doc(job_id))
        now = datetime.now(timezone.utc).isoformat()
        entry = {
            "run_id": run_id, "status": "failed", "attempt": attempt, "job_id": job_id,
            "finished_at": now, "error": reason, "reason": reason,
            "reason_code": "start_error",
        }
        if manual:
            life.finish_run(schedule_id, run_id, "failed", reason=reason,
                            reason_code="start_error", error=reason, job_id=job_id)
            return
        updated = mark_schedule_run(schedule_id, job_id, status="failed", run_entry=entry)
        emit_event(logger, "schedule.run.fire", outcome="failed", reason_code="start_error",
                   schedule_id=schedule_id, attempt=attempt + 1, error=exc)
        if before is not None and _park_reason(before, {}, entry):
            _open_finding(schedule_id, reason, attempt=attempt, job_id=job_id)
        if updated is not None:
            _notify_schedule(updated, job_id, "failed", {"error": reason})
    except Exception:  # noqa: BLE001 — recording must not mask the original error
        logger.exception("Schedule %s failed run %s was not recorded", schedule_id, run_id)


def _start_schedule_run(
    schedule_id: str,
    *,
    manual: bool,
    resync_mode: str,
    triggered_by: str,
) -> str | None:
    from services import schedule_run_lifecycle as life
    from services.schedule_store import (
        clear_schedule_running,
        get_schedule,
        mark_schedule_running,
    )

    sched = get_schedule(schedule_id)
    if not sched:
        if manual:
            raise ScheduleStartError("Schedule not found.", http_status=404, code="not_found")
        return None
    from services.request_principal import assert_bound_principal_may_use_endpoints

    assert_bound_principal_may_use_endpoints(
        getattr(sched, "workspace_id", "") or None,
        {"connector_id": sched.source_connector_id},
        {"connector_id": sched.dest_connector_id},
        # An unattended beat runs the tap and the job records why it failed; a
        # person pressing Run now is told at once.
        require_taps=manual,
    )
    # Cadence already releases create-new dest-exists parks before the beat.
    # Run now must do the same or the first unattended fire stays parked.
    from services.schedule_approvals import release_create_new_dest_exists_false_refuse

    release_create_new_dest_exists_false_refuse(
        workspace_id=str(getattr(sched, "workspace_id", "") or ""),
    )
    sched = get_schedule(schedule_id) or sched

    blocked = _append_replay_blocked(sched)
    if blocked:
        logger.info("Schedule %s skipped — append replay would duplicate", schedule_id)
        emit_event(logger, "schedule.run.skip", outcome="skipped", reason_code="append_replay_duplicate", schedule_id=schedule_id)
        life.finish_run(
            schedule_id, life.current_run_id(schedule_id), life.SKIPPED,
            reason=blocked, reason_code="committed_rows_cannot_be_replayed",
        )
        if manual:
            raise ScheduleStartError(
                blocked,
                http_status=409,
                code="committed_rows_cannot_be_replayed",
            )
        return None

    # A finished job can still hold the claim when its callback never ran.
    # Record it before Run now, or the operator is told a run is in progress
    # after the transfer already ended.
    held = str(sched.running_job_id or "").strip()
    if sched.running and held:
        from services.schedule_store import _job_dispatch_state, _job_is_live, _parse_ts

        # A missing document is terminal. A job the liveness check still calls
        # in flight is not: the two lookups can disagree for one beat, and
        # finalizing that beat let Run now start a second writer (R14-001).
        if _job_dispatch_state(held) == "terminal" and _job_is_live(held) is not True:
            started = _parse_ts(sched.running_started_at) or datetime.now(timezone.utc)
            _finalize_run(
                schedule_id,
                held,
                sched.retry_attempt if sched.retry_at else 0,
                started,
            )
            sched = get_schedule(schedule_id) or sched

    # Concurrency guard: refuse to start when this schedule or another writer
    # on the same dest object is already live. The lock is held until the new
    # job id is on the claim, so a second Run now cannot observe the gap.
    from services.schedule_store import (
        _CLAIM_LOCK,
        describe_claim_holder,
        dispatch_in_flight,
        get_schedule as _reload_schedule,
        last_claim_refusal,
    )

    with _CLAIM_LOCK:
        if mark_schedule_running(schedule_id, _scheduler_instance_id()) is None:
            holder = last_claim_refusal(schedule_id) or {}
            fresh = _reload_schedule(schedule_id)
            bound = str(getattr(fresh, "running_job_id", "") or "") if fresh else ""
            if bound and not str(holder.get("job_id") or ""):
                holder = {
                    **holder,
                    "kind": holder.get("kind") or "this_schedule",
                    "schedule_id": schedule_id,
                    "schedule_name": getattr(fresh, "name", "") or schedule_id,
                    "job_id": bound,
                    "running_since": getattr(fresh, "running_started_at", None),
                }
            detail = describe_claim_holder(holder)
            _note_claim_blocked(schedule_id, holder, detail)
            life.finish_run(
                schedule_id, life.current_run_id(schedule_id), life.SKIPPED,
                reason=detail, reason_code="already_running",
            )
            if manual:
                raise ScheduleStartError(
                    detail,
                    http_status=409,
                    code="already_running",
                )
            return None
        _clear_claim_backoff(schedule_id)

        # A parked retry resumes its own attempt count; the budget is per run, not
        # per beat, or a schedule that fails every time retries forever.
        try:
            with dispatch_in_flight(schedule_id):
                job_id = _dispatch_transfer(
                    schedule_id,
                    attempt=sched.retry_attempt if sched.retry_at else 0,
                    allow_paused=manual,
                    resync_mode=resync_mode,
                    triggered_by=triggered_by,
                )
        except ScheduleStartError:
            raise
        except Exception:
            # Nothing was submitted, so nothing is writing. Holding the claim for
            # the unbound grace made every beat skip the schedule and Run now
            # answer "already in progress" with no running job (QA MX3-24).
            logger.exception(
                "Schedule %s dispatch raised after its claim was taken; releasing the claim",
                schedule_id,
            )
            clear_schedule_running(schedule_id)
            raise
        if job_id is None:
            # Fail-closed paths (missing connector / contract) already call
            # mark_schedule_run which clears ``running``. Belt-and-suspenders clear.
            clear_schedule_running(schedule_id)
            if manual:
                raise ScheduleStartError(
                    _manual_start_failure_detail(get_schedule(schedule_id)),
                    code="start_refused",
                )
        return job_id


def _release_superseded_queued_claims() -> None:
    """Keep a queued schedule job. Do not cancel it for a later slot.

    The job has not read the source yet. Cancelling it while a large transfer
    still holds the workers drops the fire, and the next slot cancels the
    replacement too. Missed slots are counted when the job finishes. A running
    writer stays one writer.
    """
    from services.schedule_store import release_all_superseded_queued_claims

    release_all_superseded_queued_claims()


def record_schedule_for_finished_job(job_id: str) -> None:
    """Count a manual or claimed run when its job actually ends.

    The claim-queue path does not attach a completion callback, and a
    restart used to clear ``running`` without calling ``mark_schedule_run``.
    ``run_count`` then stayed 0 after a run the operator could see in Jobs.
    """
    if not str(job_id or "").strip():
        return
    from services.schedule_store import _load_all, _parse_ts

    for sched in _load_all():
        if str(sched.running_job_id or "") != str(job_id):
            continue
        if not sched.running:
            return
        started = _parse_ts(sched.running_started_at) or datetime.now(timezone.utc)
        _finalize_run(
            sched.id,
            job_id,
            sched.retry_attempt if sched.retry_at else 0,
            started,
        )
        return


def _finalize_finished_schedule_claims() -> None:
    """Record runs whose job ended without the in-process callback.

    A worker restart, a claim-queue enqueue, or a callback that never ran
    leaves ``running`` set and ``next_run_at`` on the slot that already fired.
    The next beat used to skip that schedule forever, and Run now answered
    that a run was still in progress.
    """
    from services.schedule_store import (
        _is_running_stale,
        _job_is_live,
        _load_all,
        _parse_ts,
        clear_schedule_running,
    )

    for sched in _load_all():
        if not sched.running:
            continue
        if not str(sched.running_job_id or "").strip():
            # Claim taken, job never bound (dispatch died in between). Nothing
            # is writing; release it once the unbound grace has passed.
            if _is_running_stale(sched):
                logger.warning(
                    "Schedule %s released an unbound claim from %s (no job was created)",
                    sched.id,
                    sched.running_started_at,
                )
                clear_schedule_running(sched.id)
            continue
        if _job_is_live(sched.running_job_id) is not False:
            continue
        started = _parse_ts(sched.running_started_at) or datetime.now(timezone.utc)
        _finalize_run(
            sched.id,
            sched.running_job_id,
            sched.retry_attempt if sched.retry_at else 0,
            started,
        )


def _run_due_schedules() -> int:
    if not _acquire_scheduler_lock():
        logger.warning("Scheduler lock held by another instance; skipping this beat")
        return 0
    due_ids: list[str] = []
    try:
        from services.schedule_approvals import release_same_declaration_source_drift

        # A parked TIMESTAMP_NTZ → TIMESTAMP_NTZ invent is not a decision.
        # Release it so the cadence can fire; a real narrow stays parked.
        release_same_declaration_source_drift()
        from services.schedule_approvals import release_create_new_dest_exists_false_refuse

        # Dest-exists after the first create-new write is not a plan change.
        # Release only when the operator Map hash still matches.
        release_create_new_dest_exists_false_refuse()
        _release_superseded_queued_claims()
        _finalize_finished_schedule_claims()
        _sweep_orphan_runs()
        try:
            from services.cdc_capture_release import retry_pending_capture_releases

            # A schedule delete that could not drop its source slot (QA S14)
            # keeps retrying here, so WAL is not retained forever on a client DB.
            retry_pending_capture_releases()
        except Exception:  # noqa: BLE001 — cleanup must never stop the beat
            logger.exception("Pending CDC capture release retry failed")
        from services.schedule_store import unstick_completion_pinned_schedules

        unstick_completion_pinned_schedules()
        for sched in due_schedules():
            if _claim_backed_off(sched.id):
                continue
            due_ids.append(sched.id)
    finally:
        _release_scheduler_lock()
    # Preflight and dispatch sit outside the fleet lock. Holding it across
    # both made the second due schedule wait out the first schedule's
    # planning (R3-H2: runs started 4–48s late). The per-schedule claim
    # still prevents a double fire.
    started = 0
    for schedule_id in due_ids:
        try:
            if _run_schedule(schedule_id):
                started += 1
        except Exception:
            logger.exception("Failed to run schedule %s", schedule_id)
    return started


def _sweep_orphan_runs() -> None:
    """Fail open run records whose worker is gone ("worker lost")."""
    try:
        from services.schedule_run_lifecycle import sweep_orphan_runs

        swept = sweep_orphan_runs()
        for row in swept:
            emit_event(logger, "schedule.run.orphan", outcome=row["action"],
                       reason_code="worker_lost", schedule_id=row["schedule_id"])
    except Exception:  # noqa: BLE001 — the sweep must never stop the beat
        logger.exception("Schedule run sweep failed")


def _clear_stale_running_schedules() -> None:
    """On startup, settle runs and claims left by a previous crashed instance.

    Runs first: a claim whose job died is finalized as "worker lost" before
    the claim is released, so the run is never dropped without a record.
    """
    _sweep_orphan_runs()
    from services.schedule_store import (
        _CLAIM_LOCK,
        PipelineSchedule,
        _is_running_stale,
        _load_all,
        _parse_ts,
        _save_all,
        compute_next_run,
    )

    # The whole snapshot is rewritten below: hold the store lock so an edit or
    # a run landing meanwhile is not overwritten by this stale copy.
    with _CLAIM_LOCK:
        schedules = _load_all()
        changed = False
        for i, s in enumerate(schedules):
            if s.running and str(s.running_job_id or "").strip():
                from services.schedule_store import _job_dispatch_state, get_schedule

                if _job_dispatch_state(s.running_job_id) == "terminal":
                    started = _parse_ts(s.running_started_at) or datetime.now(timezone.utc)
                    _finalize_run(
                        s.id,
                        s.running_job_id,
                        s.retry_attempt if s.retry_at else 0,
                        started,
                    )
                    fresh = get_schedule(s.id)
                    if fresh is not None:
                        schedules[i] = fresh
                    continue
            if s.running and _is_running_stale(s):
                current = datetime.now(timezone.utc)
                nxt = _parse_ts(s.next_run_at)
                advanced = s.next_run_at
                if nxt is None or nxt <= current:
                    advanced = compute_next_run(
                        s.interval, current, cron=s.cron, tz=s.timezone
                    )
                schedules[i] = PipelineSchedule.from_dict({
                    **s.to_dict(),
                    "running": False,
                    "running_instance": "",
                    "running_started_at": None,
                    "running_job_id": "",
                    "next_run_at": advanced,
                })
                changed = True
        if changed:
            _save_all(schedules)


async def run_schedule_loop() -> None:
    """Poll for due schedules and enqueue transfers."""
    logger.info("Pipeline scheduler started (interval=%ss)", CHECK_INTERVAL_SECONDS)
    try:
        await asyncio.get_running_loop().run_in_executor(
            _executor,
            functools.partial(contextvars.copy_context().run, _clear_stale_running_schedules),
        )
    except Exception:  # noqa: BLE001 — a bad document must not kill the loop
        logger.exception(
            "Startup schedule reclaim failed; the beat will continue"
        )
    try:
        from services.schedule_store import import_file_schedules_into_mongo

        imported = await asyncio.get_running_loop().run_in_executor(
            _executor,
            functools.partial(contextvars.copy_context().run, import_file_schedules_into_mongo),
        )
        if imported:
            logger.info("Imported %s schedule(s) from schedules.json into MongoDB", imported)
    except Exception:
        logger.exception("Schedule file→Mongo import failed")
    while True:
        try:
            count = await asyncio.get_running_loop().run_in_executor(
                _executor,
                functools.partial(contextvars.copy_context().run, _run_due_schedules),
            )
            if count:
                logger.info("Scheduler started %s pipeline run(s)", count)
        except Exception:
            logger.exception("Schedule loop error")
        wait = float(CHECK_INTERVAL_SECONDS)
        try:
            from services.schedule_store import seconds_until_next_schedule

            wait = float(
                await asyncio.get_running_loop().run_in_executor(
                    _executor,
                    functools.partial(
                        contextvars.copy_context().run, seconds_until_next_schedule
                    ),
                )
            )
        except Exception:  # noqa: BLE001 — a bad store falls back to the poll cap
            logger.exception("Schedule sleep horizon failed; using the poll cap")
            wait = float(CHECK_INTERVAL_SECONDS)
        await asyncio.sleep(wait)
