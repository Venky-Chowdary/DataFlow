"""Recurring pipeline schedules — shared persistence.

When a real MongoDB is available schedules are stored in a collection so
multi-instance Railway deployments share the same schedule state.  Otherwise
they fall back to a JSON file in ``data_dir``.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from services.cron_schedule import CronError, validate_cron
from services.cron_schedule import next_run as _cron_next_run
from services.platform_config import data_dir
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

try:
    from src.services.mongodb_service import get_mongodb_service
except ImportError:
    from services.mongodb_service import get_mongodb_service

STORE_PATH = data_dir() / "schedules.json"
STORE_MIGRATED_PATH = data_dir() / "schedules.json.migrated"

INTERVALS = {"hourly": timedelta(hours=1), "daily": timedelta(days=1), "weekly": timedelta(weeks=1)}
SYNC_MODES = {
    "full_refresh_overwrite",
    "full_refresh_append",
    "incremental",
    "incremental_append",
    "incremental_deduped",
    "cdc",
    "scd2",
    "mirror",
    "reverse_etl",
}
SCHEMA_POLICIES = {
    "manual_review",
    "propagate_columns",
    "propagate_all",
    "pause_on_change",
    "type_locked",
}


def _first_contract_snapshot(contracts: Any) -> str:
    for row in contracts or []:
        if not isinstance(row, dict):
            continue
        mode = str(row.get("snapshot_mode") or "").strip()
        if mode:
            return mode
    return ""


def _schedule_snapshot_mode(sync_mode: str, raw: Any) -> str:
    from services.cdc_snapshot_mode import schedule_snapshot_mode

    try:
        return schedule_snapshot_mode(sync_mode, raw)
    except ValueError:
        return schedule_snapshot_mode(sync_mode, "initial") if str(sync_mode or "").strip().lower() == "cdc" else ""


def _schedule_cdc_extras(data: Mapping[str, Any]) -> dict[str, Any]:
    from services.schedule_cdc_extras import schedule_cdc_extras

    return schedule_cdc_extras(
        data.get("sync_mode") or "full_refresh_overwrite",
        allow_append_only=data.get("allow_append_only", False),
        cdc_row_filter=data.get("cdc_row_filter"),
        multi_subnet_failover=data.get("multi_subnet_failover", False),
    )


# Keep only the most recent N runs per schedule so the history document stays small.
RUN_HISTORY_LIMIT = 25
# Window between taking a claim and its job becoming visible; a claim whose job
# has ended is reclaimable once this has passed.
CLAIM_GRACE = timedelta(minutes=5)
# Only used when the job cannot be looked up at all — a claim of unknown state
# must not wedge a schedule forever.
CLAIM_MAX_RUNTIME = timedelta(hours=4)

_file_import_attempted = False
# Same-process Run-now + beat must not both observe running=false.
_CLAIM_LOCK = threading.Lock()


def _load_schedules_from_file(path=STORE_PATH) -> list[PipelineSchedule]:
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return [PipelineSchedule.from_dict(s) for s in raw.get("schedules", [])]
    except Exception:
        return []


def import_file_schedules_into_mongo(*, force: bool = False) -> int:
    """One-shot recovery: if Mongo is empty but ``schedules.json`` has rows, import them.

    Prevents the silent “Next run shown / Runs=0 / scheduler idle” failure mode when
    the API switches from file persistence to a real MongoDB with an empty collection.
    Returns the number of schedules imported.
    """
    global _file_import_attempted
    if _file_import_attempted and not force:
        return 0

    svc = _mongo_backend()
    if not svc:
        # Do not latch attempted — Mongo may come up later in the same process.
        return 0
    _file_import_attempted = True

    existing = _load_mongo(svc)
    if existing:
        return 0

    file_schedules = _load_schedules_from_file(STORE_PATH)
    if not file_schedules and STORE_MIGRATED_PATH.exists():
        # Allow re-import from the backup if the primary file was already renamed.
        file_schedules = _load_schedules_from_file(STORE_MIGRATED_PATH)
    if not file_schedules:
        return 0

    _save_mongo(svc, file_schedules)
    try:
        if STORE_PATH.exists():
            STORE_PATH.replace(STORE_MIGRATED_PATH)
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    return len(file_schedules)


@dataclass
class PipelineSchedule:
    id: str
    name: str
    source_connector_id: str
    source_table: str
    dest_connector_id: str
    dest_table: str
    interval: str  # hourly | daily | weekly (preset cadence)
    enabled: bool = True
    # Cadence — cron (5-field) takes precedence over ``interval`` when set.
    cron: str = ""
    timezone: str = "UTC"  # IANA timezone for cron/preset evaluation
    # Transfer configuration used to build the scheduled TransferRequest.
    sync_mode: str = "full_refresh_overwrite"  # full_refresh_* | incremental | cdc
    validation_mode: str = "strict"
    schema_policy: str = "manual_review"
    backfill_new_fields: bool = False
    # Stage-then-promote. Default off so unattended runs match Studio unless set.
    write_via_staging: bool = False
    # Priority-first sync + optional row cap (0 = no limit).
    priority_column: str = ""
    priority_direction: str = "desc"
    row_limit: int = 0
    # CDC dest-owned watermark EOS is opt-in; default stays at_least_once.
    delivery_guarantee: str = "at_least_once"
    # Debezium snapshot mode (CDC only). Empty on full/incremental.
    snapshot_mode: str = ""
    # Destination Advanced CDC extras. Empty/false on full/incremental.
    allow_append_only: bool = False
    cdc_row_filter: str = ""
    multi_subnet_failover: bool = False
    mappings: list[dict] = field(default_factory=list)
    stream_contracts: list[dict] = field(default_factory=list)
    cursor_column: str = ""  # watermark column for incremental syncs
    #: What ``cursor_column`` means in the source (services.cursor_semantics).
    #: The g9 gate refuses an undeclared cursor on a strict incremental run, so
    #: a schedule that cannot carry the declaration can never run unattended.
    cursor_semantics: str = ""
    primary_key: str = ""  # key for idempotent incremental/cdc upserts
    #: Callable extract — persist the CALL/SELECT, not just the stream label.
    source_read_mode: str = ""
    procedure_call: str = ""
    source_query: str = ""
    procedure_params: dict[str, Any] = field(default_factory=dict)
    cursor_value: str = ""  # last observed watermark (advances each run)
    workspace_id: str = ""
    # Data contract — when set, scheduled runs enforce the signed contract.
    contract_id: str = ""
    require_signed_contract: bool = False
    # Validate≡Execute identity — scheduled runs must replay the same locales,
    # pre-load recipe, and approved hashes the operator signed in Studio.
    date_locale: str = ""
    number_locale: str = ""
    shape_recipe: dict[str, Any] = field(default_factory=dict)
    approved_shape_recipe_hash: str = ""
    approved_decision_artifact_hash: str = ""
    approved_ddl_identity_hash: str = ""
    #: The source shape observed on the last successful run, so a later run can
    #: tell a renamed or retyped column from one that was always that way. A
    #: schedule that remembers only its cursor cannot notice that the column it
    #: reads changed meaning, which is the largest single cause of pipeline
    #: incidents and the one that keeps succeeding while writing wrong values.
    source_schema: dict[str, str] = field(default_factory=dict)
    source_schema_fingerprint: str = ""
    source_schema_observed_at: str = ""
    #: Live source primary-key columns observed with the type map. PK-only
    #: identity changes keep the same col→type map, so a type-only baseline
    #: cannot see them — Confluent NONE / Airbyte hard-break.
    source_primary_key: list[str] = field(default_factory=list)
    #: Dual Run campaign: consecutive parallel-run cycles on this route.
    #: ``evaluate_campaign`` is the kernel; this is durable memory for it.
    fidelity_campaign: dict[str, Any] = field(default_factory=dict)
    #: Autopilot — a named human's advance signature for unattended runs, bound
    #: by hash to the mapping and source shape it was granted against. Empty is
    #: the safe default: with no grant the gates decide exactly as before.
    #: ``services/standing_authorization.py`` owns its shape and its rules.
    standing_authorization: dict[str, Any] = field(default_factory=dict)
    #: An open finding waiting on a human. While this is set the cadence is
    #: suppressed, because re-deciding an identical configuration against
    #: identical catalogs produces an identical refusal — every night, forever.
    approval_request: dict[str, Any] = field(default_factory=dict)
    # Retry policy applied on run failure.
    max_retries: int = 0
    retry_backoff_seconds: int = 60
    # Notification preferences (delivered via notification_service).
    notify_on_failure: bool = True
    notify_on_success: bool = False
    # Bookkeeping.
    last_run_at: str | None = None
    next_run_at: str | None = None
    last_job_id: str | None = None
    last_status: str | None = None
    run_count: int = 0
    #: Due instants that elapsed without a run (outage, overrun, paused deploy).
    #: Counted rather than dropped so a cadence gap is visible after the fact.
    missed_window_count: int = 0
    last_missed_windows: int = 0
    #: A pending retry is durable state, not an in-process timer: a redeploy
    #: between attempt 1 and attempt 2 must not drop the retry on the floor.
    retry_at: str | None = None
    retry_attempt: int = 0
    running: bool = False
    running_instance: str = ""
    running_started_at: str | None = None
    #: The job the claim was taken for. Liveness of that job — not a clock —
    #: decides whether the claim may be reclaimed.
    running_job_id: str = ""
    run_history: list[dict] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: _now())
    #: Store revision this copy was read at. Writes are three-way merged against
    #: it, so a stale in-memory copy can neither clobber a concurrent change nor
    #: resurrect a deleted schedule.
    version: int = 0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        from src.ai.copilot.schedule_cadence import describe_stored_cadence

        payload["cadence_label"] = describe_stored_cadence(
            self.interval, self.cron, self.timezone
        )
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PipelineSchedule:
        cdc_extras = _schedule_cdc_extras(data)
        return cls(
            id=data["id"],
            name=data["name"],
            source_connector_id=data["source_connector_id"],
            source_table=data["source_table"],
            dest_connector_id=data["dest_connector_id"],
            dest_table=data["dest_table"],
            interval=data.get("interval", "daily"),
            enabled=bool(data.get("enabled", True)),
            cron=(data.get("cron") or "").strip(),
            timezone=(data.get("timezone") or "UTC").strip() or "UTC",
            sync_mode=data.get("sync_mode") or "full_refresh_overwrite",
            validation_mode=data.get("validation_mode") or "strict",
            schema_policy=data.get("schema_policy") or "manual_review",
            backfill_new_fields=bool(data.get("backfill_new_fields", False)),
            write_via_staging=bool(data.get("write_via_staging", False)),
            priority_column=str(data.get("priority_column") or "").strip(),
            priority_direction=(
                "asc"
                if str(data.get("priority_direction") or "").strip().lower() == "asc"
                else "desc"
            ),
            row_limit=max(0, int(data.get("row_limit", 0) or 0)),
            delivery_guarantee=(
                str(data.get("delivery_guarantee") or "at_least_once")
                .strip()
                .lower()
                .replace("-", "_")
                or "at_least_once"
            ),
            snapshot_mode=_schedule_snapshot_mode(
                data.get("sync_mode") or "full_refresh_overwrite",
                data.get("snapshot_mode")
                or _first_contract_snapshot(data.get("stream_contracts")),
            ),
            allow_append_only=bool(cdc_extras["allow_append_only"]),
            cdc_row_filter=str(cdc_extras["cdc_row_filter"]),
            multi_subnet_failover=bool(cdc_extras["multi_subnet_failover"]),
            mappings=list(data.get("mappings") or []),
            stream_contracts=list(data.get("stream_contracts") or []),
            cursor_column=(data.get("cursor_column") or "").strip(),
            cursor_semantics=str(data.get("cursor_semantics") or "").strip().lower(),
            primary_key=(data.get("primary_key") or "").strip(),
            source_read_mode=str(data.get("source_read_mode") or "").strip().lower(),
            procedure_call=str(data.get("procedure_call") or "").strip(),
            source_query=str(data.get("source_query") or "").strip(),
            procedure_params=dict(data.get("procedure_params") or {})
            if isinstance(data.get("procedure_params"), dict)
            else {},
            cursor_value=str(data.get("cursor_value") or ""),
            workspace_id=(data.get("workspace_id") or "").strip(),
            contract_id=(data.get("contract_id") or "").strip(),
            require_signed_contract=bool(
                data.get(
                    "require_signed_contract",
                    bool((data.get("contract_id") or "").strip()),
                )
            ),
            date_locale=str(data.get("date_locale") or "").strip(),
            number_locale=str(data.get("number_locale") or "").strip(),
            shape_recipe=dict(data.get("shape_recipe") or {})
            if isinstance(data.get("shape_recipe"), dict)
            else {},
            approved_shape_recipe_hash=str(data.get("approved_shape_recipe_hash") or "").strip(),
            approved_decision_artifact_hash=str(
                data.get("approved_decision_artifact_hash") or ""
            ).strip(),
            approved_ddl_identity_hash=str(data.get("approved_ddl_identity_hash") or "").strip(),
            source_schema={
                str(k): str(v)
                for k, v in (data.get("source_schema") or {}).items()
                if not isinstance(v, (dict, list))
            },
            source_schema_fingerprint=str(data.get("source_schema_fingerprint") or ""),
            source_schema_observed_at=str(data.get("source_schema_observed_at") or ""),
            source_primary_key=[
                str(p).strip()
                for p in (data.get("source_primary_key") or [])
                if str(p).strip()
            ],
            fidelity_campaign=dict(data.get("fidelity_campaign") or {})
            if isinstance(data.get("fidelity_campaign"), dict)
            else {},
            standing_authorization=dict(data.get("standing_authorization") or {})
            if isinstance(data.get("standing_authorization"), dict)
            else {},
            approval_request=dict(data.get("approval_request") or {})
            if isinstance(data.get("approval_request"), dict)
            else {},
            max_retries=max(0, int(data.get("max_retries", 0) or 0)),
            retry_backoff_seconds=max(0, int(data.get("retry_backoff_seconds", 60) or 0)),
            notify_on_failure=bool(data.get("notify_on_failure", True)),
            notify_on_success=bool(data.get("notify_on_success", False)),
            last_run_at=data.get("last_run_at"),
            next_run_at=data.get("next_run_at"),
            last_job_id=data.get("last_job_id"),
            last_status=data.get("last_status"),
            run_count=int(data.get("run_count", 0)),
            missed_window_count=max(0, int(data.get("missed_window_count", 0) or 0)),
            last_missed_windows=max(0, int(data.get("last_missed_windows", 0) or 0)),
            retry_at=data.get("retry_at"),
            retry_attempt=max(0, int(data.get("retry_attempt", 0) or 0)),
            running=bool(data.get("running", False)),
            running_instance=data.get("running_instance", ""),
            running_started_at=data.get("running_started_at"),
            running_job_id=(data.get("running_job_id") or "").strip(),
            run_history=list(data.get("run_history") or []),
            created_at=data.get("created_at", _now()),
            version=_as_version(data.get("version")),
        )


def _as_version(raw: Any) -> int:
    try:
        return max(0, int(raw or 0))
    except (TypeError, ValueError):
        return 0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(value: str | datetime | None) -> datetime | None:
    """UTC-aware instant, or None when the stored value cannot be read.

    Mongo returns ``datetime``; the file store returns ISO text. A naive value
    is UTC, the same clock ``_now`` writes. Mixing that naive value with
    ``datetime.now(timezone.utc)`` raised ``TypeError`` inside ``due_schedules``
    and aborted every schedule on the beat.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_next_run(
    interval: str,
    from_time: datetime | None = None,
    *,
    cron: str = "",
    tz: str = "UTC",
) -> str:
    """Compute the next due time as an ISO-8601 UTC timestamp.

    A cron expression (5-field) takes precedence over the interval preset and is
    evaluated in the schedule's IANA ``tz``. Preset cadences (hourly/daily/weekly)
    are **rolling** offsets from ``from_time`` (typically last run), not fixed
    wall-clock times — use cron for “every day at 10:10”.
    """
    base = from_time or datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    cron = (cron or "").strip()
    if cron:
        return _cron_next_run(cron, base, tz or "UTC").isoformat()
    delta = INTERVALS.get(interval, INTERVALS["daily"])
    return (base.astimezone(timezone.utc) + delta).isoformat()


#: Ceiling on how many skipped windows are counted; an outage of months on a
#: minutely cron must not turn a bookkeeping update into an unbounded loop.
MISSED_WINDOW_SCAN_LIMIT = 1000


def count_missed_windows(
    *,
    cron: str,
    interval: str,
    tz: str,
    next_run_at: str | None,
    now: datetime | None = None,
) -> int:
    """How many due instants elapsed before this run, excluding the one running.

    A scheduler outage, a paused deployment or a run that overran its own
    cadence leaves due times in the past. The next beat runs the schedule once
    and the next due time is recomputed from the present, so the windows in
    between disappear with nothing recording that they were skipped — an
    operator reading `Runs: 7` cannot tell it should have been 10.
    """
    due = _parse_ts(next_run_at)
    if due is None:
        return 0
    current = now or datetime.now(timezone.utc)
    if due.tzinfo is None:
        due = due.replace(tzinfo=timezone.utc)
    if due > current:
        return 0
    cron = (cron or "").strip()
    missed = 0
    cursor = due
    for _ in range(MISSED_WINDOW_SCAN_LIMIT):
        if cron:
            try:
                cursor = _cron_next_run(cron, cursor, tz or "UTC")
            except CronError:
                return missed
        else:
            cursor = cursor + INTERVALS.get(interval, INTERVALS["daily"])
        if cursor > current:
            return missed
        missed += 1
    return missed


def next_run_for(sched: PipelineSchedule, from_time: datetime | None = None) -> str:
    return compute_next_run(sched.interval, from_time, cron=sched.cron, tz=sched.timezone)


def _mongo_backend():
    """Return a real MongoDB service when connected, otherwise None."""
    try:
        svc = get_mongodb_service()
    except Exception:
        return None
    if type(svc).__name__ == "MemoryMongoDBService":
        return None
    return svc if getattr(svc, "client", None) is not None else None


def _load_mongo(svc) -> list[PipelineSchedule]:
    db = svc.get_database()
    # Prefer per-schedule documents (CAS-safe). Fall back to legacy blob.
    docs = list(db["pipeline_schedules"].find({}))
    if docs:
        loaded = [_schedule_from_doc(d) for d in docs]
        for s in loaded:
            _remember_base(s)
        return loaded
    doc = db["schedule_store"].find_one({"_id": "primary"})
    if not doc:
        return []
    # Once the per-schedule store has written, an empty per-schedule store means
    # "no schedules", not "read the blob" — otherwise deleting the last schedule
    # resurrects every schedule the blob still holds.
    if doc.get("superseded_by"):
        return []
    return [PipelineSchedule.from_dict(s) for s in doc.get("schedules", [])]


def _schedule_from_doc(doc: Mapping[str, Any]) -> PipelineSchedule:
    return PipelineSchedule.from_dict({**doc, "id": doc.get("id") or str(doc.get("_id"))})


# Three-way merge base. Every caller does load → mutate one schedule → save the
# whole snapshot. Without the revision the copy was read at, a save cannot tell
# "this caller changed the field" from "this caller holds a stale value", so the
# snapshot used to overwrite concurrent changes wholesale: a beat's claim, a
# finished run's release, and — through ``upsert`` — a schedule another request
# had just deleted (QA MX3-24 stuck ``running``; the delete that "succeeded" but
# left the schedule live).
_BASE_CAP = 4096
_BASE_LOCK = threading.Lock()
_BASE_DOCS: dict[tuple[str, int], dict[str, str]] = {}


def _canon(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, default=json_default)
    except (TypeError, ValueError):
        return repr(value)


def _field_fingerprint(payload: Mapping[str, Any]) -> dict[str, str]:
    return {
        str(k): _canon(v)
        for k, v in payload.items()
        if k not in ("_id", "version")
    }


def _remember_base(sched: PipelineSchedule) -> None:
    key = (sched.id, int(sched.version or 0))
    snap = _field_fingerprint(sched.to_dict())
    with _BASE_LOCK:
        _BASE_DOCS.pop(key, None)
        _BASE_DOCS[key] = snap
        while len(_BASE_DOCS) > _BASE_CAP:
            _BASE_DOCS.pop(next(iter(_BASE_DOCS)))


def _base_for(schedule_id: str, version: int) -> dict[str, str] | None:
    with _BASE_LOCK:
        return _BASE_DOCS.get((schedule_id, int(version or 0)))


def _changed_fields(
    payload: Mapping[str, Any], reference: Mapping[str, str]
) -> dict[str, Any]:
    return {
        k: v
        for k, v in payload.items()
        if k not in ("_id", "version") and reference.get(k) != _canon(v)
    }


def _same_schedule_doc(existing: Mapping[str, Any], payload: Mapping[str, Any]) -> bool:
    """True when ``payload`` carries exactly what the stored doc already holds."""
    stored = {k: v for k, v in existing.items() if k not in ("_id", "version")}
    try:
        return json.dumps(stored, sort_keys=True, default=json_default) == json.dumps(
            {k: v for k, v in dict(payload).items() if k != "version"},
            sort_keys=True,
            default=json_default,
        )
    except (TypeError, ValueError):
        return False


_SAVE_CAS_ATTEMPTS = 8


def _save_one_mongo(coll: Any, sched: PipelineSchedule) -> None:
    """Write only what this caller changed, conditional on the stored revision.

    The changed fields are this copy versus the revision it was read at. They
    are applied with ``$set`` under a version CAS, so fields another writer
    changed meanwhile survive. A copy that was read from the store and whose
    document is now gone was deleted concurrently: it is dropped, never
    re-inserted.
    """
    from pymongo.errors import DuplicateKeyError

    payload = sched.to_dict()
    payload.pop("_id", None)
    payload.pop("version", None)
    payload["id"] = sched.id
    loaded_version = int(sched.version or 0)
    base = _base_for(sched.id, loaded_version)
    delta: dict[str, Any] = {}
    for _attempt in range(_SAVE_CAS_ATTEMPTS):
        existing = coll.find_one({"_id": sched.id})
        if existing is None:
            if base is not None:
                logging.getLogger(__name__).info(
                    "Schedule %s was deleted concurrently; stale copy not re-inserted",
                    sched.id,
                )
                return
            try:
                result = coll.find_one_and_update(
                    {"_id": sched.id},
                    {"$set": {**payload, "version": 1}},
                    upsert=True,
                    return_document=True,
                )
            except DuplicateKeyError:
                continue
            if result is not None:
                _remember_base(_schedule_from_doc(result))
                return
            continue
        current_version = _as_version(existing.get("version"))
        if current_version == loaded_version:
            # The stored doc *is* this copy's base: the diff against it is
            # exactly this caller's change.
            reference = _field_fingerprint(_schedule_from_doc(existing).to_dict())
        elif base is not None:
            reference = base
        else:
            # Base revision unknown (evicted, or a copy never read from this
            # store): fall back to the stored values. Logged — this is the one
            # path that can still apply a stale value.
            logging.getLogger(__name__).warning(
                "Schedule %s saved without its base revision v%s (stored v%s)",
                sched.id,
                loaded_version,
                current_version,
            )
            reference = _field_fingerprint(_schedule_from_doc(existing).to_dict())
        delta = _changed_fields(payload, reference)
        if not delta:
            return
        filt: dict[str, Any] = {"_id": sched.id}
        filt["version"] = (
            current_version if "version" in existing else {"$exists": False}
        )
        result = coll.find_one_and_update(
            filt,
            {"$set": {**delta, "version": current_version + 1}},
            return_document=True,
        )
        if result is not None:
            _remember_base(_schedule_from_doc(result))
            return
    # Contended past every retry: apply only this caller's fields, never the
    # whole stale snapshot, and never resurrect a deleted document.
    if delta:
        coll.update_one({"_id": sched.id}, {"$set": delta, "$inc": {"version": 1}})


def _save_mongo(
    svc,
    schedules: list[PipelineSchedule],
    *,
    removed_ids: Sequence[str] = (),
) -> None:
    """Persist schedules as individual docs with three-way-merged version CAS.

    ``removed_ids`` names the schedules this write deletes; nothing else is
    ever removed. Each schedule writes only the fields this caller changed
    since it read the doc (:func:`_save_one_mongo`), so a whole-snapshot save
    cannot clobber another instance's concurrent claim, release or watermark,
    and cannot resurrect a schedule deleted in between.
    """
    db = svc.get_database()
    coll = db["pipeline_schedules"]
    seen = set()
    for s in schedules:
        seen.add(s.id)
        _save_one_mongo(coll, s)
    # Only the ids a caller explicitly deleted are removed. Sweeping every doc
    # absent from this snapshot deleted schedules another instance created
    # between our load and this save.
    gone = [sid for sid in removed_ids if sid and sid not in seen]
    if gone:
        coll.delete_many({"_id": {"$in": gone}})
    # The per-schedule store is authoritative from the first write on, so a legacy
    # blob must never answer a later read (see ``_load_mongo``).
    if seen or gone:
        db["schedule_store"].update_one(
            {"_id": "primary"},
            {"$set": {"superseded_by": "pipeline_schedules"}},
        )


def _load_all() -> list[PipelineSchedule]:
    svc = _mongo_backend()
    if svc:
        return _load_mongo(svc)
    return _load_schedules_from_file(STORE_PATH)


def _save_all(schedules: list[PipelineSchedule], *, removed_ids: Sequence[str] = ()) -> None:
    svc = _mongo_backend()
    if svc:
        _save_mongo(svc, schedules, removed_ids=removed_ids)
        return
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STORE_PATH.write_text(
        json.dumps({"schedules": [s.to_dict() for s in schedules]}, indent=2, default=json_default),
        encoding="utf-8",
    )


def schedule_overdue_seconds(
    sched: PipelineSchedule, now: datetime | None = None
) -> int:
    """Seconds an enabled, idle schedule is past its due time (0 when not).

    Read-only. QA D11 saw ``next_run_at`` frozen in the past; the cause was a
    claim that never released (MX3-24), not the timestamp. Rewriting the
    timestamp on read hid the stuck claim *and* dropped the one catch-up run
    the beat owes for that slot without counting it as missed. The list now
    reports how late the run is and — via :func:`claim_holder` — what holds it.
    """
    if not sched.enabled or sched.running:
        return 0
    due = _parse_ts(sched.next_run_at)
    if due is None:
        return 0
    if due.tzinfo is None:
        due = due.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    return max(0, int((current - due).total_seconds()))


def list_schedules() -> list[PipelineSchedule]:
    return sorted(_load_all(), key=lambda s: s.created_at, reverse=True)


def get_schedule(schedule_id: str) -> PipelineSchedule | None:
    for s in _load_all():
        if s.id == schedule_id:
            return s
    return None


def accepted_schedule_sync_mode(sync_mode: str) -> str:
    """Schedule spelling, or the canonical mode an alias resolves to.

    Tokens already stored (``incremental``) keep that spelling. ``incremental_upsert``
    is the Pilot / transfer-tool name for an incremental deduped write; rejecting
    it made upsert schedules impossible to create.
    """
    token = (sync_mode or "").strip().lower().replace("-", "_").replace(" ", "_")
    if token in SYNC_MODES:
        return token
    from services.sync_cursor import normalize_sync_mode

    resolved = normalize_sync_mode(token, default="")
    if resolved in SYNC_MODES:
        return resolved
    raise ValueError(f"Invalid sync_mode: {sync_mode}")


def _validate_cadence(interval: str, cron: str, tz: str, sync_mode: str) -> str:
    cron = (cron or "").strip()
    if interval == "cron":
        # The cadence *is* the expression — a stored preset would contradict
        # it (QA D11/S07: interval=daily beside ``*/3 * * * *``).
        if not cron:
            raise ValueError("interval 'cron' requires a cron expression")
    elif interval not in INTERVALS:
        raise ValueError(f"Invalid interval: {interval}")
    sync_mode = accepted_schedule_sync_mode(sync_mode)
    if cron:
        try:
            validate_cron(cron)
            # Also validates the timezone against the cron horizon computation.
            _cron_next_run(cron, datetime.now(timezone.utc), tz or "UTC")
        except CronError as exc:
            raise ValueError(str(exc)) from exc
    return sync_mode


def assert_signed_contract(contract_id: str, *, require_signed: bool) -> None:
    """Fail closed when a schedule requires a signed data contract."""
    contract_id = (contract_id or "").strip()
    if not contract_id:
        if require_signed:
            raise ValueError("require_signed_contract is set but no contract_id was provided")
        return
    try:
        from services.contract_store import get_contract_store
        from services.data_contract import ContractStatus
    except ImportError:  # pragma: no cover
        from src.services.contract_store import get_contract_store
        from src.services.data_contract import ContractStatus

    contract = get_contract_store().get_contract(contract_id)
    if contract is None:
        raise ValueError(f"Contract {contract_id} not found")
    if require_signed and contract.status != ContractStatus.SIGNED:
        raise ValueError(
            f"Contract {contract_id} must be SIGNED before this run "
            f"(current status: {contract.status.value})"
        )


def schedule_bind_summary(sched: Any) -> dict[str, Any]:
    """Read-only bind preview for Pilot list/get. Never invents. Never raises.

    OPEN / unsigned binds still appear so the operator can see why Run is
    refused. Cron and Confirm use ``assert_schedule_run_allowed`` instead.
    """
    try:
        from services.contract_store import bound_contract_preview
    except ImportError:  # pragma: no cover
        from src.services.contract_store import bound_contract_preview

    return bound_contract_preview(
        getattr(sched, "contract_id", None) or "",
        require_signed=bool(getattr(sched, "require_signed_contract", False)),
    )


def _mapping_binding(mappings: list[dict[str, Any]] | None) -> frozenset[tuple[str, str]]:
    """Source/target pairs. Column identity, not confidence or transform spelling."""
    pairs: list[tuple[str, str]] = []
    for mapping in mappings or []:
        if not isinstance(mapping, dict):
            continue
        source = str(
            mapping.get("source") or mapping.get("source_column") or ""
        ).strip().lower()
        target = str(
            mapping.get("target") or mapping.get("target_column") or ""
        ).strip().lower() or source
        if source:
            pairs.append((source, target))
    return frozenset(pairs)


def mappings_bound_to_signed_contract(
    contract_id: str,
    planned: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Mappings a schedule stores so a later run matches the signed contract.

    The signed rows are the contract when they name the same source and target
    columns as the plan. A freshly planned confidence or a ``none`` versus
    empty transform is not a different mapping, and storing the plan's copy
    made ``run_schedule_now`` refuse a schedule the operator just bound.
    A different column binding raises the same error the runner raises, so
    Confirm never stages a schedule that cannot run. A contract with no
    stored mappings leaves the plan unchanged.
    """
    planned_rows = [m for m in (planned or []) if isinstance(m, dict)]
    cid = (contract_id or "").strip()
    if not cid:
        return planned_rows
    try:
        from services.contract_store import get_contract_store
        from services.schema_fingerprint import fingerprint_mappings
    except ImportError:  # pragma: no cover
        from src.services.contract_store import get_contract_store
        from src.services.schema_fingerprint import fingerprint_mappings

    contract = get_contract_store().get_contract(cid)
    if contract is None:
        return planned_rows
    contracted = [
        m for m in (getattr(contract, "mappings", None) or []) if isinstance(m, dict)
    ]
    if not contracted:
        return planned_rows
    if _mapping_binding(planned_rows) != _mapping_binding(contracted):
        expected = fingerprint_mappings(contracted)
        actual = fingerprint_mappings(planned_rows)
        raise ValueError(
            f"Schedule mappings do not match signed contract {cid} "
            f"(contract {expected[:12]} vs schedule {actual[:12]}). "
            "Open Validate and persist the approved mapping, or re-sign the contract."
        )
    return contracted


def assert_schedule_mapping_matches_contract(sched: Any) -> None:
    """Refuse a beat when the schedule mapping hash drifted from the signed contract.

    Contracts with no stored mappings skip this bind — we do not invent a
    fingerprint. Empty-mapping park stays a separate plan-change gate.
    """
    cid = (getattr(sched, "contract_id", None) or "").strip()
    if not cid:
        return
    try:
        from services.contract_store import get_contract_store
        from services.schema_fingerprint import fingerprint_mappings
    except ImportError:  # pragma: no cover
        from src.services.contract_store import get_contract_store
        from src.services.schema_fingerprint import fingerprint_mappings

    contract = get_contract_store().get_contract(cid)
    if contract is None:
        return
    contracted = [m for m in (getattr(contract, "mappings", None) or []) if isinstance(m, dict)]
    if not contracted:
        return
    expected = fingerprint_mappings(contracted)
    actual = fingerprint_mappings(
        [m for m in (getattr(sched, "mappings", None) or []) if isinstance(m, dict)]
    )
    if expected != actual:
        raise ValueError(
            f"Schedule mappings do not match signed contract {cid} "
            f"(contract {expected[:12]} vs schedule {actual[:12]}). "
            "Open Validate and persist the approved mapping, or re-sign the contract."
        )


def assert_schedule_run_allowed(sched: Any) -> dict[str, Any]:
    """Fail-closed SIGNED + breaker for a scheduled run. Returns bind preview.

    Cron, Pilot staging, and Pilot Confirm share this check. An unbound
    schedule returns ``{}`` so enforce stays unset (same as Studio).
    """
    cid = (getattr(sched, "contract_id", None) or "").strip()
    require = bool(getattr(sched, "require_signed_contract", False))
    if cid or require:
        assert_signed_contract(cid, require_signed=require)
    if cid:
        try:
            from services.contract_store import assert_contract_breaker_allows
        except ImportError:  # pragma: no cover
            from src.services.contract_store import assert_contract_breaker_allows

        assert_contract_breaker_allows(cid)
        assert_schedule_mapping_matches_contract(sched)
    return schedule_bind_summary(sched)


def _assert_callable_schedule_sync(data: Mapping[str, Any] | None, sync_mode: str) -> None:
    """Refuse CDC/SCD2/mirror on a persisted CALL/SELECT extract."""
    from services.procedure_source import callable_sync_refusal, source_read_mode_of

    reason = callable_sync_refusal(sync_mode, source_read_mode=source_read_mode_of(data or {}))
    if reason:
        raise ValueError(reason)


def _assert_cursor_semantics(data: Mapping[str, Any] | None) -> None:
    """Refuse a cursor meaning the product does not define.

    Whether the declaration is *sufficient* for the sync mode is the g9 gate's
    call at run time (it depends on validation mode and the live schema); an
    unknown word is refused here so it never reaches a run.
    """
    from services.cursor_semantics import CURSOR_SEMANTICS

    value = str((data or {}).get("cursor_semantics") or "").strip().lower()
    if value and value not in CURSOR_SEMANTICS:
        raise ValueError(
            f"Unknown cursor semantics '{value}' — declare one of: "
            + ", ".join(sorted(CURSOR_SEMANTICS))
        )


def find_studio_replay_target(
    schedule_id: str | None,
    source_connector_id: str,
    dest_connector_id: str,
    source_table: str,
    dest_table: str,
) -> PipelineSchedule | None:
    """The parked draft Studio must persist mappings onto — not a new pipeline.

    Prefer an explicit schedule id (inbox → Studio). Otherwise a unique
    empty-mapping draft on the same route, then a unique empty draft on the
    same connector pair (operator changed ``sample`` to a real table).
    Ambiguous matches return None so we do not hijack a sibling draft.
    """
    from services.schedule_mapping_contract import (
        is_empty_mapping_finding,
        persisted_mapping_rows,
    )

    sid = str(schedule_id or "").strip()
    if sid:
        existing = get_schedule(sid)
        if existing:
            return existing
    src = str(source_connector_id or "").strip()
    dst = str(dest_connector_id or "").strip()
    if not src or not dst:
        return None
    all_schedules = list_schedules()
    same_route = [
        s
        for s in all_schedules
        if str(s.source_connector_id or "") == src
        and str(s.dest_connector_id or "") == dst
        and str(s.source_table or "") == str(source_table or "")
        and str(s.dest_table or "") == str(dest_table or "")
        and not persisted_mapping_rows(s.mappings)
    ]
    if len(same_route) == 1:
        return same_route[0]
    if len(same_route) > 1:
        return None
    same_pair_empty = [
        s
        for s in all_schedules
        if str(s.source_connector_id or "") == src
        and str(s.dest_connector_id or "") == dst
        and not persisted_mapping_rows(s.mappings)
    ]
    if len(same_pair_empty) != 1:
        return None
    candidate = same_pair_empty[0]
    request = candidate.approval_request if isinstance(candidate.approval_request, dict) else {}
    if request and is_empty_mapping_finding(
        str(request.get("code") or ""),
        str(request.get("finding") or ""),
    ):
        return candidate
    # Never-run New schedule draft on this pair — Studio Schedule should
    # persist onto it rather than leave a second paused empty row.
    return candidate


def create_schedule(data: dict[str, Any]) -> PipelineSchedule:
    payload = dict(data)
    replay_id = str(payload.pop("replay_schedule_id", "") or "").strip()
    interval = payload.get("interval", "daily")
    cron = (payload.get("cron") or "").strip()
    tz = (payload.get("timezone") or "UTC").strip() or "UTC"
    sync_mode = payload.get("sync_mode") or "full_refresh_overwrite"
    sync_mode = _validate_cadence(interval, cron, tz, sync_mode)
    contract_id = (payload.get("contract_id") or "").strip()
    require_signed = bool(payload.get("require_signed_contract", bool(contract_id)))
    if contract_id or require_signed:
        assert_signed_contract(contract_id, require_signed=require_signed)
    _assert_callable_schedule_sync(payload, sync_mode)
    _assert_cursor_semantics(payload)
    from services.schedule_mapping_contract import persisted_mapping_rows

    rows = persisted_mapping_rows(payload.get("mappings"))
    if rows:
        target = find_studio_replay_target(
            replay_id,
            str(payload.get("source_connector_id") or ""),
            str(payload.get("dest_connector_id") or ""),
            str(payload.get("source_table") or ""),
            str(payload.get("dest_table") or ""),
        )
        if target:
            patch = {k: v for k, v in payload.items() if k != "id"}
            patch["interval"] = interval
            patch["cron"] = cron
            patch["timezone"] = tz
            patch["sync_mode"] = sync_mode
            patch["contract_id"] = contract_id
            patch["require_signed_contract"] = require_signed
            if "enabled" not in patch:
                patch["enabled"] = True
            updated = update_schedule(target.id, patch)
            if updated:
                return updated
    enabled = bool(payload.get("enabled", True))
    if enabled and not rows:
        # Draft is allowed. Enabling an empty-mapping schedule invents _auto_map.
        enabled = False
    schedules = _load_all()
    sched = PipelineSchedule.from_dict({
        **payload,
        "id": str(uuid.uuid4()),
        "interval": interval,
        "cron": cron,
        "timezone": tz,
        "sync_mode": sync_mode,
        "contract_id": contract_id,
        "require_signed_contract": require_signed,
        "enabled": enabled,
    })
    sched.next_run_at = next_run_for(sched)
    schedules.append(sched)
    _save_all(schedules)
    return sched


_VALIDATE_IDENTITY_HASH_KEYS = (
    "approved_shape_recipe_hash",
    "approved_decision_artifact_hash",
    "approved_ddl_identity_hash",
)


def drop_blank_validate_identity(data: dict[str, Any]) -> dict[str, Any]:
    """Empty hash / empty recipe on PATCH is omit, not wipe.

    FastAPI ``"" is not None`` would otherwise clear Studio stamps on an
    unrelated edit. Re-Validate still overwrites with a non-empty hash.
    """
    out = dict(data)
    for key in _VALIDATE_IDENTITY_HASH_KEYS:
        if key in out and not str(out.get(key) or "").strip():
            out.pop(key)
    recipe = out.get("shape_recipe")
    if recipe is not None and not (isinstance(recipe, dict) and recipe):
        out.pop("shape_recipe")
    return out


#: Fields the Studio Validate stamp was taken against. Editing any of them
#: describes a different route than the one that was approved.
_STAMPED_ROUTE_KEYS = (
    "source_connector_id",
    "source_table",
    "dest_connector_id",
    "dest_table",
    "mappings",
    "stream_contracts",
    "sync_mode",
)


def _route_stamp_is_stale(current: PipelineSchedule, merged: dict[str, Any]) -> bool:
    """True when this edit moved the route the Decision Artifact was hashed for."""
    before = current.to_dict()
    return any(merged.get(key) != before.get(key) for key in _STAMPED_ROUTE_KEYS)


def resolve_machine_interval(data: dict[str, Any], stored: str) -> str:
    """Runner token. A displayed cadence label is not a new preset.

    ``interval_preset`` wins. ``hourly`` / ``daily`` / ``weekly`` in
    ``interval`` is a preset change. A sentence such as ``Every 5 minutes
    UTC`` is the label a client echoed from GET, and it keeps the stored
    preset. Any other token is still an invalid interval.
    """
    raw_preset = data.get("interval_preset")
    if raw_preset is not None and str(raw_preset).strip():
        preset = str(raw_preset).strip().lower()
        if preset not in INTERVALS:
            raise ValueError(f"Invalid interval: {raw_preset}")
        return preset
    if "interval" not in data:
        return stored if stored in INTERVALS or stored == "cron" else "daily"
    spoken_raw = str(data.get("interval") or "").strip()
    spoken = spoken_raw.lower()
    if spoken in INTERVALS or spoken == "cron":
        return spoken
    if " " in spoken_raw:
        return stored if stored in INTERVALS or stored == "cron" else "daily"
    raise ValueError(f"Invalid interval: {spoken_raw}")


def project_operator_cadence(data: dict[str, Any]) -> dict[str, Any]:
    """Outward schedule JSON. Persistence keeps the runner token.

    When a cron is stored, the field operators read (``interval``) is the
    cadence label. ``interval_preset`` stays hourly/daily/weekly.
    """
    preset = str(data.get("interval") or "")
    cron = str(data.get("cron") or "").strip()
    label = str(data.get("cadence_label") or "")
    out = dict(data)
    out["interval_preset"] = preset
    if cron and label:
        out["interval"] = label
    return out


def update_schedule(schedule_id: str, data: dict[str, Any]) -> PipelineSchedule | None:
    data = drop_blank_validate_identity(data)
    schedules = _load_all()
    for i, s in enumerate(schedules):
        if s.id != schedule_id:
            continue
        interval = resolve_machine_interval(data, s.interval)
        data = {
            key: value
            for key, value in data.items()
            if key not in {"interval_preset", "cadence_label"}
        }
        data["interval"] = interval
        cron = (data.get("cron", s.cron) or "").strip()
        tz = (data.get("timezone", s.timezone) or "UTC").strip() or "UTC"
        sync_mode = data.get("sync_mode", s.sync_mode) or "full_refresh_overwrite"
        sync_mode = _validate_cadence(interval, cron, tz, sync_mode)
        data = {**data, "sync_mode": sync_mode}
        merged = {**s.to_dict(), **data, "id": schedule_id}
        contract_id = (merged.get("contract_id") or "").strip()
        require_signed = bool(
            merged.get("require_signed_contract", bool(contract_id))
        )
        # Re-validate when enabling, attaching a contract, or tightening the gate.
        enabling = bool(merged.get("enabled", s.enabled)) and not s.enabled
        contract_changed = contract_id != (s.contract_id or "") or require_signed != bool(
            s.require_signed_contract
        )
        if (enabling or contract_changed) and (contract_id or require_signed):
            assert_signed_contract(contract_id, require_signed=require_signed)
        _assert_callable_schedule_sync(merged, sync_mode)
        _assert_cursor_semantics(merged)
        from services.schedule_mapping_contract import (
            EMPTY_MAPPING_REFUSAL,
            persisted_mapping_rows,
        )

        if enabling and not persisted_mapping_rows(merged.get("mappings")):
            raise ValueError(EMPTY_MAPPING_REFUSAL)
        merged["contract_id"] = contract_id
        merged["require_signed_contract"] = require_signed
        # A stamp names one route. Carrying it onto an edited destination made
        # every later run — cadence tick and Run now alike — refuse with
        # "content_hash mismatch", with no control anywhere that could clear it.
        # Dropping it puts the schedule back where an unstamped one already is:
        # the run stamps inline and the gates decide on what is there now.
        if not any(
            str(data.get(key) or "").strip() for key in _VALIDATE_IDENTITY_HASH_KEYS
        ) and _route_stamp_is_stale(s, merged):
            merged["approved_decision_artifact_hash"] = ""
            merged["approved_ddl_identity_hash"] = ""
        updated = PipelineSchedule.from_dict(merged)
        # Recompute the next due time when the cadence changed.
        if (interval, cron, tz) != (s.interval, s.cron, s.timezone):
            updated.next_run_at = next_run_for(updated)
        schedules[i] = updated
        _save_all(schedules)
        if "approval_request" not in data:
            released = _release_empty_mapping_if_mapped(updated)
            if released:
                return released
        return updated
    return None


def _release_empty_mapping_if_mapped(sched: PipelineSchedule) -> PipelineSchedule | None:
    """Close EMPTY_MAPPING after Studio persists a replayable contract.

    A signature cannot invent column names. Persisting mappings is the plan
    change the inbox asked for — leave the finding open and the card still
    says Needs approval after the operator did the work.
    """
    from services.schedule_mapping_contract import (
        is_empty_mapping_finding,
        persisted_mapping_rows,
    )

    if not persisted_mapping_rows(sched.mappings):
        return None
    if not has_open_approval(sched):
        return None
    request = sched.approval_request if isinstance(sched.approval_request, dict) else {}
    if not is_empty_mapping_finding(
        str(request.get("code") or ""),
        str(request.get("finding") or ""),
    ):
        return None
    from services.schedule_approvals import resolve_plan_change

    result = resolve_plan_change(
        sched.id,
        actor="transfer_studio",
        reason=(
            "Validate-approved mapping contract persisted — empty-mapping park "
            "is a plan change, not a signature."
        ),
    )
    released = result.get("schedule") if result else None
    return released if isinstance(released, PipelineSchedule) else None


def delete_schedule(schedule_id: str) -> bool:
    schedules = _load_all()
    filtered = [s for s in schedules if s.id != schedule_id]
    if len(filtered) == len(schedules):
        return False
    _save_all(filtered, removed_ids=(schedule_id,))
    return True


# A running job document that has not been touched for this long is a dead
# worker, not a long migration. Checkpoints rewrite ``updated_at``. Leaving
# the claim held froze ``next_run_at`` in the past with no error.
_RUNNING_SILENCE = timedelta(minutes=15)


def _job_is_live(job_id: str) -> bool | None:
    """Whether the claimed job is still in flight. ``None`` when unknowable."""
    if not job_id:
        return None
    try:
        from services.job_status import is_terminal
        from services.mongodb_service import get_mongodb_service

        job = get_mongodb_service().get_job(job_id)
    except Exception:
        return None
    if not job:
        return False
    if is_terminal(job.get("status")):
        return False
    status = str(job.get("status") or "").strip().lower()
    if status == "running":
        stamp = (
            job.get("updated_at")
            or job.get("heartbeat_at")
            or job.get("last_progress_at")
        )
        seen = _parse_ts(stamp) if stamp else None
        if seen is not None and datetime.now(timezone.utc) - seen > _RUNNING_SILENCE:
            # A silent document is not proof of a dead worker: one long batch
            # writes no checkpoint. The worker lease is the owner signal the
            # orphan sweeper uses — one authority for "is anybody running it".
            return _job_lease_held(job_id)
    return True


def _job_lease_held(job_id: str) -> bool | None:
    try:
        from services.worker_leases import get_worker_lease_store

        return bool(get_worker_lease_store().is_held(job_id))
    except Exception as exc:
        logger.warning(
            "worker lease lookup failed for job %s (%s)",
            job_id,
            type(exc).__name__,
        )
        return None


def _unbound_claim_grace() -> timedelta:
    """How long a claim may exist before its job is bound.

    Dispatch builds the request (connector resolve, drift guard) before the job
    document exists. Under load that has taken minutes, so the window is
    configurable rather than the 4h lookup-failure ceiling it used to inherit.
    """
    try:
        seconds = int(os.getenv("DATAFLOW_SCHEDULE_UNBOUND_CLAIM_GRACE_SEC", "900"))
    except ValueError:
        seconds = 900
    return timedelta(seconds=max(60, seconds))


# Schedules whose dispatch (claim taken, job not yet bound) is executing in
# this process right now. An unbound claim with a live local dispatch is held.
_DISPATCHING: set[str] = set()
_DISPATCHING_LOCK = threading.Lock()


@contextmanager
def dispatch_in_flight(schedule_id: str):
    with _DISPATCHING_LOCK:
        _DISPATCHING.add(schedule_id)
    try:
        yield
    finally:
        with _DISPATCHING_LOCK:
            _DISPATCHING.discard(schedule_id)


def _dispatching_here(schedule_id: str) -> bool:
    with _DISPATCHING_LOCK:
        return schedule_id in _DISPATCHING


def _is_running_stale(sched: PipelineSchedule) -> bool:
    """Return True if a schedule's running flag may be reclaimed.

    A clock alone cannot answer this: a 40 GB migration legitimately runs for
    hours, and reclaiming it mid-flight starts a second writer against the same
    destination. So the claimed job's own state decides — a job still in flight
    holds the claim for as long as it runs, and a job that ended or vanished
    releases it immediately. The elapsed ceiling applies only when a bound job
    cannot be looked up at all.

    A claim with *no* bound job is not "unknown": nothing is writing yet. It is
    held while this process is still dispatching it, and otherwise released
    after :func:`_unbound_claim_grace`. Treating it as unknown wedged schedules
    for four hours whenever dispatch died between claim and bind (QA MX3-24).
    """
    if not sched.running:
        return True
    started = _parse_ts(sched.running_started_at)
    if started is None:
        return True
    age = datetime.now(timezone.utc) - started
    job_id = str(sched.running_job_id or "").strip()
    if not job_id:
        if _dispatching_here(sched.id):
            return False
        return age > _unbound_claim_grace()
    live = _job_is_live(job_id)
    if live is True:
        return False
    if live is False:
        return True
    return age > CLAIM_MAX_RUNTIME


def claim_holder(
    sched: PipelineSchedule,
    *,
    snapshot: Sequence[PipelineSchedule] | None = None,
) -> dict[str, Any] | None:
    """Who blocks this schedule from claiming a run, or ``None`` when free.

    This *is* the decision :func:`mark_schedule_running` takes — one owner —
    so the operator-facing reason can never disagree with the refusal.
    """
    if sched.running and not _is_running_stale(sched):
        return _holder_entry("this_schedule", sched)
    peers = list(snapshot) if snapshot is not None else _load_all()
    dest = _dest_object_holder(
        sched.dest_connector_id, sched.dest_table, sched.id, peers
    )
    if dest is not None:
        return _holder_entry("same_destination", dest)
    pair = _connector_pair_holder(
        sched.source_connector_id, sched.dest_connector_id, sched.id, peers
    )
    if pair is not None:
        return _holder_entry("same_connector_pair", pair)
    return None


_REFUSALS: dict[str, dict[str, Any]] = {}
_REFUSALS_LOCK = threading.Lock()


def _record_refusal(schedule_id: str, holder: dict[str, Any] | None) -> None:
    with _REFUSALS_LOCK:
        if holder is None:
            _REFUSALS.pop(schedule_id, None)
        else:
            _REFUSALS[schedule_id] = {**holder, "observed_at": _now()}


def last_claim_refusal(schedule_id: str) -> dict[str, Any] | None:
    """The holder that refused this schedule's most recent claim attempt."""
    with _REFUSALS_LOCK:
        found = _REFUSALS.get(schedule_id)
        return dict(found) if found else None


def _holder_entry(kind: str, holder: PipelineSchedule) -> dict[str, Any]:
    return {
        "kind": kind,
        "schedule_id": holder.id,
        "schedule_name": holder.name,
        "job_id": str(holder.running_job_id or ""),
        "running_since": holder.running_started_at,
        "running_instance": holder.running_instance,
        "dest_table": holder.dest_table,
    }


def describe_claim_holder(holder: Mapping[str, Any] | None) -> str:
    """One operator sentence naming exactly what holds the claim."""
    if not holder:
        return "A run is already in progress for this schedule."
    job = str(holder.get("job_id") or "")
    since = str(holder.get("running_since") or "")
    tail = (f" (job {job}" if job else " (no job bound yet") + (
        f", running since {since})" if since else ")"
    )
    kind = holder.get("kind")
    if kind == "this_schedule":
        return "A run of this schedule is already in progress" + tail + "."
    name = holder.get("schedule_name") or holder.get("schedule_id")
    what = (
        f"destination table {holder.get('dest_table')!s}"
        if kind == "same_destination"
        else "the same source→destination connector pair"
    )
    return f"Schedule “{name}” is writing {what}" + tail + " — one writer at a time."


def _job_dispatch_state(job_id: str) -> str:
    """``queued``, ``running``, ``terminal``, or ``unknown``.

    A job that is still waiting for a worker is not a writer. It is kept:
    cancelling it for a later slot drops a fire that has not read yet.
    """
    if not (job_id or "").strip():
        return "unknown"
    try:
        from services.job_status import is_terminal

        job = get_mongodb_service().get_job(job_id)
    except (OSError, RuntimeError, ValueError, TypeError, ImportError):
        return "unknown"
    if not job:
        return "terminal"
    status = str(job.get("status") or "").strip().lower()
    phase = str(job.get("phase") or "").strip().lower()
    if is_terminal(status):
        return "terminal"
    if status in {"pending", "queued"} or phase in {"pending", "queued"}:
        return "queued"
    if status == "running":
        return "running"
    return "unknown"


def release_superseded_queued_claim(
    schedule_id: str, now: datetime | None = None
) -> PipelineSchedule | None:
    """Keep a queued schedule job. Do not cancel it for a later slot.

    The job has not read the source yet, so it is not a stale snapshot.
    Cancelling it while a large transfer still holds the workers drops the
    fire: the replacement waits in the same queue and the next slot cancels
    that one too. Missed slots are counted when this job finishes, and a
    running writer is still one writer — the overrun is one catch-up.
    """
    del schedule_id, now
    return None


def release_all_superseded_queued_claims(now: datetime | None = None) -> int:
    """Fleet form of :func:`release_superseded_queued_claim`. Never cancels."""
    del now
    return 0


def _claim_running_mongo(
    schedule_id: str,
    instance: str,
    now: str,
    *,
    next_run_at: str | None = None,
    stale: PipelineSchedule | None = None,
) -> PipelineSchedule | None:
    """CAS the running flag on the per-schedule Mongo document.

    Free claims match ``running`` false/absent. A *stale* claim is reclaimed
    only when the document still carries exactly the claim that was judged
    stale (same start stamp and job id): two beats that both judged it stale
    cannot both win, and a claim re-taken meanwhile is not stolen.
    """
    svc = _mongo_backend()
    if not svc:
        return None
    coll = svc.get_database()["pipeline_schedules"]
    free: list[dict[str, Any]] = [
        {"running": {"$in": [False, None]}},
        {"running": {"$exists": False}},
    ]
    if stale is not None and stale.running:
        free.append(
            {
                "running": True,
                "running_started_at": stale.running_started_at,
                "running_job_id": {"$in": [str(stale.running_job_id or ""), None]}
                if not str(stale.running_job_id or "")
                else str(stale.running_job_id),
            }
        )
    result = coll.find_one_and_update(
        {"_id": schedule_id, "$or": free},
        {
            "$set": {
                "running": True,
                "running_instance": instance,
                "running_started_at": now,
                "running_job_id": "",
                **({"next_run_at": next_run_at} if next_run_at else {}),
            },
            "$inc": {"version": 1},
        },
        return_document=True,
    )
    if not result:
        return None
    claimed = _schedule_from_doc(result)
    _remember_base(claimed)
    return claimed


def mark_schedule_running(schedule_id: str, instance: str) -> PipelineSchedule | None:
    """Mark a schedule as running on this instance.

    Acts as a concurrency guard: returns ``None`` if this schedule, another
    schedule writing the same destination object, or another schedule for the
    same source→dest connector pair already has a live, non-stale run.
    """
    with _CLAIM_LOCK:
        schedules = _load_all()
        now = _now()
        for i, s in enumerate(schedules):
            if s.id != schedule_id:
                continue
            holder = claim_holder(s, snapshot=schedules)
            _record_refusal(schedule_id, holder)
            if holder is not None:
                return None
            # One catch-up for a slot already in the past, then the next
            # cadence boundary. Leaving next_run_at behind made every beat
            # fire again (a resumed schedule ran three times in one window)
            # and a crashed finalize left the schedule due forever.
            nxt = _parse_ts(s.next_run_at)
            current = _parse_ts(now) or datetime.now(timezone.utc)
            advanced = s.next_run_at
            if nxt is None or nxt <= current:
                advanced = compute_next_run(
                    s.interval, current, cron=s.cron, tz=s.timezone
                )
            if _mongo_backend() is not None:
                # The CAS is the only claim on a shared store. Falling back to a
                # snapshot write when it lost let two beats both "win" a stale
                # reclaim and start two writers against one destination.
                claimed = _claim_running_mongo(
                    schedule_id, instance, now, next_run_at=advanced, stale=s
                )
                if claimed is None:
                    _record_refusal(
                        schedule_id,
                        {"kind": "claim_lost", "schedule_id": schedule_id,
                         "schedule_name": s.name, "job_id": "", "running_since": None},
                    )
                return claimed
            updated = PipelineSchedule.from_dict({
                **s.to_dict(),
                "running": True,
                "running_instance": instance,
                "running_started_at": now,
                "running_job_id": "",
                "next_run_at": advanced,
            })
            schedules[i] = updated
            _save_all(schedules)
            return updated
        return None


def clear_schedule_running(schedule_id: str) -> PipelineSchedule | None:
    """Clear the running flag after the transfer finishes or fails."""
    schedules = _load_all()
    for i, s in enumerate(schedules):
        if s.id != schedule_id:
            continue
        updated = PipelineSchedule.from_dict({
            **s.to_dict(),
            "running": False,
            "running_instance": "",
            "running_started_at": None,
            "running_job_id": "",
        })
        schedules[i] = updated
        _save_all(schedules)
        return updated


def set_running_job(schedule_id: str, job_id: str) -> PipelineSchedule | None:
    """Record which job the running claim is held for."""
    schedules = _load_all()
    for i, s in enumerate(schedules):
        if s.id != schedule_id:
            continue
        updated = PipelineSchedule.from_dict({**s.to_dict(), "running_job_id": job_id})
        schedules[i] = updated
        _save_all(schedules)
        return updated
    return None


def mark_schedule_run(
    schedule_id: str,
    job_id: str,
    *,
    status: str | None = None,
    run_entry: dict[str, Any] | None = None,
    cursor_value: str | None = None,
) -> PipelineSchedule | None:
    """Record a completed/failed run and compute the next due time.

    ``run_entry`` (status, row counts, rejected/coerced counts, duration) is
    appended to the capped ``run_history``. ``cursor_value`` advances the
    incremental watermark so the next run only reads new rows.
    """
    schedules = _load_all()
    now = _now()
    for i, s in enumerate(schedules):
        if s.id != schedule_id:
            continue
        # A lost callback and the beat that notices the job ended can both
        # arrive. Recording the same job twice advanced run_count and skipped
        # a cron slot.
        if job_id and s.last_job_id == job_id and not s.running:
            return s
        missed = count_missed_windows(
            cron=s.cron, interval=s.interval, tz=s.timezone, next_run_at=s.next_run_at
        )
        # Windows that elapsed while this run was busy are counted, not
        # replayed. Pinning next_run to the completion instant left the
        # schedule due at a time that is not a cadence boundary, and the
        # following ticks never advanced (DEF-A-010). The run that just
        # finished is the catch-up for the overdue slot.
        next_at = compute_next_run(s.interval, _parse_ts(now), cron=s.cron, tz=s.timezone)
        history = list(s.run_history)
        if run_entry:
            entry = dict(run_entry)
            if missed:
                entry["missed_windows"] = missed
                entry["scheduled_for"] = s.next_run_at
            history.append(entry)
            history = history[-RUN_HISTORY_LIMIT:]
        payload = {
            **s.to_dict(),
            "last_run_at": now,
            "next_run_at": next_at,
            "last_job_id": job_id,
            "last_status": status or s.last_status,
            "run_count": s.run_count + 1,
            "missed_window_count": s.missed_window_count + missed,
            "last_missed_windows": missed,
            "retry_at": None,
            "retry_attempt": 0,
            "running": False,
            "running_instance": "",
            "running_started_at": None,
            "running_job_id": "",
            "run_history": history,
        }
        if cursor_value is not None:
            payload["cursor_value"] = str(cursor_value)
        updated = PipelineSchedule.from_dict(payload)
        schedules[i] = updated
        _save_all(schedules)
        return updated
    return None


def _pinned_to_completion(sched: PipelineSchedule, *, now: datetime | None = None) -> bool:
    """True when next_run was written as the completion instant and is now due.

    A real cadence boundary is not equal to last_run. Interval schedules can
    legally carry seconds, so the signal is next_run ≈ last_run, not "has seconds".
    """
    if sched.running:
        return False
    nxt = _parse_ts(sched.next_run_at)
    last = _parse_ts(sched.last_run_at)
    if nxt is None or last is None:
        return False
    if nxt.tzinfo is None:
        nxt = nxt.replace(tzinfo=timezone.utc)
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    if abs((nxt - last).total_seconds()) > 2:
        return False
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return nxt <= current


def unstick_completion_pinned_schedules(now: datetime | None = None) -> int:
    """Advance schedules whose next run is stuck on a previous completion.

    Does not start a load and does not replay the missed ticks. The next due
    time becomes the next real cadence boundary from now, and the skipped
    windows are added to the existing miss count.
    """
    current = now or datetime.now(timezone.utc)
    schedules = _load_all()
    changed = 0
    for i, sched in enumerate(schedules):
        if not _pinned_to_completion(sched, now=current):
            continue
        missed = count_missed_windows(
            cron=sched.cron,
            interval=sched.interval,
            tz=sched.timezone,
            next_run_at=sched.next_run_at,
            now=current,
        )
        next_at = compute_next_run(
            sched.interval, current, cron=sched.cron, tz=sched.timezone
        )
        schedules[i] = PipelineSchedule.from_dict({
            **sched.to_dict(),
            "next_run_at": next_at,
            "missed_window_count": sched.missed_window_count + missed,
            "last_missed_windows": missed,
        })
        changed += 1
    if changed:
        _save_all(schedules)
    return changed


def schedule_retry(
    schedule_id: str,
    *,
    retry_at: datetime,
    attempt: int,
    run_entry: dict[str, Any] | None = None,
) -> PipelineSchedule | None:
    """Park a failed attempt for another try without ending the schedule's run.

    The retry is written to the store and the running claim released, so the
    next beat picks it up through the same concurrency guard as any other run.
    Holding the attempt in an in-process timer instead loses it whenever the
    service restarts — routine on a rolling deploy — and leaves the schedule
    flagged running until the staleness window expires hours later.
    """
    schedules = _load_all()
    for i, s in enumerate(schedules):
        if s.id != schedule_id:
            continue
        history = list(s.run_history)
        if run_entry:
            history = (history + [run_entry])[-RUN_HISTORY_LIMIT:]
        updated = PipelineSchedule.from_dict({
            **s.to_dict(),
            "retry_at": retry_at.astimezone(timezone.utc).isoformat(),
            "retry_attempt": max(0, int(attempt)),
            "running": False,
            "running_instance": "",
            "running_started_at": None,
            "running_job_id": "",
            "run_history": history,
        })
        schedules[i] = updated
        _save_all(schedules)
        return updated
    return None


def record_run_history(schedule_id: str, run_entry: dict[str, Any]) -> PipelineSchedule | None:
    """Append a run-history entry without altering cadence/running state.

    Used to log intermediate retry attempts before the terminal ``mark_schedule_run``.
    """
    schedules = _load_all()
    for i, s in enumerate(schedules):
        if s.id != schedule_id:
            continue
        history = (list(s.run_history) + [run_entry])[-RUN_HISTORY_LIMIT:]
        updated = PipelineSchedule.from_dict({**s.to_dict(), "run_history": history})
        schedules[i] = updated
        _save_all(schedules)
        return updated
    return None


def _norm_dest_object(dest_connector_id: str, dest_table: str) -> tuple[str, str] | None:
    dest_c = (dest_connector_id or "").strip()
    dest_t = (dest_table or "").strip().lower()
    if not dest_c or not dest_t:
        return None
    return dest_c, dest_t


def dest_object_busy(
    dest_connector_id: str, dest_table: str, exclude_id: str = ""
) -> bool:
    """True if another non-stale schedule is writing the same dest object.

    Connector-pair lock is not enough: two sources can share a destination
    table and both append. The destination object (dest connector + table)
    is the write lock — one writer at a time, dest COUNT cannot double.
    """
    return _dest_object_holder(dest_connector_id, dest_table, exclude_id) is not None


def _dest_object_holder(
    dest_connector_id: str,
    dest_table: str,
    exclude_id: str = "",
    snapshot: Sequence[PipelineSchedule] | None = None,
) -> PipelineSchedule | None:
    needle = _norm_dest_object(dest_connector_id, dest_table)
    if needle is None:
        return None
    for s in snapshot if snapshot is not None else _load_all():
        if s.id == exclude_id:
            continue
        if _norm_dest_object(s.dest_connector_id, s.dest_table) != needle:
            continue
        if s.running and not _is_running_stale(s):
            return s
    return None


def connector_pair_busy(source_connector_id: str, dest_connector_id: str, exclude_id: str = "") -> bool:
    """Return True if another non-stale schedule for the same connector pair is running."""
    return (
        _connector_pair_holder(source_connector_id, dest_connector_id, exclude_id)
        is not None
    )


def _connector_pair_holder(
    source_connector_id: str,
    dest_connector_id: str,
    exclude_id: str = "",
    snapshot: Sequence[PipelineSchedule] | None = None,
) -> PipelineSchedule | None:
    for s in snapshot if snapshot is not None else _load_all():
        if s.id == exclude_id:
            continue
        if s.source_connector_id != source_connector_id or s.dest_connector_id != dest_connector_id:
            continue
        if s.running and not _is_running_stale(s):
            return s
    return None


def has_open_approval(sched: Any) -> bool:
    """True while a finding on this schedule is waiting on a human.

    Only an unresolved request holds the cadence: a request that was approved or
    rejected keeps its record for the audit trail but no longer blocks.
    """
    req = getattr(sched, "approval_request", None) or {}
    if not isinstance(req, dict) or not req:
        return False
    return str(req.get("status") or "open").strip().lower() == "open"


def due_schedules(now: datetime | None = None) -> list[PipelineSchedule]:
    from services.schedule_mapping_contract import persisted_mapping_rows

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    due: list[PipelineSchedule] = []
    for s in _load_all():
        try:
            if not s.enabled:
                continue
            if not persisted_mapping_rows(s.mappings):
                continue
            if s.running and not _is_running_stale(s):
                continue
            if has_open_approval(s):
                # A deterministic refusal waiting on a decision is not a cadence
                # event: the same inputs would produce the same refusal, so running
                # it again only buries the finding under identical failures.
                continue
            retry_at = _parse_ts(s.retry_at)
            if retry_at is not None:
                # A parked retry owns the schedule until it runs: the cadence must
                # not start a fresh attempt on top of the one still owed.
                if retry_at <= current:
                    due.append(s)
                continue
            nxt = _parse_ts(s.next_run_at)
            if nxt is None or nxt <= current:
                due.append(s)
        except Exception:  # noqa: BLE001 — one document must not zero the beat
            logging.getLogger(__name__).exception(
                "Schedule %s skipped this beat; its due time could not be read",
                getattr(s, "id", ""),
            )
    return due


def seconds_until_next_schedule(
    now: datetime | None = None,
    *,
    cap: float = 60.0,
) -> float:
    """Seconds to sleep before the next enabled schedule is due.

    The beat used to sleep a fixed 60s after every pass, so a schedule that
    became due one second later waited almost a full minute (measured 25s,
    then 62s). Sleep until the soonest ``next_run_at``, never longer than
    ``cap``. An already-due schedule waits 1s so a document that fails to
    advance ``next_run_at`` cannot busy-loop the process.
    """
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    soonest: datetime | None = None
    for s in _load_all():
        try:
            if not s.enabled:
                continue
            nxt = _parse_ts(getattr(s, "next_run_at", None))
        except Exception:  # noqa: BLE001 — one document must not pin the sleep
            continue
        if nxt is None:
            continue
        if soonest is None or nxt < soonest:
            soonest = nxt
    if soonest is None:
        return float(cap)
    delta = (soonest - current).total_seconds()
    if delta <= 0:
        return 1.0
    return float(min(cap, max(1.0, delta)))
