"""Recurring pipeline schedules — shared persistence.

When a real MongoDB is available schedules are stored in a collection so
multi-instance Railway deployments share the same schedule state.  Otherwise
they fall back to a JSON file in ``data_dir``.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from services.cron_schedule import CronError, next_run as _cron_next_run, validate_cron
from services.platform_config import data_dir
from services.value_serializer import json_default

try:
    from src.services.mongodb_service import get_mongodb_service
except ImportError:
    from services.mongodb_service import get_mongodb_service

STORE_PATH = data_dir() / "schedules.json"

INTERVALS = {"hourly": timedelta(hours=1), "daily": timedelta(days=1), "weekly": timedelta(weeks=1)}
SYNC_MODES = {"full_refresh_overwrite", "full_refresh_append", "incremental", "cdc"}
# Keep only the most recent N runs per schedule so the history document stays small.
RUN_HISTORY_LIMIT = 25


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
    mappings: list[dict] = field(default_factory=list)
    stream_contracts: list[dict] = field(default_factory=list)
    cursor_column: str = ""  # watermark column for incremental syncs
    primary_key: str = ""  # key for idempotent incremental/cdc upserts
    cursor_value: str = ""  # last observed watermark (advances each run)
    workspace_id: str = ""
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
    running: bool = False
    running_instance: str = ""
    running_started_at: str | None = None
    run_history: list[dict] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: _now())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PipelineSchedule:
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
            mappings=list(data.get("mappings") or []),
            stream_contracts=list(data.get("stream_contracts") or []),
            cursor_column=(data.get("cursor_column") or "").strip(),
            primary_key=(data.get("primary_key") or "").strip(),
            cursor_value=str(data.get("cursor_value") or ""),
            workspace_id=(data.get("workspace_id") or "").strip(),
            max_retries=max(0, int(data.get("max_retries", 0) or 0)),
            retry_backoff_seconds=max(0, int(data.get("retry_backoff_seconds", 60) or 0)),
            notify_on_failure=bool(data.get("notify_on_failure", True)),
            notify_on_success=bool(data.get("notify_on_success", False)),
            last_run_at=data.get("last_run_at"),
            next_run_at=data.get("next_run_at"),
            last_job_id=data.get("last_job_id"),
            last_status=data.get("last_status"),
            run_count=int(data.get("run_count", 0)),
            running=bool(data.get("running", False)),
            running_instance=data.get("running_instance", ""),
            running_started_at=data.get("running_started_at"),
            run_history=list(data.get("run_history") or []),
            created_at=data.get("created_at", _now()),
        )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def compute_next_run(
    interval: str,
    from_time: datetime | None = None,
    *,
    cron: str = "",
    tz: str = "UTC",
) -> str:
    """Compute the next due time as an ISO-8601 UTC timestamp.

    A cron expression (5-field) takes precedence over the interval preset. Both
    cron and preset cadences are evaluated in the schedule's IANA ``tz`` so DST
    boundaries are respected.
    """
    base = from_time or datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    cron = (cron or "").strip()
    if cron:
        return _cron_next_run(cron, base, tz or "UTC").isoformat()
    delta = INTERVALS.get(interval, INTERVALS["daily"])
    return (base.astimezone(timezone.utc) + delta).isoformat()


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
        return [PipelineSchedule.from_dict({**d, "id": d.get("id") or str(d.get("_id"))}) for d in docs]
    doc = db["schedule_store"].find_one({"_id": "primary"})
    if not doc:
        return []
    return [PipelineSchedule.from_dict(s) for s in doc.get("schedules", [])]


def _save_mongo(svc, schedules: list[PipelineSchedule]) -> None:
    """Persist schedules as individual docs with version CAS (no whole-blob races)."""
    db = svc.get_database()
    coll = db["pipeline_schedules"]
    seen = set()
    for s in schedules:
        seen.add(s.id)
        payload = s.to_dict()
        payload["_id"] = s.id
        for attempt in range(5):
            existing = coll.find_one({"_id": s.id})
            version = int((existing or {}).get("version") or 0)
            filt = {"_id": s.id, "$or": [{"version": version}, {"version": {"$exists": False}}]}
            if existing is None:
                filt = {"_id": s.id}
            result = coll.find_one_and_update(
                filt,
                {
                    "$set": {**payload, "version": version + 1},
                    "$setOnInsert": {"_id": s.id},
                },
                upsert=True,
                return_document=True,
            )
            if result is not None:
                break
        else:
            # Last writer wins for this schedule id after CAS retries.
            coll.replace_one({"_id": s.id}, {**payload, "version": 1}, upsert=True)
    # Remove schedules deleted from the in-memory snapshot.
    if seen:
        coll.delete_many({"_id": {"$nin": list(seen)}})


def _load_all() -> list[PipelineSchedule]:
    svc = _mongo_backend()
    if svc:
        return _load_mongo(svc)
    if not STORE_PATH.exists():
        return []
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
        return [PipelineSchedule.from_dict(s) for s in raw.get("schedules", [])]
    except Exception:
        return []


def _save_all(schedules: list[PipelineSchedule]) -> None:
    svc = _mongo_backend()
    if svc:
        _save_mongo(svc, schedules)
        return
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STORE_PATH.write_text(
        json.dumps({"schedules": [s.to_dict() for s in schedules]}, indent=2, default=json_default),
        encoding="utf-8",
    )


def list_schedules() -> list[PipelineSchedule]:
    return sorted(_load_all(), key=lambda s: s.created_at, reverse=True)


def get_schedule(schedule_id: str) -> PipelineSchedule | None:
    for s in _load_all():
        if s.id == schedule_id:
            return s
    return None


def _validate_cadence(interval: str, cron: str, tz: str, sync_mode: str) -> None:
    if interval not in INTERVALS:
        raise ValueError(f"Invalid interval: {interval}")
    if sync_mode not in SYNC_MODES:
        raise ValueError(f"Invalid sync_mode: {sync_mode}")
    cron = (cron or "").strip()
    if cron:
        try:
            validate_cron(cron)
            # Also validates the timezone against the cron horizon computation.
            _cron_next_run(cron, datetime.now(timezone.utc), tz or "UTC")
        except CronError as exc:
            raise ValueError(str(exc)) from exc


def create_schedule(data: dict[str, Any]) -> PipelineSchedule:
    schedules = _load_all()
    interval = data.get("interval", "daily")
    cron = (data.get("cron") or "").strip()
    tz = (data.get("timezone") or "UTC").strip() or "UTC"
    sync_mode = data.get("sync_mode") or "full_refresh_overwrite"
    _validate_cadence(interval, cron, tz, sync_mode)
    sched = PipelineSchedule.from_dict({
        **data,
        "id": str(uuid.uuid4()),
        "interval": interval,
        "cron": cron,
        "timezone": tz,
        "sync_mode": sync_mode,
        "enabled": bool(data.get("enabled", True)),
    })
    sched.next_run_at = next_run_for(sched)
    schedules.append(sched)
    _save_all(schedules)
    return sched


def update_schedule(schedule_id: str, data: dict[str, Any]) -> PipelineSchedule | None:
    schedules = _load_all()
    for i, s in enumerate(schedules):
        if s.id != schedule_id:
            continue
        interval = data.get("interval", s.interval)
        cron = (data.get("cron", s.cron) or "").strip()
        tz = (data.get("timezone", s.timezone) or "UTC").strip() or "UTC"
        sync_mode = data.get("sync_mode", s.sync_mode) or "full_refresh_overwrite"
        _validate_cadence(interval, cron, tz, sync_mode)
        updated = PipelineSchedule.from_dict({**s.to_dict(), **data, "id": schedule_id})
        # Recompute the next due time when the cadence changed.
        if (interval, cron, tz) != (s.interval, s.cron, s.timezone):
            updated.next_run_at = next_run_for(updated)
        schedules[i] = updated
        _save_all(schedules)
        return updated
    return None


def delete_schedule(schedule_id: str) -> bool:
    schedules = _load_all()
    filtered = [s for s in schedules if s.id != schedule_id]
    if len(filtered) == len(schedules):
        return False
    _save_all(filtered)
    return True


def _is_running_stale(sched: PipelineSchedule) -> bool:
    """Return True if a schedule's running flag is too old to be trustworthy."""
    if not sched.running:
        return True
    started = _parse_ts(sched.running_started_at)
    if started is None:
        return True
    # Allow a generous 4-hour runtime before treating a run as stale.
    return (datetime.now(timezone.utc) - started) > timedelta(hours=4)


def mark_schedule_running(schedule_id: str, instance: str) -> PipelineSchedule | None:
    """Mark a schedule as running on this instance.

    Acts as a concurrency guard: returns ``None`` if this schedule (or another
    schedule for the same source→dest connector pair) already has a live,
    non-stale run in flight.
    """
    schedules = _load_all()
    now = _now()
    for i, s in enumerate(schedules):
        if s.id != schedule_id:
            continue
        if s.running and not _is_running_stale(s):
            return None
        if connector_pair_busy(s.source_connector_id, s.dest_connector_id, exclude_id=s.id):
            return None
        updated = PipelineSchedule.from_dict({
            **s.to_dict(),
            "running": True,
            "running_instance": instance,
            "running_started_at": now,
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
        })
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
        history = list(s.run_history)
        if run_entry:
            history.append(run_entry)
            history = history[-RUN_HISTORY_LIMIT:]
        payload = {
            **s.to_dict(),
            "last_run_at": now,
            "next_run_at": compute_next_run(s.interval, _parse_ts(now), cron=s.cron, tz=s.timezone),
            "last_job_id": job_id,
            "last_status": status or s.last_status,
            "run_count": s.run_count + 1,
            "running": False,
            "running_instance": "",
            "running_started_at": None,
            "run_history": history,
        }
        if cursor_value is not None:
            payload["cursor_value"] = str(cursor_value)
        updated = PipelineSchedule.from_dict(payload)
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


def connector_pair_busy(source_connector_id: str, dest_connector_id: str, exclude_id: str = "") -> bool:
    """Return True if another non-stale schedule for the same connector pair is running."""
    for s in _load_all():
        if s.id == exclude_id:
            continue
        if s.source_connector_id != source_connector_id or s.dest_connector_id != dest_connector_id:
            continue
        if s.running and not _is_running_stale(s):
            return True
    return False


def due_schedules(now: datetime | None = None) -> list[PipelineSchedule]:
    current = now or datetime.now(timezone.utc)
    due: list[PipelineSchedule] = []
    for s in _load_all():
        if not s.enabled:
            continue
        if s.running and not _is_running_stale(s):
            continue
        nxt = _parse_ts(s.next_run_at)
        if nxt is None or nxt <= current:
            due.append(s)
    return due
