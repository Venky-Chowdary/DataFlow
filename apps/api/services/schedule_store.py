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

from services.platform_config import data_dir
from services.value_serializer import json_default

try:
    from src.services.mongodb_service import get_mongodb_service
except ImportError:
    from services.mongodb_service import get_mongodb_service

STORE_PATH = data_dir() / "schedules.json"

INTERVALS = {"hourly": timedelta(hours=1), "daily": timedelta(days=1), "weekly": timedelta(weeks=1)}


@dataclass
class PipelineSchedule:
    id: str
    name: str
    source_connector_id: str
    source_table: str
    dest_connector_id: str
    dest_table: str
    interval: str  # hourly | daily | weekly
    enabled: bool = True
    last_run_at: str | None = None
    next_run_at: str | None = None
    last_job_id: str | None = None
    run_count: int = 0
    running: bool = False
    running_instance: str = ""
    running_started_at: str | None = None
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
            last_run_at=data.get("last_run_at"),
            next_run_at=data.get("next_run_at"),
            last_job_id=data.get("last_job_id"),
            run_count=int(data.get("run_count", 0)),
            running=bool(data.get("running", False)),
            running_instance=data.get("running_instance", ""),
            running_started_at=data.get("running_started_at"),
            created_at=data.get("created_at", _now()),
        )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def compute_next_run(interval: str, from_time: datetime | None = None) -> str:
    base = from_time or datetime.now(timezone.utc)
    delta = INTERVALS.get(interval, INTERVALS["daily"])
    return (base + delta).isoformat()


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
    doc = db["schedule_store"].find_one({"_id": "primary"})
    if not doc:
        return []
    return [PipelineSchedule.from_dict(s) for s in doc.get("schedules", [])]


def _save_mongo(svc, schedules: list[PipelineSchedule]) -> None:
    db = svc.get_database()
    db["schedule_store"].replace_one(
        {"_id": "primary"},
        {"_id": "primary", "schedules": [s.to_dict() for s in schedules]},
        upsert=True,
    )


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


def create_schedule(data: dict[str, Any]) -> PipelineSchedule:
    schedules = _load_all()
    interval = data.get("interval", "daily")
    if interval not in INTERVALS:
        raise ValueError(f"Invalid interval: {interval}")
    sched = PipelineSchedule(
        id=str(uuid.uuid4()),
        name=data["name"],
        source_connector_id=data["source_connector_id"],
        source_table=data["source_table"],
        dest_connector_id=data["dest_connector_id"],
        dest_table=data["dest_table"],
        interval=interval,
        enabled=bool(data.get("enabled", True)),
        next_run_at=compute_next_run(interval),
    )
    schedules.append(sched)
    _save_all(schedules)
    return sched


def update_schedule(schedule_id: str, data: dict[str, Any]) -> PipelineSchedule | None:
    schedules = _load_all()
    for i, s in enumerate(schedules):
        if s.id != schedule_id:
            continue
        interval = data.get("interval", s.interval)
        if interval not in INTERVALS:
            raise ValueError(f"Invalid interval: {interval}")
        updated = PipelineSchedule.from_dict({**s.to_dict(), **data, "id": schedule_id})
        if interval != s.interval:
            updated.next_run_at = compute_next_run(interval)
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
    """Mark a schedule as running on this instance."""
    schedules = _load_all()
    now = _now()
    for i, s in enumerate(schedules):
        if s.id != schedule_id:
            continue
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


def mark_schedule_run(schedule_id: str, job_id: str) -> PipelineSchedule | None:
    """Record a completed/failed run and compute the next due time."""
    schedules = _load_all()
    now = _now()
    for i, s in enumerate(schedules):
        if s.id != schedule_id:
            continue
        updated = PipelineSchedule.from_dict({
            **s.to_dict(),
            "last_run_at": now,
            "next_run_at": compute_next_run(s.interval, _parse_ts(now)),
            "last_job_id": job_id,
            "run_count": s.run_count + 1,
            "running": False,
            "running_instance": "",
            "running_started_at": None,
        })
        schedules[i] = updated
        _save_all(schedules)
        return updated
    return None


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
