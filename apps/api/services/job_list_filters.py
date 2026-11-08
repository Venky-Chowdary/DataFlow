"""Shared, bounded query rules for the operator-facing Jobs list."""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from services.job_status import COMPLETED_STATUSES

RUNNING_STATUSES = ("running", "pending", "queued", "retrying")
SEARCH_FIELDS = (
    "_id",
    "name",
    "source_name",
    "source_type",
    "destination_type",
    "destination_database",
    "destination_collection",
    "status",
    "error",
    "job_id",
)
SEARCH_MAX = 120


def status_values(status: str | None) -> tuple[str, ...] | None:
    value = str(status or "all").strip().lower()
    if value == "running":
        return RUNNING_STATUSES
    if value == "completed":
        return tuple(sorted(COMPLETED_STATUSES | {"success", "succeeded"}))
    if value == "failed":
        return ("failed",)
    return None


def _day_start(value: date | None) -> datetime | None:
    if value is None:
        return None
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def mongo_job_list_query(
    base: dict[str, Any] | None = None,
    *,
    status: str | None = None,
    search: str = "",
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    clauses: list[dict[str, Any]] = []
    if base:
        clauses.append(base)

    statuses = status_values(status)
    if statuses:
        clauses.append({"status": {"$in": list(statuses)}})

    needle = str(search or "").strip()[:SEARCH_MAX]
    if needle:
        expression = re.escape(needle)
        alternatives = [
            {field: {"$regex": expression, "$options": "i"}}
            for field in SEARCH_FIELDS
        ]
        if re.fullmatch(r"[a-fA-F0-9]{1,24}", needle):
            try:
                from bson import ObjectId

                prefix = needle.lower()
                alternatives.append({
                    "_id": {
                        "$gte": ObjectId(prefix.ljust(24, "0")),
                        "$lte": ObjectId(prefix.ljust(24, "f")),
                    }
                })
            except Exception:  # noqa: BLE001 - malformed/legacy IDs remain text searchable
                pass
        clauses.append({"$or": alternatives})

    created: dict[str, datetime] = {}
    lower = _day_start(date_from)
    upper_day = date_to + timedelta(days=1) if date_to else None
    upper = _day_start(upper_day)
    if lower:
        created["$gte"] = lower
    if upper:
        created["$lt"] = upper
    if created:
        clauses.append({"created_at": created})

    if not clauses:
        return {}
    return clauses[0] if len(clauses) == 1 else {"$and": clauses}


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def memory_job_matches(
    job: dict[str, Any],
    *,
    status: str | None = None,
    search: str = "",
    date_from: date | None = None,
    date_to: date | None = None,
) -> bool:
    statuses = status_values(status)
    job_status = str(job.get("status") or "unknown").strip().lower()
    if statuses and job_status not in statuses:
        return False

    needle = str(search or "").strip()[:SEARCH_MAX].casefold()
    if needle:
        values = [job.get(field) for field in SEARCH_FIELDS]
        streams = job.get("stream_names")
        if isinstance(streams, (list, tuple)):
            values.extend(streams)
        if not any(needle in str(value).casefold() for value in values if value is not None):
            return False

    if date_from or date_to:
        created = _parse_timestamp(job.get("created_at"))
        if created is None:
            return False
        created_day = created.date()
        if date_from and created_day < date_from:
            return False
        if date_to and created_day > date_to:
            return False
    return True