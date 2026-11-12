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
# "Needs attention" on the Jobs page: a failed run, or a run whose Gate-8
# verification recorded ``passed: false`` (its differences were not explained).
ATTENTION_STATUS = "attention"
VERIFICATION_FILTERS = ("failed", "passed", "none")


def status_values(status: str | None) -> tuple[str, ...] | None:
    value = str(status or "all").strip().lower()
    if value == "running":
        return RUNNING_STATUSES
    if value == "completed":
        return tuple(sorted(COMPLETED_STATUSES | {"success", "succeeded"}))
    if value == "failed":
        return ("failed",)
    return None


# One definition of a recorded Gate-8 verdict for the list, the counts and both
# job stores: ``reconciliation`` is an object whose ``passed`` is a boolean.
# Anything else (missing, null, a list, a non-bool flag) is "no verdict". The
# Mongo forms are aggregation expressions, so an array-shaped field is never
# matched element-wise the way a plain ``{"reconciliation.passed": ...}``
# query would.
_RECON_IS_OBJECT = {"$eq": [{"$type": "$reconciliation"}, "object"]}
_VERDICT_IS_BOOL = {"$eq": [{"$type": "$reconciliation.passed"}, "bool"]}


def verdict_expr(verdict: str) -> dict[str, Any]:
    """Mongo ``$expr`` for a verification facet: failed, passed or none."""
    if verdict == "failed":
        return {"$and": [_RECON_IS_OBJECT, {"$eq": ["$reconciliation.passed", False]}]}
    if verdict == "passed":
        return {"$and": [_RECON_IS_OBJECT, {"$eq": ["$reconciliation.passed", True]}]}
    return {"$not": [{"$and": [_RECON_IS_OBJECT, _VERDICT_IS_BOOL]}]}


def attention_expr() -> dict[str, Any]:
    """Mongo ``$expr`` for "Needs attention": status exactly ``failed``, or a failed verdict."""
    return {"$or": [{"$eq": ["$status", "failed"]}, verdict_expr("failed")]}


def job_verdict(job: dict[str, Any]) -> bool | None:
    """The recorded Gate-8 verdict, or None when the run recorded none (same rule as ``verdict_expr``)."""
    recon = job.get("reconciliation")
    if not isinstance(recon, dict):
        return None
    passed = recon.get("passed")
    return passed if isinstance(passed, bool) else None


def job_needs_attention(job: dict[str, Any]) -> bool:
    """Failed, or its Gate-8 verification failed — the Jobs page "Needs attention" pivot."""
    return job.get("status") == "failed" or job_verdict(job) is False


def _exact(value: str | None) -> str:
    return str(value or "").strip()[:SEARCH_MAX]


def _verification_clause(value: str | None) -> dict[str, Any] | None:
    verdict = _exact(value).lower()
    if verdict in VERIFICATION_FILTERS:
        return {"$expr": verdict_expr(verdict)}
    return None


def _day_start(value: date | None) -> datetime | None:
    if value is None:
        return None
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def created_bounds(
    date_from: date | datetime | None, date_to: date | datetime | None
) -> tuple[datetime | None, datetime | None]:
    """``created_at`` window as ``[lower, upper)`` UTC instants.

    A plain ``date`` is a whole UTC calendar day, inclusive at both ends (the
    Jobs grid). A ``datetime`` is an exact instant — inclusive lower, exclusive
    upper — so a caller whose "today" is not the UTC day (a workspace time
    zone) can ask for precisely its own day.
    """
    if isinstance(date_from, datetime):
        lower = _as_utc(date_from)
    else:
        lower = _day_start(date_from)
    if isinstance(date_to, datetime):
        upper = _as_utc(date_to)
    else:
        upper = _day_start(date_to + timedelta(days=1) if date_to else None)
    return lower, upper


def mongo_job_list_query(
    base: dict[str, Any] | None = None,
    *,
    status: str | None = None,
    search: str = "",
    date_from: date | datetime | None = None,
    date_to: date | datetime | None = None,
    source_type: str | None = None,
    destination_type: str | None = None,
    sync_mode: str | None = None,
    verification: str | None = None,
) -> dict[str, Any]:
    clauses: list[dict[str, Any]] = []
    if base:
        clauses.append(base)

    statuses = status_values(status)
    if statuses:
        clauses.append({"status": {"$in": list(statuses)}})
    elif str(status or "").strip().lower() == ATTENTION_STATUS:
        clauses.append({"$expr": attention_expr()})

    for field, value in (
        ("source_type", source_type),
        ("destination_type", destination_type),
        ("sync_mode", sync_mode),
    ):
        if _exact(value):
            clauses.append({field: _exact(value)})
    verdict = _verification_clause(verification)
    if verdict:
        clauses.append(verdict)

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
    lower, upper = created_bounds(date_from, date_to)
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
    date_from: date | datetime | None = None,
    date_to: date | datetime | None = None,
    source_type: str | None = None,
    destination_type: str | None = None,
    sync_mode: str | None = None,
    verification: str | None = None,
) -> bool:
    statuses = status_values(status)
    # Exact, as the Mongo ``$in`` is: a stored "FAILED" is not the failed status.
    job_status = str(job.get("status") or "unknown")
    if statuses and job_status not in statuses:
        return False
    if str(status or "").strip().lower() == ATTENTION_STATUS and not job_needs_attention(job):
        return False
    for field, value in (
        ("source_type", source_type),
        ("destination_type", destination_type),
        ("sync_mode", sync_mode),
    ):
        if _exact(value) and job.get(field) != _exact(value):
            return False
    verdict = _exact(verification).lower()
    if verdict in VERIFICATION_FILTERS:
        expected = {"failed": False, "passed": True, "none": None}[verdict]
        if job_verdict(job) is not expected:
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
        lower, upper = created_bounds(date_from, date_to)
        if lower and created < lower:
            return False
        if upper and created >= upper:
            return False
    return True