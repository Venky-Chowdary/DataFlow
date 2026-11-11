"""The ``list_jobs`` read: one page of the job ledger plus the Jobs page's counts.

Every number an answer may state about jobs comes from here, and each one is
named for the Jobs page field it must equal (``GET /api/v1/jobs``): ``total``,
``status_counts``, ``attention_count`` and ``filtered_total``. Status and
created-window filters are the store's own (``services.job_list_filters``), so
a "failed today" answer is a count the store made, never a page Pilot filtered.

``degraded`` marks a read that did not reach the durable ledger, and ``as_of``
is the instant it was read; the composer must honour both.
"""

from __future__ import annotations

from typing import Any

from services.job_list_filters import ATTENTION_STATUS, VERIFICATION_FILTERS
from services.workspace_filter import UNASSIGNED_WORKSPACE

from . import job_window
from .job_reads import job_list_workspace_scope, list_transfer_jobs, workspace_scope_unresolved

#: The Jobs page's status pivots. Anything else is refused, not ignored.
STATUS_FILTERS = ("all", "running", "completed", "failed", ATTENTION_STATUS)

UNRESOLVED_SCOPE_ERROR = (
    "This key can read every workspace and is not acting in one. "
    "Send X-Workspace-Id to list one workspace's jobs, or pass "
    "scope=all for every workspace."
)


def read_job_ledger(
    limit: int = 10,
    scope: str = "workspace",
    status: str = "",
    window: str = "",
    date_from: str = "",
    date_to: str = "",
    verification: str = "",
) -> tuple[dict[str, Any] | None, str]:
    """``(output, "")`` for a ledger read, or ``(None, refusal)``."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > 100:
        return None, "limit must be an integer from 1 to 100."
    try:
        workspace_id = job_list_workspace_scope(scope)
    except ValueError as exc:
        return None, str(exc)
    if workspace_id == "" and workspace_scope_unresolved():
        return None, UNRESOLVED_SCOPE_ERROR
    status_filter = str(status or "all").strip().lower() or "all"
    if status_filter not in STATUS_FILTERS:
        return None, "status must be one of: " + ", ".join(STATUS_FILTERS) + "."
    verdict = str(verification or "").strip().lower()
    if verdict and verdict not in VERIFICATION_FILTERS:
        return None, "verification must be one of: " + ", ".join(VERIFICATION_FILTERS) + "."
    as_of = job_window.utc_now()
    resolved, refusal = job_window.resolve_window(window, date_from, date_to, now=as_of)
    if refusal:
        return None, refusal

    filters: dict[str, Any] = {}
    if status_filter != "all":
        filters["status"] = status_filter
    if verdict:
        filters["verification"] = verdict
    if resolved:
        # Exact instants: the store filters created_at in [start, end).
        filters.update(date_from=resolved.start, date_to=resolved.end)
    # Filters ride along only when set, so a plain read keeps the plain call.
    jobs, counts, source = list_transfer_jobs(limit=limit, workspace_id=workspace_id, **filters)

    total = int(counts.get("total") or 0)
    filtered_total = int(counts.get("filtered_total") or 0) if filters else total
    output: dict[str, Any] = {
        "jobs": jobs,
        "count": len(jobs),
        # Whole history for this scope — the Jobs page ``total``.
        "total": total,
        "status_counts": counts.get("by_status") or {},
        "attention_count": int(counts.get("attention") or 0),
        "filtered_total": max(filtered_total, len(jobs)) if filters else total,
        "truncated": (filtered_total if filters else total) > len(jobs),
        "degraded": bool(counts.get("degraded")),
        "as_of": as_of.isoformat(),
        "store": source,
        "scope": (
            "all" if workspace_id is None
            else "unassigned" if workspace_id == UNASSIGNED_WORKSPACE
            else "workspace"
        ),
    }
    if output["degraded"]:
        output["degraded_reason"] = str(counts.get("degraded_reason") or "job_store_unreachable")
        # Nothing here is the workspace's history: no count may be stated from it.
        output["unread"] = True
    if filters:
        output["filter"] = {
            "status": status_filter,
            "verification": verdict,
            "window": resolved.to_dict() if resolved else None,
        }
    if resolved:
        in_window = counts.get("window") or {}
        output["window_counts"] = {
            "total": int(in_window.get("total") or 0),
            "status_counts": in_window.get("by_status") or {},
            "attention_count": int(in_window.get("attention") or 0),
        }
    return output, ""


#: The tool definition advertised to MCP clients and the model.
LIST_JOBS_TOOL: dict[str, Any] = {
    "name": "list_jobs",
    "description": (
        "List transfer jobs with status, IDs, and record counts, newest first. "
        "``total``, ``status_counts`` and ``attention_count`` are the whole history "
        "for ``scope`` (default ``workspace``, the same population as brief_workspace "
        "and the Jobs page). Filter server-side with ``status`` and a time ``window`` "
        "(or ``date_from``/``date_to``); ``filtered_total`` then counts the matches and "
        "``window_counts`` the jobs created in the window. ``truncated`` means ``jobs`` "
        "is a page, not the population; ``degraded`` means the job ledger could not be "
        "read and no count may be stated. Pass ``scope=all`` only when you explicitly "
        "need every workspace."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "limit": {
                "type": "integer",
                "description": "Max jobs to return. Integer from 1 to 100. Negative and unbounded limits are refused.",
                "default": 10,
                "minimum": 1,
                "maximum": 100,
            },
            "scope": {
                "type": "string",
                "enum": ["workspace", "all", "unassigned"],
                "default": "workspace",
                "description": (
                    "workspace = this workspace; all = every workspace; "
                    "unassigned = legacy jobs with no workspace (platform admins)"
                ),
            },
            "status": {
                "type": "string",
                "enum": ["all", "running", "completed", "failed", "attention"],
                "default": "all",
                "description": (
                    "The Jobs page pivots: running = running/pending/queued/retrying; "
                    "completed = every completed class; failed = failed; attention = "
                    "failed or Gate-8 verification failed."
                ),
            },
            "window": {
                "type": "string",
                "description": (
                    "Created-at window in the workspace time zone: today, yesterday, "
                    "this_week, last_week, this_month, last_month, since_yesterday, or "
                    "last_<N>_hours / last_<N>_days / last_<N>_weeks. Anything else is refused."
                ),
            },
            "date_from": {
                "type": "string",
                "description": "ISO date (whole workspace-zone day) or date-time; window start, inclusive.",
            },
            "date_to": {
                "type": "string",
                "description": "ISO date (whole workspace-zone day, inclusive) or date-time (exclusive).",
            },
            "verification": {
                "type": "string",
                "enum": ["", "failed", "passed", "none"],
                "description": "Gate-8 verification verdict recorded on the run.",
            },
        },
        "required": [],
    },
}
