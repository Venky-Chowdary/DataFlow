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
    name: str = "",
    search: str = "",
    source_type: str = "",
    destination_type: str = "",
    sync_mode: str = "",
) -> tuple[dict[str, Any] | None, str]:
    """``(output, "")`` for a ledger read, or ``(None, refusal)``.

    ``search``, ``source_type``, ``destination_type`` and ``sync_mode`` are the
    Jobs page's own filters. ``name`` is a source or destination name an
    operator said in words: it is applied as the Jobs page search only when a
    job in scope has that name, so an unknown word ("production") is refused
    rather than counted as a search that happens to match nothing.
    """
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
    narrowing, refusal = _narrowing(name, search, source_type, destination_type, sync_mode)
    if refusal:
        return None, refusal
    if narrowing.get("name"):
        needle = narrowing.pop("name")
        if not _has_named_job(needle, workspace_id):
            return None, (
                f"No job in this workspace has a source or destination named “{needle}”, "
                "so I can't count jobs for it."
            )
        narrowing["search"] = needle
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
    filters.update(narrowing)
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
            "narrowing": dict(narrowing),
        }
    if resolved or narrowing:
        in_window = counts.get("window") or {}
        output["window_counts"] = {
            "total": int(in_window.get("total") or 0),
            "status_counts": in_window.get("by_status") or {},
            "attention_count": int(in_window.get("attention") or 0),
        }
    return output, ""


def _narrowing(name: str, search: str, source_type: str, destination_type: str,
               sync_mode: str) -> tuple[dict[str, str], str]:
    from services.job_list_filters import SEARCH_MAX

    out: dict[str, str] = {}
    for key, value in (("name", name), ("search", search), ("source_type", source_type),
                       ("destination_type", destination_type), ("sync_mode", sync_mode)):
        if not isinstance(value, str):
            return {}, f"{key} must be a string."
        text = value.strip()
        if len(text) > SEARCH_MAX or any(ord(ch) < 32 for ch in text):
            return {}, f"{key} must be at most {SEARCH_MAX} printable characters."
        if text:
            out[key] = text
    if out.get("name") and out.get("search"):
        return {}, "Pass name or search, not both."
    return out, ""


def named_job_exists(name: str, scope: str = "workspace") -> bool:
    """Whether a job the caller may read has ``name`` as a source or destination.

    Read before answering, so a word that names nothing ("production") is
    refused by name instead of being counted as a search that matches nothing.
    An unreadable scope answers False: the question is then refused, never guessed.
    """
    try:
        workspace_id = job_list_workspace_scope(scope)
    except ValueError:
        return False
    if workspace_id == "" and workspace_scope_unresolved():
        return False
    try:
        return _has_named_job(name.strip(), workspace_id)
    except Exception:  # noqa: BLE001 - an unread ledger refuses the name rather than guessing
        return False


def _has_named_job(needle: str, workspace_id: str | None) -> bool:
    """Whether a job in scope has ``needle`` in its source or destination name."""
    jobs, _counts, _source = list_transfer_jobs(limit=100, workspace_id=workspace_id, search=needle)
    want = needle.casefold()
    return any(
        want in str(job.get(key) or "").casefold()
        for job in jobs
        for key in ("source", "destination", "source_table", "dest_table")
    )


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
                    "last_<N>_minutes / last_<N>_hours / last_<N>_days / last_<N>_weeks. "
                    "Anything else is refused."
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
            "search": {
                "type": "string",
                "description": "The Jobs page search: a substring of a job's id, names, types, status or error.",
            },
            "name": {
                "type": "string",
                "description": (
                    "A source or destination name. Refused unless a job in scope has it; "
                    "then applied as the Jobs page search."
                ),
            },
            "source_type": {"type": "string", "description": "Exact source connector type (e.g. postgresql)."},
            "destination_type": {"type": "string", "description": "Exact destination connector type."},
            "sync_mode": {"type": "string", "description": "Exact sync mode (e.g. cdc, full_refresh_append)."},
        },
        "required": [],
    },
}
