"""Live workspace briefing — facts only, then the composer narrates them.

A ChatGPT-style copilot that cannot say “here is what needs you today”
is just a FAQ. This collector reads the same stores Jobs / Schedules /
Connectors already use. Counts come from those lists. Nothing is guessed.
"""

from __future__ import annotations

from typing import Any


#: Briefing section -> the store it is read from. A section whose store raised
#: is listed in ``facts["unreadable"]`` and reported as unread, never as zero.
SOURCES = ("connectors", "jobs", "pipelines", "contracts")


class _Unreadable(RuntimeError):
    """A store answered, but not with the durable data (a degraded job read)."""


def _read(source: str, fn, default: Any, unreadable: list[str]) -> Any:
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - reported to the operator as an unread section
        import logging

        logging.getLogger(__name__).warning(
            "workspace briefing: %s could not be read (%s)", source, type(exc).__name__
        )
        unreadable.append(source)
        return default


def collect_workspace_briefing(*, workspace_id: str = "") -> dict[str, Any]:
    """Read connectors, jobs, schedules, contracts. Never invent a green."""
    from datetime import datetime, timezone

    from services.job_list_filters import RUNNING_STATUSES, status_values

    unreadable: list[str] = []
    connectors = _read("connectors", lambda: _load_connectors(workspace_id), [], unreadable)
    loaded_jobs = _read("jobs", lambda: _load_jobs(workspace_id), ([], {"total": 0, "by_status": {}}), unreadable)
    if isinstance(loaded_jobs, tuple):
        jobs, job_counts = loaded_jobs
    else:
        jobs = list(loaded_jobs or [])
        by_loaded: dict[str, int] = {}
        for j in jobs:
            key = str(j.get("status") or "unknown")
            by_loaded[key] = by_loaded.get(key, 0) + 1
        job_counts = {"total": len(jobs), "by_status": by_loaded}
    schedules = _read("pipelines", lambda: _load_schedules(workspace_id), [], unreadable)
    contract_facts = _read("contracts", lambda: _contract_facts(workspace_id), {}, unreadable)

    from services.connector_store import connector_health

    health = [(c, connector_health(c)) for c in connectors]
    failed_connectors = [c for c, h in health if h == "failed"]
    untested_connectors = [c for c, h in health if h == "untested"]
    passed_connectors = [c for c, h in health if h == "passed"]

    # One definition of each class for the Jobs page, list_jobs and this
    # briefing (services.job_list_filters): failed is the ``failed`` status,
    # in flight is running/pending/queued/retrying, completed is every
    # completed status including "with warnings" (audit H2).
    completed_statuses = set(status_values("completed") or ())
    failed_jobs = [j for j in jobs if str(j.get("status") or "") == "failed"]

    enabled = [s for s in schedules if s.get("enabled")]
    parked = [s for s in schedules if s.get("needs_approval") or s.get("approval_finding")]
    next_runs = _future_schedules(enabled)

    contract_total = int(contract_facts.get("contract_count") or 0)
    contracts_unsigned_n = int(contract_facts.get("contracts_unsigned") or 0)

    job_total = int(job_counts.get("total") or 0) or len(jobs)
    by_status = job_counts.get("by_status") or {}
    jobs_failed_n = int(by_status.get("failed") or 0)
    jobs_running_n = sum(int(by_status.get(s) or 0) for s in RUNNING_STATUSES)
    jobs_ok_n = sum(int(n or 0) for s, n in by_status.items() if s in completed_statuses)
    # "Needs attention" on the Jobs page: failed, or Gate-8 verification failed.
    jobs_attention_n = int(job_counts.get("attention") or 0) or jobs_failed_n

    attention: list[str] = []
    if jobs_failed_n:
        attention.append(f"{jobs_failed_n} failed transfer job(s)")
    if jobs_attention_n > jobs_failed_n:
        attention.append(
            f"{jobs_attention_n - jobs_failed_n} job(s) completed but failed verification"
        )
    if parked:
        attention.append(f"{len(parked)} pipeline(s) waiting on approval")
    if failed_connectors:
        attention.append(f"{len(failed_connectors)} connector(s) failed their last test")
    if contracts_unsigned_n:
        attention.append(f"{contracts_unsigned_n} contract(s) not signed")
    if not next_runs and any(s.get("next_run_at") for s in enabled):
        attention.append(
            "Enabled pipelines have a next run in the past — open Schedules to refresh the cadence."
        )

    return {
        "connector_count": len(connectors),
        "connectors_passed": len(passed_connectors),
        "connectors_failed": len(failed_connectors),
        "connectors_untested": len(untested_connectors),
        "connector_names": [str(c.get("name") or "") for c in connectors[:8] if c.get("name")],
        "failed_connector_names": [
            str(c.get("name") or "") for c in failed_connectors[:6] if c.get("name")
        ],
        "job_count": job_total,
        "jobs_ok": jobs_ok_n,
        "jobs_failed": jobs_failed_n,
        "jobs_running": jobs_running_n,
        "jobs_attention": jobs_attention_n,
        "latest_failed_job": _job_line(failed_jobs[0]) if failed_jobs else "",
        "schedule_count": len(schedules),
        "schedules_enabled": len(enabled),
        "schedules_parked": len(parked),
        "next_schedule": _schedule_line(next_runs[0]) if next_runs else "",
        "parked_names": [str(s.get("name") or "") for s in parked[:6] if s.get("name")],
        "contract_count": contract_total,
        "contracts_unsigned": contracts_unsigned_n,
        "contracts_by_status": dict(contract_facts.get("contracts_by_status") or {}),
        "recent_contracts": list(contract_facts.get("recent_contracts") or []),
        "attention": attention,
        # Sections whose store could not be read this turn, and when it was read.
        "unreadable": unreadable,
        "as_of": datetime.now(timezone.utc).isoformat(),
        # Empty is a statement about the workspace; it needs every store to have answered.
        "empty_workspace": (
            not unreadable
            and not connectors and job_total == 0 and not schedules and not contract_total
        ),
    }


def _future_schedules(enabled: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Soonest next_run_at that is still in the future. A past stamp is not next."""
    from datetime import datetime, timezone

    from services.schedule_store import has_open_approval

    now = datetime.now(timezone.utc)
    future: list[tuple[datetime, dict[str, Any]]] = []
    for sched in enabled:
        if has_open_approval(sched):
            # Parked pipelines are held out of the cadence — not a next run.
            continue
        raw = sched.get("next_run_at")
        if not raw:
            continue
        text = str(raw).strip().replace("Z", "+00:00")
        try:
            when = datetime.fromisoformat(text)
        except ValueError:
            continue
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        if when > now:
            future.append((when, sched))
    future.sort(key=lambda pair: pair[0])
    return [sched for _, sched in future]


def _job_line(job: dict[str, Any]) -> str:
    jid = str(job.get("id") or job.get("job_id") or "")[:12]
    src = job.get("source") or (job.get("route") or {}).get("source_table") or "?"
    dst = job.get("destination") or (job.get("route") or {}).get("dest_table") or "?"
    return f"`{jid}` {src} → {dst}"


def _schedule_line(sched: dict[str, Any]) -> str:
    name = sched.get("name") or "pipeline"
    nxt = sched.get("next_run_at") or "—"
    return f"**{name}** next `{nxt}`"


# The loaders below raise on failure on purpose: ``_read`` owns the fallback and
# the record that a store was unreachable. Swallowing the error here handed back
# an empty list, which the briefing then stated as "0 jobs" (audit C3).
def _load_connectors(workspace_id: str) -> list[dict[str, Any]]:
    from services import connector_store
    from services.workspace_filter import current_workspace_filter

    rows = connector_store.list_connectors(workspace_id=workspace_id)
    visible = current_workspace_filter()
    out = []
    for c in rows or []:
        if visible is not None and not visible.matches(getattr(c, "workspace_id", "")):
            continue
        if hasattr(c, "to_dict"):
            d = c.to_dict()
        elif isinstance(c, dict):
            d = c
        else:
            d = {
                "name": getattr(c, "name", ""),
                "last_test_ok": getattr(c, "last_test_ok", None),
                "last_tested_at": getattr(c, "last_tested_at", None),
                "last_transfer_ok_at": getattr(c, "last_transfer_ok_at", None),
            }
        out.append(d)
    return out


def _load_jobs(workspace_id: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from .job_reads import list_transfer_jobs

    # Keep "" as this-workspace/legacy scope — never coerce to None (all workspaces).
    rows, counts, _source = list_transfer_jobs(limit=25, workspace_id=workspace_id)
    if (counts or {}).get("degraded"):
        raise _Unreadable("job ledger unreachable")
    out = []
    for j in rows or []:
        if not isinstance(j, dict):
            continue
        out.append(
            {
                "id": str(j.get("id") or ""),
                "source": j.get("source") or "",
                "destination": j.get("destination") or "",
                "status": j.get("status"),
                "route": {
                    "source_table": j.get("source_table") or j.get("source"),
                    "dest_table": j.get("dest_table") or j.get("destination"),
                },
            }
        )
    if not int((counts or {}).get("total") or 0):
        by_status: dict[str, int] = {}
        for j in out:
            key = str(j.get("status") or "unknown")
            by_status[key] = by_status.get(key, 0) + 1
        counts = {"total": len(out), "by_status": by_status}
    return out, counts


def _load_schedules(workspace_id: str) -> list[dict[str, Any]]:
    from services.schedule_store import list_schedules
    from services.workspace_filter import current_workspace_filter

    rows = list_schedules(current_workspace_filter())
    out = []
    for s in rows or []:
        d = s.to_dict() if hasattr(s, "to_dict") else dict(s)
        if workspace_id and d.get("workspace_id") and d.get("workspace_id") != workspace_id:
            continue
        req = d.get("approval_request") or {}
        d["needs_approval"] = str(req.get("status") or "").lower() == "open"
        d["approval_finding"] = req.get("finding") or ""
        out.append(d)
    return out


def _contract_facts(workspace_id: str) -> dict[str, Any]:
    """Contract status from the one read model the Contracts page uses.

    A 50-row page used to stand in for the population inside a workspace, so
    the newest signed rows hid an older Draft and the briefing said "all
    signed" while the page said Draft. Raises when the census is unavailable.
    """
    from services.contract_read_model import briefing_summary

    return briefing_summary(workspace_id=workspace_id)
