"""Live workspace briefing — facts only, then the composer narrates them.

A ChatGPT-style copilot that cannot say “here is what needs you today”
is just a FAQ. This collector reads the same stores Jobs / Schedules /
Connectors already use. Counts come from those lists. Nothing is guessed.
"""

from __future__ import annotations

from typing import Any


def _safe_list(fn, default: list | None = None) -> list:
    try:
        rows = fn()
    except Exception:
        return list(default or [])
    return list(rows or [])


def collect_workspace_briefing(*, workspace_id: str = "") -> dict[str, Any]:
    """Read connectors, jobs, schedules, contracts. Never invent a green."""
    connectors = _load_connectors(workspace_id)
    loaded_jobs = _load_jobs(workspace_id)
    if isinstance(loaded_jobs, tuple):
        jobs, job_counts = loaded_jobs
    else:
        jobs = list(loaded_jobs or [])
        by_loaded: dict[str, int] = {}
        for j in jobs:
            key = str(j.get("status") or "unknown")
            by_loaded[key] = by_loaded.get(key, 0) + 1
        job_counts = {"total": len(jobs), "by_status": by_loaded}
    schedules = _load_schedules(workspace_id)
    contracts = _load_contracts(workspace_id)

    failed_connectors = [
        c for c in connectors if c.get("last_test_ok") is False
    ]
    untested_connectors = [
        c for c in connectors if c.get("last_test_ok") not in (True, False)
    ]
    passed_connectors = [c for c in connectors if c.get("last_test_ok") is True]

    failed_jobs = [j for j in jobs if str(j.get("status") or "").lower() in {"failed", "error"}]
    running_jobs = [
        j for j in jobs if str(j.get("status") or "").lower() in {"running", "pending"}
    ]
    ok_jobs = [
        j
        for j in jobs
        if str(j.get("status") or "").lower() in {"completed", "success", "succeeded"}
    ]

    enabled = [s for s in schedules if s.get("enabled")]
    parked = [s for s in schedules if s.get("needs_approval") or s.get("approval_finding")]
    next_runs = [s for s in enabled if s.get("next_run_at")]

    unsigned = [
        c
        for c in contracts
        if str(c.get("status") or "").upper() not in {"SIGNED", "ACTIVE"}
    ]

    job_total = int(job_counts.get("total") or 0) or len(jobs)
    by_status = job_counts.get("by_status") or {}
    jobs_failed_n = int(by_status.get("failed") or 0) if int(job_counts.get("total") or 0) else len(failed_jobs)
    jobs_running_n = (
        int(by_status.get("running") or 0) + int(by_status.get("pending") or 0)
        if int(job_counts.get("total") or 0)
        else len(running_jobs)
    )
    jobs_ok_n = (
        int(by_status.get("completed") or 0)
        + int(by_status.get("completed_with_quarantine") or 0)
    ) or len(ok_jobs)

    attention: list[str] = []
    if jobs_failed_n:
        attention.append(f"{jobs_failed_n} failed transfer job(s)")
    if parked:
        attention.append(f"{len(parked)} pipeline(s) waiting on approval")
    if failed_connectors:
        attention.append(f"{len(failed_connectors)} connector(s) failed their last test")
    if unsigned:
        attention.append(f"{len(unsigned)} contract(s) not signed")

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
        "latest_failed_job": _job_line(failed_jobs[0]) if failed_jobs else "",
        "schedule_count": len(schedules),
        "schedules_enabled": len(enabled),
        "schedules_parked": len(parked),
        "next_schedule": _schedule_line(next_runs[0]) if next_runs else "",
        "parked_names": [str(s.get("name") or "") for s in parked[:6] if s.get("name")],
        "contract_count": len(contracts),
        "contracts_unsigned": len(unsigned),
        "attention": attention,
        "empty_workspace": not connectors and job_total == 0 and not schedules,
    }


def _job_line(job: dict[str, Any]) -> str:
    jid = str(job.get("id") or job.get("job_id") or "")[:12]
    src = job.get("source") or (job.get("route") or {}).get("source_table") or "?"
    dst = job.get("destination") or (job.get("route") or {}).get("dest_table") or "?"
    return f"`{jid}` {src} → {dst}"


def _schedule_line(sched: dict[str, Any]) -> str:
    name = sched.get("name") or "pipeline"
    nxt = sched.get("next_run_at") or "—"
    return f"**{name}** next `{nxt}`"


def _load_connectors(workspace_id: str) -> list[dict[str, Any]]:
    try:
        from services import connector_store

        rows = connector_store.list_connectors(workspace_id=workspace_id)
        out = []
        for c in rows or []:
            if hasattr(c, "to_dict"):
                d = c.to_dict()
            elif isinstance(c, dict):
                d = c
            else:
                d = {
                    "name": getattr(c, "name", ""),
                    "last_test_ok": getattr(c, "last_test_ok", None),
                }
            out.append(d)
        return out
    except Exception:
        return []


def _load_jobs(workspace_id: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    empty_counts: dict[str, Any] = {"total": 0, "by_status": {}}
    try:
        from .job_reads import list_transfer_jobs

        # Keep "" as this-workspace/legacy scope — never coerce to None (all workspaces).
        rows, counts, _source = list_transfer_jobs(
            limit=25, workspace_id=workspace_id
        )
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
    except Exception:
        return [], empty_counts


def _load_schedules(workspace_id: str) -> list[dict[str, Any]]:
    try:
        from services.schedule_store import list_schedules

        rows = list_schedules()
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
    except Exception:
        return []


def _load_contracts(workspace_id: str) -> list[dict[str, Any]]:
    try:
        from services.contract_store import get_contract_store

        store = get_contract_store()
        rows = store.list_contracts(limit=50) if hasattr(store, "list_contracts") else []
        out = []
        for c in rows or []:
            d = c.to_dict() if hasattr(c, "to_dict") else {}
            meta = getattr(c, "metadata", None) or d.get("metadata") or {}
            cws = str(meta.get("workspace_id") or "")
            if workspace_id and cws and cws != workspace_id:
                continue
            out.append(
                {
                    "id": d.get("id") or getattr(c, "id", ""),
                    "name": d.get("name") or getattr(c, "name", ""),
                    "status": str(
                        getattr(getattr(c, "status", None), "value", None)
                        or d.get("status")
                        or ""
                    ),
                }
            )
        return out
    except Exception:
        return []
