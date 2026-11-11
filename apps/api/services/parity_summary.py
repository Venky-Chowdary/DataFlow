"""Parity Studio: the workspace dashboard (trend, heatmap, KPIs).

Cheap by design: it reads the job store and what Parity Studio already stored
on each job (the compact summary written when a report is computed), falling
back to a report built from the stored evidence alone. It never re-reads a
customer database. Per-route drift comes from the route history ring buffer in
:mod:`services.data_quality_history` (median + MAD), where each computed report
also leaves its outcome on that job's load record.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any

from services.parity_state import job_store, now_iso, state_of

logger = logging.getLogger(__name__)

_SMALL_ROWS = 10
_SMALL_RATE = 0.01


def _route(job: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]] | None:
    req = job.get("transfer_request") if isinstance(job.get("transfer_request"), dict) else {}
    src, dst = req.get("source"), req.get("destination")
    return (src, dst) if isinstance(src, dict) and isinstance(dst, dict) and src and dst else None


def remember_report(job_id: str, job: dict[str, Any], report: dict[str, Any]) -> None:
    """Store the compact summary and keys a re-read proved equal; no version bump.

    These are caches of what the evidence showed, not decisions, so they do not
    change the report version. Written only when something changed.
    """
    from services.parity_report import compact_summary

    state = state_of(job)
    summary = compact_summary(report)
    changed = {k: v for k, v in (state.get("summary") or {}).items() if k != "at"} != {
        k: v for k, v in summary.items() if k != "at"
    }
    for ev in report.get("_tables_ev") or []:
        if ev.resolved_now:
            merged = sorted(set(state["resolved"].get(ev.name) or []) | set(ev.resolved_now))
            if merged != sorted(state["resolved"].get(ev.name) or []):
                state["resolved"][ev.name] = merged
                changed = True
    if not changed:
        return
    state["summary"] = summary
    try:
        job_store().update_job_fields(job_id, {"parity": state}, touch_updated_at=False)
    except Exception as exc:  # noqa: BLE001 - a cache write must not fail the read
        logger.info("parity summary not stored (%s)", type(exc).__name__)
    route = _route(job)
    if route:
        try:
            from services.data_quality_history import record_run_parity

            record_run_parity(route[0], route[1], job_id=job_id, parity={
                "verdict": summary["verdict"], "unexplained": summary["unexplained"],
                "proven_equal_pct": summary["proven_equal_pct"], "at": summary["at"],
            })
        except Exception as exc:  # noqa: BLE001 - history is advisory
            logger.info("parity route history not updated (%s)", type(exc).__name__)


def _cell(table: dict[str, Any]) -> str:
    status = table.get("status")
    if status == "match":
        return "proven"
    if status == "explained":
        return "explained"
    if status == "unexplained":
        bad = int(table.get("unexplained") or 0)
        rows = table.get("rows_source") or 0
        small = bad <= _SMALL_ROWS and (not rows or bad / rows <= _SMALL_RATE)
        return "unexplained_small" if small else "unexplained_large"
    return "none"


def _job_summary(job: dict[str, Any], job_id: str) -> dict[str, Any]:
    stored = state_of(job).get("summary")
    if isinstance(stored, dict) and stored.get("verdict"):
        return stored
    from services.parity_report import build_parity_report, compact_summary

    return compact_summary(build_parity_report(job, reread=False, job_id=job_id))


def build_summary(workspace_filter: Any, *, limit: int = 30) -> dict[str, Any]:
    """KPIs, the per-run trend (newest last) and the table x run heatmap."""
    from services.job_status import job_timestamp_iso

    store = job_store()
    listed = store.list_jobs(limit=limit, workspace_filter=workspace_filter)
    rows: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for brief in listed:
        job_id = str(brief.get("_id") or brief.get("id") or brief.get("job_id") or "")
        job = store.get_job(job_id) if job_id else None
        if not job:
            continue
        rows.append((brief, job, _job_summary(job, job_id)))
    rows.reverse()  # the store lists newest first; the trend reads oldest to newest
    runs = []
    for brief, job, summary in rows:
        job_id = str(brief.get("_id") or brief.get("id") or brief.get("job_id") or "")
        drift = None
        route = _route(job)
        if route:
            try:
                from services.data_quality_history import parity_is_drifting

                drift = parity_is_drifting(route[0], route[1], job_id=job_id)
            except Exception as exc:  # noqa: BLE001 - history is advisory
                logger.info("parity drift unavailable (%s)", type(exc).__name__)
        runs.append({
            "job_id": job_id,
            # ISO-8601 UTC: Mongo hands job timestamps back naive (QA MX3-19).
            "at": job_timestamp_iso(job.get("completed_at") or job.get("created_at") or brief.get("created_at")),
            "name": str(job.get("name") or brief.get("name") or job_id),
            "proven_equal_pct": summary.get("proven_equal_pct"),
            "unexplained": int(summary.get("unexplained") or 0),
            "verdict": summary.get("verdict") or "unverifiable",
            "drift": drift,
        })
    tables = sorted({t["name"] for _b, _j, s in rows for t in s.get("tables") or []})
    by_job = {r["job_id"]: {t["name"]: t for t in s.get("tables") or []} for r, (_b, _j, s) in zip(runs, rows)}
    cells = [[_cell(by_job[r["job_id"]].get(name) or {}) for r in runs] for name in tables]
    drifting = Counter(
        name for name, row in zip(tables, cells) for c in row if c.startswith("unexplained")
    )
    decided = [r for r in runs if r["verdict"] != "unverifiable"]
    return {
        "generated_at": now_iso(),
        "kpis": {
            "runs": len(runs),
            "fully_proven_pct": (
                round(100.0 * sum(r["verdict"] in {"proven", "explained"} for r in decided) / len(decided), 2)
                if decided else None
            ),
            "runs_with_unexplained": sum(1 for r in runs if r["unexplained"] > 0),
            "signoffs": sum(1 for _b, _j, s in rows if s.get("signed")),
            "most_drifting_table": drifting.most_common(1)[0][0] if drifting else None,
        },
        "runs": runs,
        "heatmap": {"tables": tables, "job_ids": [r["job_id"] for r in runs], "cells": cells},
    }
