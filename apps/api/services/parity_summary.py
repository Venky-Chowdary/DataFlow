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

from services.parity_state import job_store, now_iso, state_of, store_cache

logger = logging.getLogger(__name__)

_SMALL_ROWS = 10
_SMALL_RATE = 0.01


def _route(job: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]] | None:
    req = job.get("transfer_request") if isinstance(job.get("transfer_request"), dict) else {}
    src, dst = req.get("source"), req.get("destination")
    return (src, dst) if isinstance(src, dict) and isinstance(dst, dict) and src and dst else None


def remember_report(job_id: str, job: dict[str, Any], report: dict[str, Any]) -> None:
    """Cache the compact summary the dashboard reads; no version bump, no decision.

    Written as ``parity.summary`` alone and only while the state is still at
    the version this report was built from, so a read computed from an older
    snapshot can never overwrite a decision written since (compare-and-swap).
    A read persists nothing else: keys a re-read proved equal are recomputed
    by the next re-read, and only a fix's re-check round records them.
    """
    from services.parity_evidence import job_is_terminal
    from services.parity_report import compact_summary

    if not job_is_terminal(job):
        return  # a running job's report is "unverifiable" now and stale the moment it finishes
    state = state_of(job)
    summary = compact_summary(report, job)
    if {k: v for k, v in (state.get("summary") or {}).items() if k != "at"} == {
        k: v for k, v in summary.items() if k != "at"
    }:
        return
    try:
        if not store_cache(job_id, {"summary": summary}, version=int(report.get("version") or 0)):
            return  # a decision landed since this report was built; it wrote its own summary
    except Exception as exc:  # noqa: BLE001 - a cache write must not fail the read
        logger.info("parity summary not stored (%s)", type(exc).__name__)
        return
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
    if table.get("failed_verification"):
        return "failed"  # Gate-8 failed on rows it did not name: not "not checked"
    if status in {"match", "explained"} and table.get("check") == "sampled":
        # A sample bounds the rate of unexplained differences; only a full compare is proof.
        return "sampled"
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
    from services.parity_report import summary_stamp

    stored = state_of(job).get("summary")
    # A summary computed for another job state (stored while the job ran, before a later
    # update, or without its stamp) is recomputed: completion does not bump the version.
    if isinstance(stored, dict) and any(stored.get(k) != v for k, v in summary_stamp(job).items()):
        stored = None
    # A summary stored before it carried the check method (tables' check, the run's
    # method) cannot tell a sample from a full compare, nor (without
    # ``failed_verification``) a failed Gate-8 from an unchecked run: recompute it.
    if isinstance(stored, dict) and stored.get("verdict") and "method" in stored and all(
        "check" in t and "failed_verification" in t for t in stored.get("tables") or []
    ):
        return stored
    from services.parity_report import build_parity_report, compact_summary

    return compact_summary(build_parity_report(job, reread=False, job_id=job_id))


def _is_repair_job(job: dict[str, Any]) -> bool:
    req = job.get("transfer_request") if isinstance(job.get("transfer_request"), dict) else {}
    return bool(str(job.get("parent_job_id") or req.get("parent_job_id") or "").strip())


def _jobs_by_id(store: Any, ids: list[str]) -> dict[str, dict[str, Any]]:
    batch = getattr(store, "get_jobs", None)
    if callable(batch):
        return dict(batch(ids) or {})
    return {i: j for i in ids if (j := store.get_job(i))}


def build_summary(workspace_filter: Any, *, limit: int = 30) -> dict[str, Any]:
    """KPIs, the per-run trend (newest last) and the table x run heatmap."""
    from services.job_status import job_timestamp_iso

    store = job_store()
    # Repair jobs (a Parity Studio re-sync, a quarantine replay) are not runs of
    # their own: their outcome is the parent's re-check round. Read past them.
    listed = store.list_jobs(limit=min(2 * limit, 200), workspace_filter=workspace_filter)
    ids = [str(b.get("_id") or b.get("id") or b.get("job_id") or "") for b in listed]
    full = _jobs_by_id(store, [i for i in ids if i])  # one batch read, not one read per job
    rows: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for brief, job_id in zip(listed, ids):
        if len(rows) >= limit:
            break
        job = full.get(job_id) if job_id else None
        if not job or _is_repair_job(job):
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
            # How the run compared values: a sampled run has no exact figure to draw.
            "method": summary.get("method") or "none",
            "rows_checked": summary.get("rows_checked"),
            "rows_total": summary.get("rows_total"),
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
    # A run Gate-8 failed without naming the rows is decided as far as the KPI goes: it is not proven.
    failed = {job_id for job_id, tables in by_job.items() if any(t.get("failed_verification") for t in tables.values())}
    decided = [r for r in runs if r["verdict"] != "unverifiable" or r["job_id"] in failed]
    return {
        "generated_at": now_iso(),
        "kpis": {
            "runs": len(runs),
            "fully_proven_pct": (
                round(100.0 * sum(r["verdict"] in {"proven", "explained"} for r in decided) / len(decided), 2)
                if decided else None
            ),
            "runs_with_unexplained": sum(1 for r in runs if r["unexplained"] > 0),
            "runs_failed_verification": len(failed),
            "signoffs": sum(1 for _b, _j, s in rows if s.get("signed")),
            "most_drifting_table": drifting.most_common(1)[0][0] if drifting else None,
        },
        "runs": runs,
        "heatmap": {"tables": tables, "job_ids": [r["job_id"] for r in runs], "cells": cells},
    }
