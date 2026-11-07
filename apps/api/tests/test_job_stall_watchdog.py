"""D10: per-step stall watchdog — a live-but-stuck job is flagged, then failed.

The MySQL→Snowflake jobs that sat at "Connecting to destination and creating
table…" for 10+ minutes were not orphans — the worker thread held its lease,
so the orphan sweep skipped them and ``stalled`` stayed ``false`` forever.
The watchdog measures the *active step*: quiet past the step's threshold
(connect/DDL default 120 s) → ``stalled=True`` + ``stall_seconds``; past the
hard limit (3× threshold) → ``failed`` with typed finding ``step_stalled``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services import job_watchdog
from services.mongodb_service import MemoryMongoDBService

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)


def _job(phase: str, started_secs_ago: float, updated_secs_ago: float | None = None):
    return {
        "status": "running",
        "phase": phase,
        "updated_at": (
            NOW - timedelta(seconds=updated_secs_ago)
        ).isoformat() if updated_secs_ago is not None else (
            NOW - timedelta(seconds=started_secs_ago)
        ).isoformat(),
        "phases": [
            {"name": "intake", "status": "done"},
            {
                "name": phase,
                "status": "active",
                "started_at": (NOW - timedelta(seconds=started_secs_ago)).isoformat(),
                "message": "Connecting to destination and creating table…",
            },
        ],
    }


def _store(*jobs):
    store = MemoryMongoDBService()
    for job in jobs:
        store.create_transfer_job(job)
    return store


def test_quiet_connect_step_flags_stalled():
    store = _store(_job("connect", started_secs_ago=200))
    out = job_watchdog.sweep_stalled_jobs(mongo=store, now=NOW)
    assert out and out[0]["action"] == "flagged"
    doc = store.list_unfinished_jobs()[0]
    assert doc["stalled"] is True
    assert doc["stall_step"] == "connect"
    assert doc["stall_seconds"] >= 200


def test_stuck_step_hard_fails_with_typed_finding():
    # connect threshold 120 s → hard fail at 360 s
    store = _store(_job("connect", started_secs_ago=400))
    out = job_watchdog.sweep_stalled_jobs(mongo=store, now=NOW)
    assert out and out[0]["action"] == "failed"
    assert out[0]["error_code"] == "step_stalled"
    assert store.list_unfinished_jobs() == []
    # the settled doc carries the typed finding
    found = store.list_jobs()
    doc = next(j for j in found if j.get("status") == "failed")
    assert doc.get("error_code") == "step_stalled"
    assert "connect" in str(doc.get("error") or "")
    assert doc.get("stalled") is True


def test_progress_moving_is_not_a_stall():
    """A long step that is still writing progress must not flag."""
    store = _store(_job("load", started_secs_ago=2000, updated_secs_ago=5))
    out = job_watchdog.sweep_stalled_jobs(mongo=store, now=NOW)
    assert out == []
    doc = store.list_unfinished_jobs()[0]
    assert not doc.get("stalled")


def test_fresh_connect_step_does_not_flag():
    store = _store(_job("connect", started_secs_ago=30))
    out = job_watchdog.sweep_stalled_jobs(mongo=store, now=NOW)
    assert out == []


def test_terminal_jobs_never_flag():
    job = _job("connect", started_secs_ago=5000)
    job["status"] = "completed"
    store = _store(job)
    assert job_watchdog.sweep_stalled_jobs(mongo=store, now=NOW) == []


def test_flag_then_fail_across_sweeps():
    """Flag at threshold; a later sweep past the hard limit fails it."""
    store = _store(_job("connect", started_secs_ago=200))
    job_watchdog.sweep_stalled_jobs(mongo=store, now=NOW)
    later = NOW + timedelta(seconds=200)  # total quiet = 400 s > 360 s
    out = job_watchdog.sweep_stalled_jobs(mongo=store, now=later)
    assert out[0]["action"] == "failed"


def test_per_step_threshold_override(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STEP_STALL_CONNECT_SEC", "60")
    store = _store(_job("connect", started_secs_ago=90))
    out = job_watchdog.sweep_stalled_jobs(mongo=store, now=NOW)
    assert out and out[0]["action"] == "flagged"
