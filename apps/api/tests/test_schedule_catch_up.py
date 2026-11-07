"""Operator-approved catch-up replay of missed cadence windows.

Missed windows were counted but never replayed. request_schedule_catch_up
queues a bounded, visible replay: each claim advances next_run_at by exactly
one boundary and fires one run per overdue slot, tagged catch_up on the run
record. Regression: before this change the store had no catch_up_windows_left
and mark_schedule_running always jumped to the current boundary.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _file_store(tmp_path, monkeypatch):
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    return store


def _new_schedule(store, **extra):
    payload = {
        "name": f"sched-{len(store.list_schedules())}",
        "source_connector_id": "a",
        "source_table": "s",
        "dest_connector_id": "b",
        "dest_table": "d",
        "interval": "hourly",
        "mappings": [{"source": "id", "target": "id"}],
    }
    payload.update(extra)
    return store.create_schedule(payload)


def _past(hours: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def _overdue(store, sched, hours: float):
    """A schedule whose next due slot is `hours` behind now."""
    return store.update_schedule(sched.id, {"next_run_at": _past(hours)})


def test_catch_up_replays_one_window_per_run(tmp_path, monkeypatch):
    """3 overdue windows + catch_up(3) -> 3 sequential claims, one boundary each."""
    store = _file_store(tmp_path, monkeypatch)
    sched = _overdue(store, _new_schedule(store), 3.5)
    updated, queued = store.request_schedule_catch_up(sched.id, 3)
    assert queued == 3
    assert updated.catch_up_windows_left == 3

    boundaries = []
    for _ in range(3):
        claimed = store.mark_schedule_running(sched.id, "inst-1", f"job-{len(boundaries)}")
        assert claimed is not None
        assert claimed.claimed_catch_up is True
        boundaries.append(claimed.next_run_at)
        # The counter drains at claim time — a crash mid-run cannot replay.
        left = store.get_schedule(sched.id).catch_up_windows_left
        store.mark_schedule_run(sched.id, claimed.running_job_id or "", status="succeeded")
        assert store.get_schedule(sched.id).catch_up_windows_left == left

    # Every claim advanced exactly one boundary from the previous next_run_at.
    assert len(set(boundaries)) == 3


def test_catch_up_completion_advances_one_boundary_not_now(tmp_path, monkeypatch):
    """Marking a catch-up run done keeps the sequence: next due is the next
    historical boundary, still in the past while windows remain."""
    store = _file_store(tmp_path, monkeypatch)
    sched = _overdue(store, _new_schedule(store), 3.5)
    store.request_schedule_catch_up(sched.id, 3)
    claimed = store.mark_schedule_running(sched.id, "inst-1", "job-a")
    done = store.mark_schedule_run(sched.id, "job-a", status="succeeded")
    next_at = datetime.fromisoformat(done.next_run_at)
    # Still due (past) because 2 catch-up windows remain.
    assert next_at <= datetime.now(timezone.utc)
    # The claim already placed next_run_at on the next overdue boundary —
    # completion keeps it exactly (no double-advance, no jump-to-now).
    assert next_at == datetime.fromisoformat(claimed.next_run_at)


def test_catch_up_exhausts_then_jumps_to_now(tmp_path, monkeypatch):
    store = _file_store(tmp_path, monkeypatch)
    sched = _overdue(store, _new_schedule(store), 2.5)
    store.request_schedule_catch_up(sched.id, 2)
    for i in range(2):
        c = store.mark_schedule_running(sched.id, "inst-1", f"job-{i}")
        assert c.claimed_catch_up is True
        store.mark_schedule_run(sched.id, f"job-{i}", status="succeeded")
    # Counter drained: the next claim is a normal fire — no more catch_up tag.
    c = store.mark_schedule_running(sched.id, "inst-1", "job-last")
    assert c is not None
    assert c.claimed_catch_up is False
    assert datetime.fromisoformat(c.next_run_at) > datetime.now(timezone.utc)


def test_catch_up_caps_at_window_cap(tmp_path, monkeypatch):
    store = _file_store(tmp_path, monkeypatch)
    sched = _overdue(store, _new_schedule(store, interval="every_5m"), 40 * 5 / 60)
    _, queued = store.request_schedule_catch_up(sched.id, 0)  # all outstanding
    assert queued == store.CATCH_UP_WINDOW_CAP == 24


def test_catch_up_does_not_skip_unrequested_windows(tmp_path, monkeypatch):
    """Only the requested count replays; without a request the claim path still
    jumps to now and counts the skip exactly as before."""
    store = _file_store(tmp_path, monkeypatch)
    sched = _overdue(store, _new_schedule(store), 4.5)
    claimed = store.mark_schedule_running(sched.id, "inst-1", "job-x")
    assert claimed.claimed_catch_up is False
    assert claimed.claimed_missed_windows >= 4


def test_catch_up_roundtrip_persists(tmp_path, monkeypatch):
    store = _file_store(tmp_path, monkeypatch)
    sched = _overdue(store, _new_schedule(store), 2.5)
    store.request_schedule_catch_up(sched.id, 2)
    reloaded = store.get_schedule(sched.id)
    assert reloaded.catch_up_windows_left == 2


def test_run_entry_marks_catch_up_jobs(tmp_path, monkeypatch):
    from services.schedule_runner import _run_entry

    entry = _run_entry("j1", "succeeded", 0, datetime.now(timezone.utc), {"catch_up": True})
    assert entry["catch_up"] is True
    entry2 = _run_entry("j2", "succeeded", 0, datetime.now(timezone.utc), {})
    assert "catch_up" not in entry2


def test_catch_up_request_unknown_schedule(tmp_path, monkeypatch):
    store = _file_store(tmp_path, monkeypatch)
    updated, queued = store.request_schedule_catch_up("nope", 5)
    assert updated is None and queued == 0
