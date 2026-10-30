"""Scheduled runs never fail silently or show a stale status (QA round 5).

Owner's report: "Scheduled runs fail silently or show stale status."

Every case drives the real scheduler beat (``_run_due_schedules`` ->
``_run_schedule`` -> ``_dispatch_transfer`` -> job -> ``_finalize_run``) on real
engines (PostgreSQL source, MySQL destination), then reads the result where an
operator reads it: the REST routes the UI calls (``GET /api/v1/schedules/``,
``GET /api/v1/schedules/{id}``, ``GET /api/v1/schedules/{id}/history``), the MCP
``get_schedule`` tool, and the workspace notification.

The contract (one run-lifecycle state machine):

* a run record exists before any work starts (status ``pending``), and every
  transition is persisted on it;
* every terminal state carries a reason;
* the schedule's displayed status is derived from its latest run;
* a run whose worker died is failed with the reason "worker lost";
* a skipped beat (overlap, pending approval) is recorded with its reason;
* failures reach REST, MCP and notifications.

Needs PG* and MYSQL_* env (see AGENTS.md); each test skips when its engine is
absent.
"""

from __future__ import annotations

import time
import uuid
from concurrent.futures import Future
from datetime import datetime, timedelta, timezone

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(300)

_OPEN = {"pending", "running"}
_INFLIGHT: list[dict] = []


@pytest.fixture
def env(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    import services.notification_service as ns

    sent: list[dict] = []
    monkeypatch.setattr(ns, "notify_workspace", lambda ws, payload: sent.append(dict(payload)) or [])
    import threading

    from services import schedule_runner

    # Count finalize calls in flight so a beat is only "done" once its
    # done-callback (record, close findings, notify) has fully returned.
    inflight = {"n": 0}
    lock = threading.Lock()
    real_finalize = schedule_runner._finalize_run

    def _counted(*a, **k):
        with lock:
            inflight["n"] += 1
        try:
            return real_finalize(*a, **k)
        finally:
            with lock:
                inflight["n"] -= 1

    monkeypatch.setattr(schedule_runner, "_finalize_run", _counted)
    _INFLIGHT.clear()
    _INFLIGHT.append(inflight)
    from fastapi.testclient import TestClient
    from src.main import app

    # No lifespan: the background scheduler loop must not race these beats.
    yield {"client": TestClient(app), "notes": sent}
    # A finalize callback still running on a job thread would write into the
    # next test's store (STORE_PATH is a module global): let it land first.
    deadline = time.monotonic() + 60
    while inflight["n"] and time.monotonic() < deadline:
        time.sleep(0.1)


def _pipeline(kind: str = "mysql", **extra):
    """PG source table (NULL, '', whitespace, unicode rows) and a saved schedule."""
    from services.connector_store import create_connector
    from services.schedule_store import create_schedule

    pg = h.pg_conn()
    table = "r5s_" + uuid.uuid4().hex[:8]
    try:
        cur = pg.cursor()
        cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)")  # nosec B608
        cur.executemany(
            f"INSERT INTO {table} VALUES (%s, %s)",  # nosec B608
            [(1, "Ada"), (2, None), (3, ""), (4, "  "), (5, "naïve 中文 ✓")],
        )
    finally:
        pg.close()
    src = create_connector({
        "name": f"src-{table}", "type": "postgresql", "role": "source",
        "ssl": False, **h.PG, "schema": "public",
    })
    dst = create_connector({
        "name": f"dst-{table}", "role": "destination", "ssl": False,
        **h.dest_config(kind),
    })
    sched = create_schedule({
        "name": f"r5 lifecycle {table}",
        "source_connector_id": src.id, "source_table": table,
        "dest_connector_id": dst.id, "dest_table": table,
        "interval": "hourly", "timezone": "UTC",
        "sync_mode": "full_refresh_overwrite", "enabled": True,
        "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "name")],
        **extra,
    })
    return table, src, dst, sched


def _cleanup(table: str, kind: str = "mysql") -> None:
    h.drop_table("postgresql", table)
    h.drop_table(kind, table)


def _make_due(schedule_id: str, *, retry: bool = False) -> None:
    from services.schedule_store import update_schedule

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    update_schedule(schedule_id, {"retry_at": past} if retry else {"next_run_at": past, "retry_at": None})


def _beat_and_wait(schedule_id: str) -> int:
    """One real scheduler beat, then wait for the job and the finalize callback."""
    from services import schedule_runner
    from services.schedule_store import get_schedule

    started = schedule_runner._run_due_schedules()
    sched = get_schedule(schedule_id)
    job_id = str(sched.running_job_id or "")
    if job_id:
        h.wait_job(job_id, 180)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and (
            get_schedule(schedule_id).running or (_INFLIGHT and _INFLIGHT[0]["n"])
        ):
            time.sleep(0.2)
    return started


def _rest(client, schedule_id: str) -> tuple[dict, dict, list[dict]]:
    one = client.get(f"/api/v1/schedules/{schedule_id}")
    assert one.status_code == 200, one.text
    rows = client.get("/api/v1/schedules/")
    assert rows.status_code == 200, rows.text
    row = next(r for r in rows.json() if r["id"] == schedule_id)
    hist = client.get(f"/api/v1/schedules/{schedule_id}/history")
    assert hist.status_code == 200, hist.text
    return one.json(), row, hist.json()["runs"]


def _mcp(schedule_id: str) -> dict:
    from src.ai.copilot.tools import get_pilot_tools

    res = get_pilot_tools().execute("get_schedule", {"schedule_id": schedule_id})
    assert res.success, res.error
    return dict(res.output or {})


def _statuses(run: dict) -> list[str]:
    return [t["status"] for t in run.get("transitions") or []]


def test_source_auth_failure_is_a_failed_run_recorded_before_work_and_surfaced(env, monkeypatch):
    """Run 1 lands; then the source login is refused. The failed run is recorded
    and surfaced at once, and - a rejected login being deterministic - no
    retry is owed: the run names the credential fix instead.

    The run then fails transiently (source port closed) with one retry
    allowed: the first failed attempt used to leave the schedule showing the
    previous ``completed`` (stale) until the retry finished.

    PostgreSQL words the same unknown login 'password authentication failed'
    (password auth, CI) or 'role ... does not exist' (trust auth): both are a
    rejected login, so the outcome no longer depends on pg_hba.conf.
    """
    import socket

    from services.connector_store import update_connector
    from services.schedule_store import get_schedule
    from src.transfer.engine import UniversalTransferEngine

    client, notes = env["client"], env["notes"]
    table, src, _dst, sched = _pipeline(max_retries=1, retry_backoff_seconds=0)
    seen_at_job_create: list[list[dict]] = []
    original = UniversalTransferEngine._create_pending_job

    def _snapshot(self, request, *a, **k):
        # The moment the first unit of work (the job) is created, the run record
        # must already exist.
        seen_at_job_create.append(list(get_schedule(sched.id).run_history))
        return original(self, request, *a, **k)

    monkeypatch.setattr(UniversalTransferEngine, "_create_pending_job", _snapshot)
    try:
        _make_due(sched.id)
        assert _beat_and_wait(sched.id) == 1
        one, row, runs = _rest(client, sched.id)
        assert one["last_status"] == "completed", one
        assert runs[0]["status"] == "completed" and runs[0]["reason"], runs[0]
        assert _statuses(runs[0]) == ["pending", "running", "completed"], runs[0]
        assert seen_at_job_create[0] and seen_at_job_create[0][-1]["status"] == "pending"
        assert seen_at_job_create[0][-1]["run_id"] == runs[0]["run_id"]
        assert seen_at_job_create[0][-1]["trigger"] == "cadence"

        # QA: force a source auth failure.
        update_connector(src.id, {"username": "r5_no_such_role"})
        notes.clear()
        _make_due(sched.id)
        assert _beat_and_wait(sched.id) == 1
        one, row, runs = _rest(client, sched.id)
        for view in (one, row):
            assert view["last_status"] == "failed", view
            assert "r5_no_such_role" in view["last_status_reason"], view
            assert view["current_run"] is None, view
        assert runs[0]["status"] == "failed" and "r5_no_such_role" in runs[0]["reason"], runs[0]
        assert _statuses(runs[0]) == ["pending", "running", "failed"], runs[0]
        # A rejected login fails identically next time: no retry, the fix named.
        assert not runs[0].get("retry_scheduled"), runs[0]
        assert "Credentials" in str(runs[0].get("retry_refused") or ""), runs[0]
        assert (runs[0].get("failure_class") or {}).get("kind") == "deterministic", runs[0]
        assert not get_schedule(sched.id).retry_at
        mcp = _mcp(sched.id)
        assert mcp["last_status"] == "failed", mcp
        assert "r5_no_such_role" in mcp["last_status_reason"], mcp
        assert mcp["recent_runs"][0]["status"] == "failed", mcp
        failed = [n for n in notes if n.get("status") == "failed" and "r5_no_such_role" in str(n.get("error"))]
        assert failed, notes

        # Parked on that finding: the next due beat does not replay the login,
        # it is recorded as skipped with the reason.
        _make_due(sched.id)
        assert _beat_and_wait(sched.id) == 0
        latest = _rest(client, sched.id)[2][0]
        assert latest["status"] == "skipped", latest
        assert latest["reason_code"] == "approval_pending", latest
        assert "r5_no_such_role" in latest["reason"], latest
    finally:
        _cleanup(table)

    # A transient failure (source port closed) with one retry allowed.
    table, src, _dst, sched = _pipeline(max_retries=1, retry_backoff_seconds=0)
    try:
        _make_due(sched.id)
        assert _beat_and_wait(sched.id) == 1
        assert _rest(client, sched.id)[0]["last_status"] == "completed"
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
        probe.close()
        update_connector(src.id, {"port": closed_port})
        _make_due(sched.id)
        assert _beat_and_wait(sched.id) == 1
        one, row, runs = _rest(client, sched.id)
        # A retry is owed, but the schedule must not keep showing a stale status.
        for view in (one, row):
            assert view["last_status"] == "failed", view
            assert view["last_status_reason"], view
        assert runs[0]["status"] == "failed" and runs[0]["retry_scheduled"], runs[0]
        assert runs[0]["reason"], runs[0]
        assert _statuses(runs[0]) == ["pending", "running", "failed"], runs[0]

        _make_due(sched.id, retry=True)
        assert _beat_and_wait(sched.id) == 1
        one, row, runs = _rest(client, sched.id)
        assert one["last_status"] == "failed" and one["current_run"] is None, one
        assert runs[0]["trigger"] == "retry" and runs[0]["attempt"] == 1, runs[0]
        assert runs[0]["status"] == "failed" and runs[0]["reason"], runs[0]
        assert _mcp(sched.id)["recent_runs"][0]["status"] == "failed"
    finally:
        _cleanup(table)


def test_destination_dropped_mid_run_fails_the_run_with_its_reason(env, monkeypatch):
    """QA: drop the destination table mid-way through a scheduled load."""
    from services.mongodb_service import MemoryMongoDBService

    client = env["client"]
    table, _src, _dst, sched = _pipeline()
    pg = h.pg_conn()
    try:
        pg.cursor().execute(
            f"INSERT INTO {table} SELECT g, 'row ' || g FROM generate_series(10, 60000) g"  # nosec B608
        )
    finally:
        pg.close()
    dropped: dict = {}
    original = MemoryMongoDBService.update_job_status

    def _drop_after_first_batch(self, job_id, status, **kw):
        if not dropped and status == "running" and int(kw.get("records_processed") or 0) > 0:
            dropped["rows"] = kw.get("records_processed")
            h.execute("mysql", f"DROP TABLE IF EXISTS `{table}`")  # nosec B608
        return original(self, job_id, status, **kw)

    monkeypatch.setattr(MemoryMongoDBService, "update_job_status", _drop_after_first_batch)
    try:
        _make_due(sched.id)
        assert _beat_and_wait(sched.id) == 1
        assert dropped, "the destination was never dropped mid-run"
        one, row, runs = _rest(client, sched.id)
        for view in (one, row):
            assert view["last_status"] == "failed", view
            assert "doesn't exist" in view["last_status_reason"], view
        assert runs[0]["status"] == "failed", runs[0]
        assert _statuses(runs[0]) == ["pending", "running", "failed"], runs[0]
        assert runs[0]["reason_code"], runs[0]
        assert any(n.get("status") == "failed" for n in env["notes"]), env["notes"]
    finally:
        _cleanup(table)


def test_worker_killed_mid_run_is_failed_worker_lost_not_running_forever(env, monkeypatch):
    """QA: kill the worker during a run. The job was accepted, went ``running``
    and its worker died: no callback, no heartbeat, no lease. The base recorded
    ``last_status='running'`` for good."""
    import src.transfer.background as bg
    from services import schedule_runner
    from services.mongodb_service import get_mongodb_service
    from services.schedule_store import get_schedule

    client, notes = env["client"], env["notes"]
    table, _src, _dst, sched = _pipeline()
    real_submit = bg.run_transfer_async

    def _worker_dies(job_id, request, **_kw):
        stale = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        get_mongodb_service().update_job_status(
            job_id, "running", phase="load", heartbeat_at=stale, records_processed=2
        )
        return Future()  # never completes: the thread that ran it is gone

    try:
        monkeypatch.setattr(bg, "run_transfer_async", _worker_dies)
        _make_due(sched.id)
        assert schedule_runner._run_due_schedules() == 1
        job_id = get_schedule(sched.id).running_job_id
        one, _row, runs = _rest(client, sched.id)
        assert one["current_run"]["status"] == "running", one
        assert one["current_run"]["job_id"] == job_id, one

        # Simulated restart: startup reclaim, then the next beat.
        monkeypatch.setattr(bg, "run_transfer_async", real_submit)
        schedule_runner._clear_stale_running_schedules()
        schedule_runner._run_due_schedules()

        one, row, runs = _rest(client, sched.id)
        for view in (one, row):
            assert view["last_status"] == "failed", view
            assert "worker lost" in view["last_status_reason"].lower(), view
            assert not view["running"], view
        assert one["current_run"] is None, one
        assert runs[0]["job_id"] == job_id and runs[0]["status"] == "failed", runs[0]
        assert runs[0]["reason_code"] == "worker_lost", runs[0]
        assert _statuses(runs[0]) == ["pending", "running", "failed"], runs[0]
        job = get_mongodb_service().get_job(job_id)
        assert job["status"] == "failed" and job.get("error_code") == "worker_lost", job
        assert "worker lost" in _mcp(sched.id)["last_status_reason"].lower()
        assert any(
            n.get("status") == "failed" and "worker lost" in str(n.get("error")).lower()
            for n in notes
        ), notes

        # Not wedged: the next due beat runs and lands.
        _make_due(sched.id)
        assert _beat_and_wait(sched.id) == 1
        one, _row, _runs = _rest(client, sched.id)
        assert one["last_status"] == "completed", one
    finally:
        _cleanup(table)


def test_scheduler_killed_before_the_job_existed_is_swept_as_worker_lost(env, monkeypatch):
    """The scheduler process dies in preflight (claim taken, no job yet). After
    a restart the run is failed "worker lost" and the claim is released; the
    base left nothing behind but a claim."""
    import services.schedule_store as store
    from services import schedule_runner

    client, notes = env["client"], env["notes"]
    table, _src, _dst, sched = _pipeline()
    real_build = schedule_runner.build_schedule_request
    kill = {"on": True}

    class _ProcessKilled(BaseException):
        """Not an Exception: nothing in the process gets to clean up."""

    def _build(*a, **k):
        if kill["on"]:
            raise _ProcessKilled()
        return real_build(*a, **k)

    monkeypatch.setattr(schedule_runner, "build_schedule_request", _build)
    try:
        _make_due(sched.id)
        with pytest.raises(_ProcessKilled):
            schedule_runner._run_due_schedules()
        kill["on"] = False
        one, _row, runs = _rest(client, sched.id)
        assert one["current_run"]["status"] == "pending", one

        # Restart after the unbound-claim grace: age the claim and the record.
        old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        rows = store._load_all()
        for i, s in enumerate(rows):
            if s.id == sched.id:
                hist = [dict(r) for r in s.run_history]
                hist[-1]["heartbeat_at"] = old
                rows[i] = store.PipelineSchedule.from_dict(
                    {**s.to_dict(), "running_started_at": old, "run_history": hist}
                )
        store._save_all(rows)
        notes.clear()
        schedule_runner._clear_stale_running_schedules()

        one, row, runs = _rest(client, sched.id)
        for view in (one, row):
            assert view["last_status"] == "failed", view
            assert "worker lost" in view["last_status_reason"].lower(), view
            assert not view["running"], view
        assert runs[0]["reason_code"] == "worker_lost" and not runs[0]["job_id"], runs[0]
        assert _statuses(runs[0]) == ["pending", "failed"], runs[0]
        assert any("worker lost" in str(n.get("error")).lower() for n in notes), notes

        _make_due(sched.id)
        assert _beat_and_wait(sched.id) == 1
        one, _row, _runs = _rest(client, sched.id)
        assert one["last_status"] == "completed", one
    finally:
        _cleanup(table)


def test_overlapping_beat_is_recorded_as_skipped_with_the_holder(env, monkeypatch):
    """QA: overlapping beats. The second due beat while run A is still in flight
    used to leave no trace at all."""
    import src.transfer.background as bg
    from services import schedule_runner
    from services.schedule_store import get_schedule

    client = env["client"]
    table, _src, _dst, sched = _pipeline()
    held: dict = {}

    def _queued(job_id, request, **_kw):
        held.update(job_id=job_id, request=request, future=Future())
        return held["future"]  # job stays pending (queued behind busy workers)

    try:
        monkeypatch.setattr(bg, "run_transfer_async", _queued)
        _make_due(sched.id)
        assert schedule_runner._run_due_schedules() == 1
        _make_due(sched.id)
        assert schedule_runner._run_due_schedules() == 0  # overlap
        one, _row, runs = _rest(client, sched.id)
        skipped = [r for r in runs if r["status"] == "skipped"]
        assert skipped, runs
        assert skipped[0]["reason_code"] == "already_running", skipped[0]
        assert held["job_id"] in skipped[0]["reason"], skipped[0]
        assert one["last_skip"]["reason_code"] == "already_running", one
        assert one["current_run"]["job_id"] == held["job_id"], one

        # Run A really executes now, and its result becomes the status.
        bg._run_transfer(held["job_id"], held["request"])
        held["future"].set_result(None)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and get_schedule(sched.id).running:
            time.sleep(0.2)
        one, _row, runs = _rest(client, sched.id)
        assert one["last_status"] == "completed", one
        assert one["current_run"] is None, one
        run_a = next(r for r in runs if r["job_id"] == held["job_id"])
        assert run_a["status"] == "completed", run_a
        assert _statuses(run_a) == ["pending", "running", "completed"], run_a
        assert one["last_skip"] is None, one  # the overlap was about run A, now done
        assert h.query("mysql", f"SELECT COUNT(*) FROM `{table}`")[0][0] == 5  # nosec B608
    finally:
        _cleanup(table)


def test_pending_approval_beat_is_recorded_and_status_comes_from_the_run(env):
    """QA: a pending approval. The beat was skipped with no record, and the
    schedule showed the approval inbox's own copy (``needs_approval``) instead
    of what the last run did."""
    from services import schedule_runner
    from services.schedule_approvals import build_approval_request, open_approval_request

    client = env["client"]
    table, _src, _dst, sched = _pipeline()
    try:
        _make_due(sched.id)
        assert _beat_and_wait(sched.id) == 1
        open_approval_request(sched.id, build_approval_request(
            kind="run_refused", code="R5_TEST", finding="Needs a human: r5 finding",
            corrective_action="Answer the finding.", binding={},
        ))
        _make_due(sched.id)
        assert schedule_runner._run_due_schedules() == 0
        one, row, runs = _rest(client, sched.id)
        assert row["needs_approval"] is True, row
        for view in (one, row):
            assert view["last_status"] == "completed", view  # the latest run
            assert view["last_skip"]["reason_code"] == "approval_pending", view
            assert "r5 finding" in view["last_skip"]["reason"], view
        assert runs[0]["status"] == "skipped" and runs[0]["reason_code"] == "approval_pending", runs[0]
        # A second beat while still parked does not add another row.
        schedule_runner._run_due_schedules()
        _one, _row, again = _rest(client, sched.id)
        assert _row["needs_approval"] is True, (_row, again)
        assert len([r for r in again if r["status"] == "skipped"]) == 1, again
        mcp = _mcp(sched.id)
        assert mcp["last_status"] == "completed" and mcp.get("needs_approval") is True, (mcp, _row)
        assert mcp["last_skip"]["reason_code"] == "approval_pending", mcp
    finally:
        _cleanup(table)


def test_preflight_crash_before_any_job_is_a_failed_run_and_notifies(env, monkeypatch):
    """An exception before a job record exists (preflight crash) was only a log
    line: no history, the previous success still displayed, no notification,
    and the schedule re-fired on every beat."""
    from services import schedule_runner
    from services.schedule_store import get_schedule

    client, notes = env["client"], env["notes"]
    table, _src, _dst, sched = _pipeline()
    try:
        _make_due(sched.id)
        assert _beat_and_wait(sched.id) == 1

        def _crash(*_a, **_k):
            raise RuntimeError("preflight crashed: r5 injected")

        monkeypatch.setattr(schedule_runner, "build_schedule_request", _crash)
        notes.clear()
        _make_due(sched.id)
        assert schedule_runner._run_due_schedules() == 0
        one, row, runs = _rest(client, sched.id)
        for view in (one, row):
            assert view["last_status"] == "failed", view
            assert "preflight crashed: r5 injected" in view["last_status_reason"], view
            assert not view["running"], view
        assert runs[0]["status"] == "failed" and not runs[0]["job_id"], runs[0]
        assert _statuses(runs[0]) == ["pending", "failed"], runs[0]
        assert any("preflight crashed" in str(n.get("error")) for n in notes), notes
        # The slot is consumed: the next beat does not hot-loop the same crash.
        assert schedule_runner._run_due_schedules() == 0
        assert len([r for r in get_schedule(sched.id).run_history if r.get("status") == "failed"]) == 1
    finally:
        _cleanup(table)


def test_legacy_schedule_documents_are_read_through_the_run_records(env):
    """Backfill: documents written before run records existed (no run_id, no
    reason, a stored ``last_status`` that disagrees with the last run)."""
    import services.schedule_store as store

    client = env["client"]
    now = datetime.now(timezone.utc).isoformat()
    legacy = store.PipelineSchedule.from_dict({
        "id": "legacy-r5", "name": "legacy r5", "source_connector_id": "a",
        "source_table": "t", "dest_connector_id": "b", "dest_table": "t",
        "interval": "hourly", "enabled": True, "created_at": now,
        "last_status": "completed", "last_job_id": "j2",
        "run_history": [
            {"job_id": "j1", "status": "completed", "attempt": 0, "started_at": now,
             "finished_at": now, "error": ""},
            {"job_id": "j2", "status": "failed", "attempt": 0, "started_at": now,
             "finished_at": now, "error": "boom from an old run"},
        ],
    })
    store._save_all(store._load_all() + [legacy])
    one, row, runs = _rest(client, "legacy-r5")
    for view in (one, row):
        assert view["last_status"] == "failed", view
        assert view["last_status_reason"] == "boom from an old run", view
    assert [r["status"] for r in runs] == ["failed", "completed"], runs
    assert all(r["run_id"] and r["reason"] for r in runs), runs
