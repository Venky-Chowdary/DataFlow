"""Parity at Scale phase 3: a verification's life on its worker (review defects 1 and 12).

* A worker that dies leaves a lease that expires: the verification reads as
  stopped (never "running" forever), sign-off says to re-verify, the schedule
  does not wait on it, and re-verify takes it over.
* The sweeper resumes a stopped scan from its checkpoint, reading full job
  documents (not the job list's slim rows).
* The background thread path runs, finishes and lets go of the lease.
* Another worker's live lease is left alone (no double scan).
* Every scan has a wall-clock cap; a slot wait keeps the lease alive; the scan
  and the catalog connections are closed on every path.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402
from test_parity_scale_verification_live import (  # noqa: E402,F401
    _env,
    _job,
    _live_lease,
    _report,
    tables,
)

live = pytest.mark.skipif(not parity_engines.available("postgresql"),
                          reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")


def _svc():
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


def _expire_lease(job_id: str) -> None:
    from datetime import datetime, timedelta, timezone

    lease = dict(_svc().get_job(job_id).get("parity_scale_lease") or {"token": "dead", "owner": "gone:1"})
    lease["expires_at"] = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    _svc().update_job_fields(job_id, {"parity_scale_lease": lease})


@live
def test_a_stopped_worker_reads_as_stopped_and_reverify_takes_over(tables) -> None:
    from services.parity_scale.verification import is_running, start_for_job

    src, dst = tables
    job_id = _job(src, dst)
    _svc().update_job_fields(job_id, {"parity_scale": {
        "state": "running", "started_at": "2026-11-21T00:00:00+00:00",
        "tables": [{"name": dst, "status": "running", "scan_id": "psd1-dead", "progress": {"items_done": 4}}]}})
    _expire_lease(job_id)
    job = _svc().get_job(job_id)
    assert not is_running(job)
    report = _report(job_id)
    assert report["verdict"] != "verifying"
    assert report["verification"]["state"] == "error"
    assert "stopped before it finished" in report["verification"]["tables"][0]["error"]
    assert report["signoff"]["status"] == "blocked"
    # Re-verify takes the stopped run over.
    block = start_for_job(job_id, only_table=dst, force=True, background=False)
    assert block["state"] == "passed", block
    assert _svc().get_job(job_id).get("parity_scale_lease") is None


@live
def test_the_sweeper_resumes_a_stopped_scan_from_its_checkpoint(tables) -> None:
    from services.parity_scale import verification
    from services.parity_scale.driver import ParityScan, ScanConfig
    from services.parity_scale.lease import claim

    src, dst = tables
    job_id = _job(src, dst)
    token = claim(job_id)
    spec, _why = verification.table_spec(_svc().get_job(job_id))
    scan_id = "psd1-resume0001"
    _svc().update_job_fields(job_id, {"parity_scale": {
        "state": "running", "started_at": "2026-11-21T00:00:00+00:00",
        "tables": [{"name": dst, "status": "running", "scan_id": scan_id, "progress": {}}]}})
    scan = ParityScan.start(verification.plan_for(spec), store=verification.JobScanStore(job_id, token),
                            open_side=verification.side_opener(spec),
                            config=ScanConfig(keys_stored_cap=2000, segment_rows=300), scan_id=scan_id)
    while scan.state["items_done"] < 3:
        scan.run(max_seconds=0.01)
    assert not scan.done
    scan.close()  # the worker dies here: its checkpoint is on the job, its lease runs out
    _expire_lease(job_id)
    assert job_id in _svc().list_job_ids_where({"parity_scale.state": "running"})
    assert verification.sweep_once(background=False) == 1
    job = _svc().get_job(job_id)
    table = job["parity_scale"]["tables"][0]
    assert job["parity_scale"]["state"] == "passed", table
    assert table["evidence"]["resumed"] == 1
    assert job.get("parity_scale_lease") is None
    assert not (job.get("parity_scale_checkpoints") or {})  # pruned once finished
    assert verification.sweep_once(background=False) == 0


@live
def test_the_background_thread_runs_finishes_and_lets_go(tables) -> None:
    from services.parity_scale import verification

    job_id = _job(*tables)
    block = verification.start_for_job(job_id)
    assert block["state"] in {"running", "passed"}
    deadline = time.monotonic() + 120
    while verification.is_running(_svc().get_job(job_id)) and time.monotonic() < deadline:
        time.sleep(0.2)
    job = _svc().get_job(job_id)
    assert job["parity_scale"]["state"] == "passed", job["parity_scale"]
    assert job.get("parity_scale_lease") is None
    for _ in range(50):
        if job_id not in verification._ACTIVE:
            break
        time.sleep(0.1)
    assert job_id not in verification._ACTIVE


@live
def test_a_live_lease_elsewhere_is_left_alone(tables) -> None:
    from services.parity_scale import verification

    job_id = _job(*tables)
    _live_lease(job_id)
    with pytest.raises(verification.VerificationBusy):  # a clear refusal, never a silent no-op
        verification.start_for_job(job_id, force=True, background=False)
    assert not verification.block_of(_svc().get_job(job_id))  # nothing started: another worker owns it
    _svc().update_job_fields(job_id, {"parity_scale": {"state": "running", "tables": [
        {"name": tables[1], "status": "running", "scan_id": "x"}]}})
    assert verification.resume_job(job_id, background=False) is False
    assert verification.is_running(_svc().get_job(job_id))


@live
def test_the_wall_clock_cap_ends_the_scan_as_incomplete(tables, monkeypatch) -> None:
    from services.parity_scale import verification

    monkeypatch.setattr(verification, "WALL_SECONDS", 0.0)
    job_id = _job(*tables)
    assert verification.start_for_job(job_id, background=False)["state"] == "incomplete"
    assert _report(job_id)["verdict"] != "proven"


@live
def test_the_scan_is_closed_when_the_loop_fails(tables, monkeypatch) -> None:
    from services.parity_scale import verification

    closed: list[str] = []
    real_opener = verification.side_opener

    def opener(spec: dict[str, Any]):
        inner = real_opener(spec)

        def open_(role: str, plan: Any) -> Any:
            side = inner(role, plan)
            real_close = side.close

            def close() -> None:
                closed.append(role)
                real_close()

            side.close = close
            return side

        return open_

    from services.parity_scale.driver import ParityScan

    real_run = ParityScan.run

    def broken(self: Any, max_seconds: float | None = None) -> Any:
        real_run(self, max_seconds=0.001)  # one item, then the worker hits an error mid-scan
        raise RuntimeError("the slice failed")

    monkeypatch.setattr(verification, "side_opener", opener)
    monkeypatch.setattr(ParityScan, "run", broken)
    job_id = _job(*tables)
    assert verification.start_for_job(job_id, background=False)["state"] == "error"
    assert sorted(set(closed)) == ["destination", "source"]
    assert _svc().get_job(job_id).get("parity_scale_lease") is None


def test_the_catalog_read_closes_the_first_connection_when_the_second_fails(monkeypatch) -> None:
    from services.parity_scale import verification

    opened: list[Any] = []

    class Conn:
        closed = False

        def close(self) -> None:
            self.closed = True

    def connect(engine: str, _cfg: Any) -> Any:
        if engine == "mysql":
            raise ConnectionError("refused")
        opened.append(Conn())
        return opened[-1]

    monkeypatch.setattr(verification, "connect", connect)
    monkeypatch.setattr(verification, "catalog_types", lambda *_a: {"id": "bigint"})
    spec = {"source": {"engine": "postgresql", "cfg": {}, "schema": "", "table": "t"},
            "destination": {"engine": "mysql", "cfg": {}, "schema": "", "table": "t"},
            "mappings": [], "key_columns": ["id"], "error_policy": "quarantine"}
    with pytest.raises(ConnectionError):
        verification.plan_for(spec)
    assert opened and all(c.closed for c in opened)


def test_a_slot_wait_keeps_the_lease_alive(monkeypatch) -> None:
    from services.parity_scale import lease, verification

    answers = iter([False, False, True])
    renewed: list[str] = []

    class Slots:
        def acquire(self, timeout: float) -> bool:
            return next(answers)

    monkeypatch.setattr(verification, "_SLOTS", Slots())
    monkeypatch.setattr(lease, "renew", lambda job_id, token: renewed.append(token))
    verification._take_slot("job-1", "tok")
    assert renewed == ["tok", "tok"]


@live
def test_a_failed_reverify_never_leaves_the_earlier_match_standing(tables, monkeypatch) -> None:
    """Defect 7: the record is superseded when a re-verify starts; sign-off and the pack follow the block."""
    from services.parity_scale import verification
    from services.signed_proof_pack import classify_post_write_assurance

    src, dst = tables
    job_id = _job(src, dst)
    assert verification.start_for_job(job_id, background=False)["state"] == "passed"
    assert _report(job_id)["verdict"] == "proven"

    def unreachable(_spec: Any) -> Any:
        raise ConnectionError("the destination went away")

    monkeypatch.setattr(verification, "plan_for", unreachable)
    assert verification.start_for_job(job_id, only_table=dst, force=True, background=False)["state"] == "error"
    recon = _svc().get_job(job_id)["reconciliation"]
    assert recon["parity_at_scale"]["outcome"] == "error"
    assert classify_post_write_assurance(recon)["migration_proven"] is False
    report = _report(job_id)
    assert report["verdict"] != "proven"
    assert report["signoff"]["status"] == "blocked"
    assert "did not finish" in report["signoff"]["blocked_reason"]


@live
def test_a_summary_stored_while_verification_ran_is_recomputed_when_it_ends(tables) -> None:
    """Defect 8: verification writes do not touch updated_at, so the stamp must carry its state."""
    from services.parity_report import build_parity_report, compact_summary
    from services.parity_scale import verification
    from services.parity_summary import _job_summary

    job_id = _job(*tables)
    _svc().update_job_fields(job_id, {"parity_scale": {"state": "running", "tables": [
        {"name": tables[1], "status": "running", "scan_id": "x"}]}})
    _live_lease(job_id)
    job = _svc().get_job(job_id)
    running = compact_summary(build_parity_report(job, reread=False, job_id=job_id), job)
    assert running["verdict"] == "verifying"
    _svc().update_job_fields(job_id, {"parity.summary": running, "parity_scale": {}, "parity_scale_lease": None},
                             touch_updated_at=False)
    assert verification.start_for_job(job_id, background=False)["state"] == "passed"
    assert _job_summary(_svc().get_job(job_id), job_id)["verdict"] == "proven"


@live
def test_an_item_longer_than_the_lease_keeps_its_worker(tables, monkeypatch) -> None:
    """Review 2, N1: the heartbeat renews on its own timer while the scan makes progress.

    One item runs three times the lease TTL: the job never reads "stopped", the
    sweeper does not start a second scan, the schedule still waits, and a
    re-verify in the same process is a clear refusal (409), not a no-op.
    """
    from services.parity_scale import lease, verification
    from services.parity_scale.driver import ParityScan

    monkeypatch.setattr(lease, "LEASE_TTL_S", 1)
    real = ParityScan._range_unit
    slowed: list[int] = []

    def slow_unit(self, item):
        if not slowed:
            slowed.append(1)
            time.sleep(3.0)  # one statement in flight, longer than the lease
        return real(self, item)

    monkeypatch.setattr(ParityScan, "_range_unit", slow_unit)
    job_id = _job(*tables)
    verification.start_for_job(job_id)
    deadline = time.monotonic() + 10
    while not slowed and time.monotonic() < deadline:
        time.sleep(0.05)
    time.sleep(2.0)  # well past the 1 s lease, inside the slow item
    job = _svc().get_job(job_id)
    assert verification.is_running(job), verification.effective_block(job)

    class Sched:
        id = "s1"
        last_job_id = job_id
        wait_for_verification = True

    assert verification.schedule_waits_for_verification(Sched()).startswith("Waiting")
    assert verification.sweep_once(background=False) == 0  # no second scan
    with pytest.raises(verification.VerificationBusy):
        verification.start_for_job(job_id, only_table=tables[1], force=True, background=False)
    deadline = time.monotonic() + 60
    while verification.is_running(_svc().get_job(job_id)) and time.monotonic() < deadline:
        time.sleep(0.2)
    block = _svc().get_job(job_id)["parity_scale"]
    assert block["state"] == "passed", block
    assert block["tables"][0]["evidence"]["resumed"] == 0


def test_a_silent_worker_stops_renewing(monkeypatch) -> None:
    """Review 2, N1: no progress past the stall limit → no renewal, so the lease lapses."""
    from services.parity_scale import lease, verification

    monkeypatch.setattr(lease, "LEASE_TTL_S", 0.5)
    renewed: list[str] = []
    monkeypatch.setattr(lease, "renew", lambda job_id, token: renewed.append(token))
    stalled = verification._Heartbeat("job", "tok", lambda: time.monotonic() - 1000.0, stall_after=5.0)
    stalled.start()
    time.sleep(0.5)
    stalled.stop()
    assert renewed == []
    moving = verification._Heartbeat("job", "tok", time.monotonic, stall_after=5.0)
    moving.start()
    time.sleep(0.5)
    moving.stop()
    assert renewed  # a worker making progress renews on its own timer


@live
def test_the_wall_clock_cap_holds_across_resumes(tables) -> None:
    """Review 2, N1: time spent in an item when the worker stopped counts after a resume."""
    from datetime import datetime, timedelta, timezone

    from services.parity_scale import verification
    from services.parity_scale.driver import ParityScan, ScanConfig
    from services.parity_scale.store import MemoryScanStore

    job_id = _job(*tables)
    spec, _why = verification.table_spec(_svc().get_job(job_id))
    store = MemoryScanStore()
    plan = verification.plan_for(spec)
    scan = ParityScan.start(plan, store=store, open_side=verification.side_opener(spec),
                            config=ScanConfig(segment_rows=300, max_wall_seconds=60.0))
    scan.run(max_seconds=0.01)
    scan.close()
    state = store.load(scan.state["scan_id"])
    state["inflight"] = {"t": "stream", "since": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()}
    store.save(scan.state["scan_id"], state)
    resumed = ParityScan.resume(scan.state["scan_id"], store=store, open_side=verification.side_opener(spec),
                                plan=plan)
    assert resumed.state["elapsed_s"] >= 300
    while not resumed.done:
        resumed.run(max_seconds=5)
    assert resumed.result()["outcome"] != "match"  # the cap ran out: never a pass


@live
def test_the_streamed_pass_is_chunked_into_resumable_key_ranges() -> None:
    """Review 2, N1: a streamed column is compared range by range, each its own item."""
    import uuid

    from services.parity_scale import verification
    from services.parity_scale.driver import ParityScan, ScanConfig
    from services.parity_scale.store import MemoryScanStore
    from test_parity_scale_verification_live import N, _endpoint, _sql

    src, dst = f"psd1_c_s_{uuid.uuid4().hex[:8]}", f"psd1_c_d_{uuid.uuid4().hex[:8]}"
    _sql(f'CREATE TABLE "{src}" (id bigint PRIMARY KEY, name varchar(40), amt numeric(12,2))',
         f"INSERT INTO \"{src}\" SELECT g, 'n' || g, g FROM generate_series(1, {N}) g",
         f'CREATE TABLE "{dst}" (LIKE "{src}" INCLUDING ALL)',
         f'INSERT INTO "{dst}" SELECT id, upper(name), amt FROM "{src}"',
         f"UPDATE \"{dst}\" SET name = 'WRONG' WHERE id = 2500")
    try:
        job_id = _svc().create_transfer_job({
            "status": "completed",
            "transfer_request": {"source": _endpoint(src), "destination": _endpoint(dst),
                                 "mappings": [{"source": "id", "target": "id"},
                                              {"source": "name", "target": "name", "transform": "upper"},
                                              {"source": "amt", "target": "amt"}],
                                 "sync_mode": "full_refresh_overwrite"},
            "destination_summary": {"table": dst, "primary_key_columns": ["id"]},
            "reconciliation": {"passed": True, "source_rows": N, "target_rows": N,
                               "coverage_limit": {"rows": N, "limit": 100}},
        })
        spec, _why = verification.table_spec(_svc().get_job(job_id))
        plan = verification.plan_for(spec)
        assert plan.stream_columns, "the transformed column is compared by the streamed pass"
        scan = ParityScan.start(plan, store=MemoryScanStore(), open_side=verification.side_opener(spec),
                                config=ScanConfig(segment_rows=500, mode="R"))
        scan.run(max_seconds=0.01)  # the start item: plans the ranges
        streams = [i for i in scan.state["queue"] if i["t"] == "stream"]
        assert len(streams) > 1 and all("lo" in i for i in streams)
        while not scan.done:
            scan.run(max_seconds=5)
        evidence = scan.result()
        assert evidence["outcome"] == "differences_found"
        assert evidence["differences"]["changed_rows"] == 1
        assert evidence["coverage"]["complete"] is True
    finally:
        _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')


@live
def test_a_streamed_read_runs_under_a_server_side_timeout(tables) -> None:
    """Review 2, N1: every statement the scan issues has a server-side timeout, the stream's too."""
    from services.parity_scale import verification

    job_id = _job(*tables)
    spec, _why = verification.table_spec(_svc().get_job(job_id))
    side = verification.side_opener(spec)("source", verification.plan_for(spec))
    try:
        side.set_timeout(1)
        started = time.monotonic()
        with pytest.raises(Exception, match="(?i)cancel|timeout"):
            list(side.stream("SELECT pg_sleep(5)", [], 10))
        assert time.monotonic() - started < 4
        assert side.execute("SELECT 1", [])[0][0] == 1
    finally:
        side.close()
