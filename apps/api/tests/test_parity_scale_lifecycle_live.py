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
