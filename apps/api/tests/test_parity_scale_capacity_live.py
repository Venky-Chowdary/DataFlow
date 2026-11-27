"""Parity at Scale phase 3: capacity, per process and per workspace (review 2, N4).

A verification waiting for capacity reads "Queued" with its place, not
"running"; a workspace cannot take every slot; the queue is bounded (beyond
it: not started, re-verify available); a schedule waits on a merely queued
verification only up to its own limit.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402
from test_parity_scale_verification_live import _env, _job, _live_lease, _report, tables  # noqa: E402,F401

live = pytest.mark.skipif(not parity_engines.available("postgresql"),
                          reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")


@pytest.fixture(autouse=True)
def _empty_capacity(monkeypatch):
    from services.parity_scale import capacity

    monkeypatch.setattr(capacity, "_RUNNING", {})
    monkeypatch.setattr(capacity, "_QUEUE", [])
    yield


def _svc():
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


def test_one_workspace_cannot_take_every_slot(monkeypatch) -> None:
    from services.parity_scale import capacity

    monkeypatch.setattr(capacity, "_RUNNING", {"a1": "ws-a"})
    monkeypatch.setattr(capacity, "_QUEUE", [("a2", "ws-a"), ("b1", "ws-b")])
    assert capacity._admissible("a2") is False  # ws-a already has its one scan
    assert capacity._admissible("b1") is True  # the next workspace goes first


def test_the_queue_is_bounded(monkeypatch) -> None:
    from services.parity_scale import capacity

    monkeypatch.setattr(capacity, "_QUEUE", [(f"j{i}", "ws") for i in range(capacity.MAX_QUEUED)])
    with pytest.raises(capacity.CapacityReached):
        capacity.admit("one-too-many", "ws", keep_alive=lambda: None, on_position=lambda n: None)


@live
def test_a_queued_verification_reads_queued_with_its_place(tables, monkeypatch) -> None:
    from services.parity_scale import capacity, verification

    monkeypatch.setattr(capacity, "_RUNNING", {"other-1": "ws-x", "other-2": "ws-y"})  # both slots taken
    job_id = _job(*tables)
    verification.start_for_job(job_id)  # background: it queues
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        table = (_svc().get_job(job_id).get("parity_scale") or {}).get("tables") or [{}]
        if table[0].get("queue_position"):
            break
        time.sleep(0.05)
    job = _svc().get_job(job_id)
    assert verification.is_queued(job)
    report = _report(job_id)
    assert report["verdict"] == "verifying" and "queued" in report["verdict_reason"]
    assert report["verification"]["state"] == "queued"
    assert report["verification"]["tables"][0]["queue_position"] == 1
    assert report["signoff"]["status"] == "blocked"
    capacity.done("other-1")  # capacity frees up: it runs and finishes
    deadline = time.monotonic() + 60
    while verification.is_running(_svc().get_job(job_id)) and time.monotonic() < deadline:
        time.sleep(0.2)
    assert _svc().get_job(job_id)["parity_scale"]["state"] == "passed"


@live
def test_a_full_queue_does_not_start_and_offers_reverify(tables, monkeypatch) -> None:
    from services.parity_scale import capacity, verification

    monkeypatch.setattr(capacity, "MAX_QUEUED", 0)
    job_id = _job(*tables)
    block = verification.start_for_job(job_id, background=False)
    assert block["state"] == "error"
    assert block["tables"][0]["error"] == verification.CAPACITY_REASON
    monkeypatch.setattr(capacity, "MAX_QUEUED", 20)
    assert verification.start_for_job(job_id, only_table=tables[1], force=True, background=False)["state"] == "passed"


@live
def test_a_schedule_waits_on_a_queued_verification_only_up_to_its_limit(tables) -> None:
    from datetime import datetime, timedelta, timezone

    from services.parity_scale import verification

    job_id = _job(*tables)

    def queued(minutes_ago: int) -> None:
        since = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()
        _svc().update_job_fields(job_id, {"parity_scale": {"state": "queued", "queued_since": since, "tables": [
            {"name": tables[1], "status": "queued", "scan_id": "s", "queued_since": since, "queue_position": 3}]}})
        _live_lease(job_id)

    class Sched:
        id = "s1"
        last_job_id = job_id
        wait_for_verification = True
        verification_queue_wait_minutes = 30

    queued(5)
    assert verification.schedule_waits_for_verification(Sched()).startswith("Waiting")
    queued(45)
    assert verification.schedule_waits_for_verification(Sched()) == ""
