"""Parity at Scale phase 3: a schedule's next tick waits for verification by default, and can opt out.

Owner decision 2: the next scheduled run waits while the previous run is
verified after the write, configurable per schedule (``wait_for_verification``).
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("DATAWRAP_PARITY_SCALE", "1")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    import services.mongodb_service as ms

    monkeypatch.setattr(ms, "_mongodb_service", None)
    yield
    monkeypatch.setattr(ms, "_mongodb_service", None)


def _running_job(*, lease_minutes: int = 10) -> str:
    """A job whose verification runs on a worker holding a lease (negative minutes: expired)."""
    from datetime import datetime, timedelta, timezone

    from services.mongodb_service import get_mongodb_service

    svc = get_mongodb_service()
    job_id = svc.create_transfer_job({"status": "completed"})
    now = datetime.now(timezone.utc)
    svc.update_job_fields(job_id, {
        "parity_scale": {"state": "running", "tables": [{"name": "t", "status": "running", "scan_id": "s"}]},
        "parity_scale_lease": {"token": "w", "owner": "worker:1",
                               "expires_at": (now + timedelta(minutes=lease_minutes)).isoformat(),
                               "expires_epoch": (now + timedelta(minutes=lease_minutes)).timestamp()},
    })
    return job_id


def test_the_setting_round_trips_and_defaults_to_waiting() -> None:
    from services.schedule_store import PipelineSchedule
    from src.routers.schedules_router import ScheduleCreate, ScheduleResponse, ScheduleUpdate

    base = {"id": "s1", "name": "n", "source_connector_id": "a", "source_table": "t",
            "dest_connector_id": "b", "dest_table": "u"}
    assert PipelineSchedule.from_dict(base).wait_for_verification is True
    assert PipelineSchedule.from_dict({**base, "wait_for_verification": False}).wait_for_verification is False
    assert PipelineSchedule.from_dict({**base, "wait_for_verification": False}).to_dict()["wait_for_verification"] is False
    assert ScheduleUpdate(wait_for_verification=False).model_dump(exclude_none=True) == {"wait_for_verification": False}
    assert ScheduleUpdate().model_dump(exclude_none=True).get("wait_for_verification") is None
    assert ScheduleCreate.model_fields["wait_for_verification"].default is True
    assert ScheduleResponse.model_fields["wait_for_verification"].default is True


def test_next_tick_waits_unless_the_schedule_opted_out(monkeypatch) -> None:
    from services.parity_scale.verification import schedule_waits_for_verification

    job_id = _running_job()

    class Sched:
        id = "s1"
        last_job_id = job_id
        wait_for_verification = True

    assert schedule_waits_for_verification(Sched()).startswith("Waiting for the previous run's verification")
    Sched.wait_for_verification = False
    assert schedule_waits_for_verification(Sched()) == ""
    monkeypatch.delenv("DATAWRAP_PARITY_SCALE")
    Sched.wait_for_verification = True
    assert schedule_waits_for_verification(Sched()) == ""  # flag off: nothing changes


def test_a_stopped_verification_does_not_hold_the_schedule() -> None:
    """Defect 1: a worker that died must not make every later tick wait."""
    from services.parity_scale.verification import schedule_waits_for_verification

    job_id = _running_job(lease_minutes=-1)

    class Sched:
        id = "s1"
        last_job_id = job_id
        wait_for_verification = True

    assert schedule_waits_for_verification(Sched()) == ""


def test_the_sweep_finds_a_stopped_job_behind_many_healthy_ones() -> None:
    """Review 2, N7: only jobs without a live lease are listed, so a stopped one is never crowded out."""
    import time

    from services.mongodb_service import get_mongodb_service

    healthy = [_running_job() for _ in range(55)]
    stopped = _running_job(lease_minutes=-1)
    found = get_mongodb_service().list_job_ids_where({"parity_scale.state": ("running", "queued")}, limit=50,
                                                     lease_expired_before=time.time())
    assert found == [stopped]
    assert not set(found) & set(healthy)


def test_a_schedule_or_workspace_never_raises_the_snapshot_cap(monkeypatch) -> None:
    """Review 3, item 6: a schedule's longer hold replaced the admin's cap."""
    import services.workspace_store as workspace_store

    from services.parity_scale import verification

    monkeypatch.setattr(workspace_store, "get_workspace_settings",
                        lambda: {"verification_snapshot": True, "verification_snapshot_max_minutes": 30})

    class Tenant:
        verification_snapshot = None
        verification_snapshot_max_minutes = 240

    class Sched:
        verification_snapshot = None
        verification_snapshot_max_minutes = 120

    monkeypatch.setattr(verification, "_tenant_of", lambda job: Tenant(), raising=False)
    monkeypatch.setattr(verification, "_schedule_of", lambda job: Sched())
    assert verification.snapshot_policy({})["max_hold_s"] == 1800.0
    Sched.verification_snapshot_max_minutes = 5
    assert verification.snapshot_policy({})["max_hold_s"] == 300.0
    Tenant.verification_snapshot_max_minutes = 2
    assert verification.snapshot_policy({})["max_hold_s"] == 120.0
