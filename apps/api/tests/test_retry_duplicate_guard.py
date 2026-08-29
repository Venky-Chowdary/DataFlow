"""Retry-from-start must never duplicate rows a failed attempt already wrote.

Resume continues from a committed checkpoint; retry re-reads the source from
zero. For an append that is the difference between one copy of the data and
two, and the scheduler does it unattended, so the decision lives in one shared
contract used by both the operator endpoint and the schedule runner.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.execution_engine_contract import (  # noqa: E402
    ExecutionContractError,
    assert_retry_from_start_allowed,
    committed_rows_of,
    decide_retry_from_start,
)
from services.schedule_runner import _is_success, _retry_decision, _should_retry  # noqa: E402
from services.schedule_store import count_missed_windows  # noqa: E402


def test_append_with_committed_rows_refuses_retry():
    decision = decide_retry_from_start(
        status="failed", sync_mode="incremental_append", rows_committed=500
    )
    assert decision["allowed"] is False
    assert "500 row(s)" in decision["reason"]
    assert "resume" in decision["reason"].lower()


def test_append_with_zero_committed_rows_allows_retry():
    decision = decide_retry_from_start(
        status="failed", sync_mode="incremental_append", rows_committed=0
    )
    assert decision["allowed"] is True
    assert decision["kind"] == "from_zero_no_writes"


def test_unknown_committed_count_is_not_zero():
    decision = decide_retry_from_start(
        status="failed",
        sync_mode="full_refresh_append",
        rows_committed=0,
        rows_committed_known=False,
    )
    assert decision["allowed"] is False
    assert "unknown number of rows" in decision["reason"]


def test_convergent_sync_mode_allows_retry():
    for mode in ("full_refresh_overwrite", "incremental_upsert", "mirror"):
        decision = decide_retry_from_start(
            status="failed", sync_mode=mode, rows_committed=10_000
        )
        assert decision["allowed"] is True, mode
        assert decision["kind"] == "from_zero_idempotent_sync"


def test_cancelled_run_is_not_restarted_even_when_convergent():
    decision = decide_retry_from_start(
        status="cancelled", sync_mode="full_refresh_overwrite", rows_committed=0
    )
    assert decision["allowed"] is False
    assert "cancelled" in decision["reason"].lower()


def test_assert_helper_fails_closed():
    try:
        assert_retry_from_start_allowed(
            status="failed", sync_mode="append", rows_committed=1
        )
    except ExecutionContractError as exc:
        assert "duplicate" in str(exc).lower()
    else:
        raise AssertionError("a duplicating retry was allowed through")


def test_committed_rows_of_reports_unknown_rather_than_zero():
    assert committed_rows_of({"records_processed": 42}) == (42, True)
    assert committed_rows_of({"records_processed": "not-a-number"}) == (0, False)
    assert committed_rows_of(None) == (0, False)
    assert committed_rows_of({}) == (0, False)
    assert committed_rows_of({"checkpoint": {"rows_processed": 7}}) == (7, True)


def test_scheduler_does_not_retry_a_cancelled_run():
    assert _should_retry("cancelled", attempt=0, max_retries=3) is False
    assert _retry_decision("cancelled", 0, 3)["reason"]


def test_scheduler_does_not_retry_a_partially_committed_append():
    assert (
        _should_retry(
            "failed",
            attempt=0,
            max_retries=3,
            sync_mode="incremental_append",
            rows_committed=500,
        )
        is False
    )


def test_scheduler_still_retries_a_clean_failure():
    assert _should_retry(
        "failed", attempt=0, max_retries=3, sync_mode="incremental_append"
    ) is True
    assert _should_retry("failed", attempt=3, max_retries=3) is False


def test_scheduler_success_vocabulary_matches_the_engine():
    assert _is_success("completed") is True
    assert _is_success("completed_with_quarantine") is True
    assert _is_success("failed") is False
    assert _is_success(None) is False


def test_missed_cron_windows_are_counted():
    due = datetime.now(timezone.utc) - timedelta(days=3, minutes=1)
    missed = count_missed_windows(
        cron="0 3 * * *", interval="daily", tz="UTC", next_run_at=due.isoformat()
    )
    assert missed >= 2


def test_on_time_beat_counts_no_missed_window():
    due = datetime.now(timezone.utc) - timedelta(seconds=5)
    assert (
        count_missed_windows(
            cron="*/30 * * * *", interval="hourly", tz="UTC", next_run_at=due.isoformat()
        )
        == 0
    )
    assert (
        count_missed_windows(cron="", interval="daily", tz="UTC", next_run_at=None) == 0
    )


def test_missed_window_scan_is_bounded():
    due = datetime.now(timezone.utc) - timedelta(days=3650)
    missed = count_missed_windows(
        cron="* * * * *", interval="hourly", tz="UTC", next_run_at=due.isoformat()
    )
    assert missed <= 1000


def _retry_via_api(monkeypatch, job: dict, *, force: bool = False):
    from fastapi.testclient import TestClient

    from services.mongodb_service import get_mongodb_service
    from src.main import app

    mongo = get_mongodb_service()
    created: dict[str, object] = {}
    monkeypatch.setattr(mongo, "get_job", lambda jid: job if jid == job["job_id"] else None)
    monkeypatch.setattr(
        mongo,
        "update_job_status",
        lambda jid, status, **kw: created.update({"job_id": jid, "status": status, **kw}),
    )

    import src.transfer.engine as engine_mod

    class _Engine:
        def _create_pending_job(self, req):
            return "new-job"

    monkeypatch.setattr(engine_mod, "get_transfer_engine", lambda: _Engine())
    monkeypatch.setattr("src.transfer.background.run_transfer_async", lambda *a, **k: None)

    url = f"/api/v1/connectors/jobs/{job['job_id']}/retry" + ("?force=true" if force else "")
    with TestClient(app) as client:
        return client.post(url), created


def _failed_append_job() -> dict:
    return {
        "job_id": "job-append",
        "status": "failed",
        "records_processed": 500,
        "transfer_request": {
            "source": {"kind": "database", "type": "sqlite", "table": "src"},
            "destination": {"kind": "database", "type": "sqlite", "table": "dst"},
            "sync_mode": "incremental_append",
        },
    }


def test_api_refuses_a_duplicating_retry_and_names_resume(monkeypatch):
    res, _ = _retry_via_api(monkeypatch, _failed_append_job())
    assert res.status_code == 409
    detail = res.json()["detail"]
    assert detail["primary_action"] == "resume"
    assert detail["rows_committed"] == 500
    assert "duplicate" in detail["error"].lower()


def test_api_force_records_the_operator_acknowledgement(monkeypatch):
    res, created = _retry_via_api(monkeypatch, _failed_append_job(), force=True)
    assert res.status_code == 200, res.text
    body = res.json()
    assert body.get("duplicate_risk_acknowledged") is True
    # The override must be legible on the job itself, not only in the response.
    assert "duplicate" in str(created.get("message", "")).lower()


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


def test_parked_retry_is_durable_and_releases_the_running_claim(tmp_path, monkeypatch):
    store = _file_store(tmp_path, monkeypatch)
    sched = _new_schedule(store, max_retries=2)
    assert store.mark_schedule_running(sched.id, "instance-a") is not None

    parked = store.schedule_retry(
        sched.id,
        retry_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        attempt=1,
        run_entry={"job_id": "job-1", "status": "failed"},
    )
    assert parked is not None
    assert parked.retry_attempt == 1
    assert parked.running is False
    assert parked.run_history[-1]["job_id"] == "job-1"

    # Nothing in memory carries the retry: the store alone makes it due again.
    assert sched.id in {s.id for s in store.due_schedules()}


def test_parked_retry_waits_for_its_backoff(tmp_path, monkeypatch):
    store = _file_store(tmp_path, monkeypatch)
    sched = _new_schedule(store, max_retries=2)
    store.schedule_retry(
        sched.id,
        retry_at=datetime.now(timezone.utc) + timedelta(minutes=10),
        attempt=1,
    )
    assert sched.id not in {s.id for s in store.due_schedules()}


def test_pending_retry_suppresses_the_ordinary_cadence(tmp_path, monkeypatch):
    store = _file_store(tmp_path, monkeypatch)
    sched = _new_schedule(store, max_retries=2)
    past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    store.update_schedule(sched.id, {"next_run_at": past})
    store.schedule_retry(
        sched.id,
        retry_at=datetime.now(timezone.utc) + timedelta(minutes=10),
        attempt=1,
    )
    # The cadence is overdue, but the owed retry has not come round yet: starting
    # a fresh attempt now would run two attempts of the same schedule.
    assert sched.id not in {s.id for s in store.due_schedules()}


def test_terminal_run_clears_the_pending_retry(tmp_path, monkeypatch):
    store = _file_store(tmp_path, monkeypatch)
    sched = _new_schedule(store, max_retries=2)
    store.schedule_retry(
        sched.id, retry_at=datetime.now(timezone.utc), attempt=1
    )
    updated = store.mark_schedule_run(sched.id, "job-2", status="completed")
    assert updated is not None
    assert updated.retry_at is None
    assert updated.retry_attempt == 0


def test_a_long_running_job_keeps_its_claim_past_the_elapsed_ceiling(
    tmp_path, monkeypatch
):
    store = _file_store(tmp_path, monkeypatch)
    sched = _new_schedule(store)
    store.mark_schedule_running(sched.id, "instance-a")
    store.set_running_job(sched.id, "job-long")
    store.update_schedule(
        sched.id,
        {
            "running_started_at": (
                datetime.now(timezone.utc) - timedelta(hours=9)
            ).isoformat()
        },
    )
    monkeypatch.setattr(store, "_job_is_live", lambda job_id: True)

    # A 9-hour migration is legitimate; reclaiming it would start a second
    # writer against the same destination.
    assert store._is_running_stale(store.get_schedule(sched.id)) is False
    assert store.mark_schedule_running(sched.id, "instance-b") is None


def test_a_crashed_run_releases_its_claim_after_the_grace_period(tmp_path, monkeypatch):
    store = _file_store(tmp_path, monkeypatch)
    sched = _new_schedule(store)
    store.mark_schedule_running(sched.id, "instance-a")
    store.set_running_job(sched.id, "job-dead")
    store.update_schedule(
        sched.id,
        {
            "running_started_at": (
                datetime.now(timezone.utc) - timedelta(minutes=30)
            ).isoformat()
        },
    )
    monkeypatch.setattr(store, "_job_is_live", lambda job_id: False)

    assert store._is_running_stale(store.get_schedule(sched.id)) is True
    assert store.mark_schedule_running(sched.id, "instance-b") is not None


def test_an_unknowable_job_falls_back_to_the_elapsed_ceiling(tmp_path, monkeypatch):
    store = _file_store(tmp_path, monkeypatch)
    sched = _new_schedule(store)
    store.mark_schedule_running(sched.id, "instance-a")
    monkeypatch.setattr(store, "_job_is_live", lambda job_id: None)

    assert store._is_running_stale(store.get_schedule(sched.id)) is False
    store.update_schedule(
        sched.id,
        {
            "running_started_at": (
                datetime.now(timezone.utc) - timedelta(hours=5)
            ).isoformat()
        },
    )
    assert store._is_running_stale(store.get_schedule(sched.id)) is True


def test_missed_windows_land_on_the_schedule_record(tmp_path, monkeypatch):
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    sched = store.create_schedule(
        {
            "name": "missed-window",
            "source_connector_id": "a",
            "source_table": "s",
            "dest_connector_id": "b",
            "dest_table": "d",
            "interval": "hourly",
            "cron": "0 * * * *",
        }
    )
    stale = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    store.update_schedule(sched.id, {"next_run_at": stale})
    updated = store.mark_schedule_run(
        sched.id, "job-1", status="completed", run_entry={"job_id": "job-1"}
    )
    assert updated is not None
    assert updated.last_missed_windows >= 3
    assert updated.missed_window_count >= 3
    assert updated.run_history[-1]["missed_windows"] >= 3
