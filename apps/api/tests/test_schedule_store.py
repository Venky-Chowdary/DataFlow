"""Schedule store unit tests."""

from datetime import datetime, timedelta, timezone

import pytest

import services.schedule_store as store


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    path = tmp_path / "schedules.json"
    monkeypatch.setattr(store, "STORE_PATH", path)
    yield path


def test_create_and_list(temp_store):
    sched = store.create_schedule({
        "name": "Nightly sync",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
    })
    assert sched.enabled
    assert sched.interval == "daily"
    assert len(store.list_schedules()) == 1


def test_due_schedules(temp_store):
    sched = store.create_schedule({
        "name": "Hourly",
        "source_connector_id": "a",
        "source_table": "t1",
        "dest_connector_id": "b",
        "dest_table": "t2",
        "interval": "hourly",
    })
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    store.update_schedule(sched.id, {"next_run_at": past})
    due = store.due_schedules()
    assert any(s.id == sched.id for s in due)


def test_mark_run_updates_next(temp_store):
    sched = store.create_schedule({
        "name": "Weekly",
        "source_connector_id": "a",
        "source_table": "t1",
        "dest_connector_id": "b",
        "dest_table": "t2",
        "interval": "weekly",
    })
    updated = store.mark_schedule_run(sched.id, "job-123")
    assert updated is not None
    assert updated.last_job_id == "job-123"
    assert updated.run_count == 1
    assert updated.next_run_at is not None
