"""Two creates of the same pipeline name: one row (R2-SCHED-DUP)."""

from __future__ import annotations

import threading

import pytest

import services.schedule_store as store


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    yield


def test_concurrent_creates_keep_one_schedule_name(temp_store):
    barrier = threading.Barrier(8)
    winners: list[str] = []
    errors: list[str] = []
    lock = threading.Lock()

    def go() -> None:
        barrier.wait()
        try:
            sched = store.create_schedule({
                "name": "QA_R3_sched_up",
                "source_connector_id": "src",
                "source_table": "payments",
                "dest_connector_id": "dst",
                "dest_table": "payments_copy",
                "interval": "daily",
                "mappings": [{"source": "id", "target": "id"}],
            })
        except ValueError as exc:
            with lock:
                errors.append(str(exc))
            return
        with lock:
            winners.append(sched.id)

    threads = [threading.Thread(target=go) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(winners) == 1
    assert len(errors) == 7
    assert all("already exists" in message for message in errors)
    assert len(store.list_schedules()) == 1
