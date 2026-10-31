"""Run records on the per-document Mongo schedule store (live MongoDB).

The lifecycle writes only the schedule it changes (a version-CAS delta). A
claim taken by another writer between a run record's load and save must
survive, and records written before the lifecycle existed are adopted.
Needs MongoDB on ``DATAFLOW_TEST_MONGO_URI`` (default localhost:27017).
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

pymongo = pytest.importorskip("pymongo")


@pytest.fixture
def mongo_store(monkeypatch, tmp_path):
    uri = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://127.0.0.1:27017")
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MongoDB not reachable: {exc}")
    name = "r5_sched_" + uuid.uuid4().hex[:8]

    class _Svc:
        def __init__(self):
            self.client = client

        def get_database(self):
            return client[name]

    import services.schedule_store as store

    monkeypatch.setattr(store, "_mongo_backend", lambda: _Svc())
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    try:
        yield store, client[name]
    finally:
        client.drop_database(name)
        client.close()


def _schedule(store, name: str):
    return store.create_schedule({
        "name": name, "source_connector_id": "a", "source_table": "t",
        "dest_connector_id": "b", "dest_table": f"t_{name}", "interval": "hourly",
        "enabled": False,
        "mappings": [{"source": "id", "target": "id"}],
    })


def test_run_transitions_persist_per_document_and_keep_a_concurrent_claim(mongo_store):
    store, db = mongo_store
    from services import schedule_run_lifecycle as life

    one = _schedule(store, "r5a")
    other = _schedule(store, "r5b")
    run_id = life.begin_run(one.id, trigger="cadence")
    real_load = store._load_all
    raced = {"done": False}

    def _load_then_race():
        rows = real_load()
        if not raced["done"]:
            raced["done"] = True
            # Another replica claims the schedule after this load, before the save.
            db["pipeline_schedules"].update_one(
                {"_id": one.id},
                {"$set": {"running": True, "running_instance": "replica-2"},
                 "$inc": {"version": 1}},
            )
        return rows

    store._load_all = _load_then_race
    try:
        life.mark_running(one.id, run_id, "job-1")
    finally:
        store._load_all = real_load
    assert raced["done"]
    doc = db["pipeline_schedules"].find_one({"_id": one.id})
    assert doc["running"] is True and doc["running_instance"] == "replica-2", doc
    assert [r["status"] for r in doc["run_history"]] == ["running"], doc["run_history"]
    assert [t["status"] for t in doc["run_history"][0]["transitions"]] == ["pending", "running"]

    life.finish_run(one.id, run_id, "failed", reason="Worker lost: test", reason_code="worker_lost")
    for _ in range(3):
        life.record_skip(one.id, "approval_pending", "Waiting on an approval: x")
    sched = store.get_schedule(one.id)
    view = life.derive_status(sched)
    assert view["last_status"] == "failed" and view["last_status_reason"] == "Worker lost: test"
    assert view["last_skip"]["occurrences"] == 3, view
    assert len(sched.run_history) == 2, sched.run_history
    # A terminal record is final.
    life.mark_running(one.id, run_id, "job-2")
    assert store.get_schedule(one.id).run_history[0]["status"] == "failed"
    assert store.get_schedule(other.id).run_history == []


def test_legacy_mongo_document_is_adopted_by_the_finalize_writer(mongo_store):
    store, db = mongo_store
    from services import schedule_run_lifecycle as life

    sched = _schedule(store, "r5legacy")
    now = datetime.now(timezone.utc).isoformat()
    db["pipeline_schedules"].update_one(
        {"_id": sched.id},
        {"$set": {"run_history": [{"job_id": "old", "status": "completed", "started_at": now}],
                  "last_status": "completed"},
         "$inc": {"version": 1}},
    )
    life.mark_running(sched.id, "", "j-new")
    store.mark_schedule_run(sched.id, "j-new", status="failed",
                            run_entry={"job_id": "j-new", "status": "failed", "error": "boom"})
    rows = store.get_schedule(sched.id).run_history
    assert [r["status"] for r in rows] == ["completed", "failed"], rows
    assert rows[1]["reason"] == "boom" and rows[1]["run_id"], rows[1]
    assert life.derive_status(store.get_schedule(sched.id))["last_status"] == "failed"
