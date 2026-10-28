"""D172 for schedules - a scheduled run uses the saved connector as it is now.

A schedule keeps connector ids, not hosts. Each fire must read the connector at
run time (current host/credentials), refuse a connector from another workspace,
and name a deleted connector instead of replaying anything frozen.
"""

from __future__ import annotations

import os
from concurrent.futures import Future

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.connector_store as connectors  # noqa: E402
import services.schedule_runner as runner  # noqa: E402
import services.schedule_store as schedules  # noqa: E402


@pytest.fixture()
def stores(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setattr(schedules, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedules, "_mongo_backend", lambda: None)
    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod

    store = mongo_mod.MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    return store


def _connector(name: str, host: str, workspace: str = "") -> object:
    return connectors.create_connector(
        {
            "type": "mysql", "name": name, "host": host, "port": 3306,
            "database": "app", "username": "u", "password": "p",
            "ssl": False, "workspace_id": workspace,
        }
    )


def _schedule(src, dst, workspace: str = ""):
    return schedules.create_schedule(
        {
            "name": "nightly", "source_connector_id": src.id, "source_table": "orders",
            "dest_connector_id": dst.id, "dest_table": "orders_copy",
            "interval": "daily", "sync_mode": "full_refresh_overwrite",
            "workspace_id": workspace,
            "mappings": [{"source": "id", "target": "id"}],
        }
    )


@pytest.fixture()
def captured(monkeypatch):
    seen: dict = {}

    def fake_run(job_id, request, **_kw):
        seen["request"] = request
        fut: Future = Future()
        fut.set_result(None)
        return fut

    monkeypatch.setattr("src.transfer.background.run_transfer_async", fake_run)
    return seen


def test_scheduled_run_uses_the_connectors_current_host(stores, captured, monkeypatch):
    src, dst = _connector("src", "old-db.internal"), _connector("dst", "dst.internal")
    sched = _schedule(src, dst)
    connectors.update_connector(src.id, {"host": "new-db.internal"})

    assert runner._dispatch_transfer(sched.id, allow_paused=True)
    assert captured["request"].source.host == "new-db.internal"
    assert captured["request"].destination.host == "dst.internal"


def test_deleted_connector_is_named_not_replayed(stores, captured, monkeypatch):
    src, dst = _connector("src", "db.internal"), _connector("dst", "dst.internal")
    sched = _schedule(src, dst)
    connectors.delete_connector(src.id)
    parked: dict = {}
    monkeypatch.setattr(
        runner, "_park_on_decision",
        lambda sid, exc, **kw: parked.update(message=str(exc)),
    )
    import services.mongodb_service as mongo_mod

    monkeypatch.setattr(
        mongo_mod.MemoryMongoDBService, "get_connector", lambda self, cid: None, raising=False
    )

    assert runner._dispatch_transfer(sched.id, allow_paused=True) is None
    assert f"Connector {src.id} no longer exists" in parked["message"]
    assert "request" not in captured


def test_connector_from_another_workspace_is_not_used(stores, captured, monkeypatch):
    src = _connector("src", "other-tenant-db", workspace="ws-b")
    dst = _connector("dst", "dst.internal", workspace="ws-a")
    sched = _schedule(src, dst, workspace="ws-a")
    parked: dict = {}
    monkeypatch.setattr(
        runner, "_park_on_decision",
        lambda sid, exc, **kw: parked.update(message=str(exc)),
    )

    assert runner._dispatch_transfer(sched.id, allow_paused=True) is None
    assert "no longer exists" in parked["message"]
    assert "request" not in captured
