"""Security re-check F2 / F3: naming history on a live MongoDB job store.

F2: ``naming_history_jobs`` matched ``{"workspace_id": ""}``, which on MongoDB
misses a job whose workspace_id is missing or null (how the oldest rows were
written, D218); the memory store treated them alike and hid it. Both stores
now use ``WorkspaceFilter.only(workspace).mongo_query()`` / ``.matches``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def stores():
    pymongo = pytest.importorskip("pymongo")
    from services.mongodb_service import MemoryMongoDBService, MongoDBService

    uri = (os.environ.get("DATAFLOW_TEST_MONGO_URI") or os.environ.get("MONGO_URI")
           or "mongodb://127.0.0.1:27017")
    try:
        pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000).admin.command("ping")
    except Exception as exc:  # noqa: BLE001 - no server, no test
        pytest.skip(f"MongoDB not reachable: {exc}")
    live = MongoDBService(uri)
    live.db_name = f"g11_naming_hist_{uuid.uuid4().hex[:8]}"
    live.connect()
    mem = MemoryMongoDBService()
    mem.connect()
    try:
        yield live, mem
    finally:
        live.client.drop_database(live.db_name)


def _add(stores, **job):
    live, mem = stores
    doc = {"destination_type": "mongodb", "status": "completed", "records_processed": 5,
           "created_at": T0, **job}
    live.get_database()["transfer_jobs"].insert_one(dict(doc))
    jid = uuid.uuid4().hex[:24]
    mem._jobs[jid] = {**doc, "_id": jid}


def _history(svc, monkeypatch, obj, endpoint=None):
    import services.mongodb_service as ms
    from services.naming_history import object_history

    monkeypatch.setattr(ms, "get_mongodb_service", lambda: svc)
    return object_history("mongodb", obj, {"host": "mongo-a", "port": 27017}, endpoint, "")


def test_unscoped_legacy_jobs_count_on_both_stores(stores, monkeypatch):
    tok = uuid.uuid4().hex[:8]
    dest = {"transfer_request": {"destination": {"host": "mongo-a", "port": 27017}}}
    _add(stores, destination_collection=f"c_missing_{tok}", **dest)
    _add(stores, destination_collection=f"c_null_{tok}", workspace_id=None, **dest)
    _add(stores, destination_collection=f"c_blank_{tok}", workspace_id="", **dest)
    _add(stores, destination_collection=f"c_other_{tok}", workspace_id="wsX", **dest)
    live, mem = stores
    for name in ("missing", "null", "blank"):
        obj = f"c_{name}_{tok}"
        assert (_history(live, monkeypatch, obj), _history(mem, monkeypatch, obj)) == (
            "legacy", "legacy"), name
    obj = f"c_other_{tok}"  # another workspace's job never counts here
    assert (_history(live, monkeypatch, obj), _history(mem, monkeypatch, obj)) == (
        "none", "none")
