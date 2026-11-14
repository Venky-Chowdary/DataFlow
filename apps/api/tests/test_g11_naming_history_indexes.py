"""Security re-check F5: the naming-history indexes, built off the request path.

``naming_history_jobs`` created its indexes synchronously, unbounded, on the
first request that read history, and the indexes did not cover the
newest-first sort. Now ``ensure_naming_history_indexes`` builds
(workspace_id, naming.object, created_at desc) and (workspace_id,
destination_collection, created_at desc) with a server time limit, from the
API's startup in the background; the query path never builds an index.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

WANTED = [
    [("workspace_id", 1), ("naming.object", 1), ("created_at", -1)],
    [("workspace_id", 1), ("destination_collection", 1), ("created_at", -1)],
]


@pytest.fixture
def live():
    pymongo = pytest.importorskip("pymongo")
    from services.mongodb_service import MongoDBService

    uri = (os.environ.get("DATAFLOW_TEST_MONGO_URI") or os.environ.get("MONGO_URI")
           or "mongodb://127.0.0.1:27017")
    try:
        pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000).admin.command("ping")
    except Exception as exc:  # noqa: BLE001 - no server, no test
        pytest.skip(f"MongoDB not reachable: {exc}")
    svc = MongoDBService(uri)
    svc.db_name = f"g11_naming_idx_{uuid.uuid4().hex[:8]}"
    svc.connect()
    try:
        yield svc
    finally:
        svc.client.drop_database(svc.db_name)


def test_the_query_path_builds_no_index(live):
    jobs = live.get_database()["transfer_jobs"]
    jobs.insert_one({"workspace_id": "wsA", "destination_collection": "c", "status": "completed",
                     "records_processed": 1, "created_at": datetime.now(timezone.utc)})
    live.naming_history_jobs("c", workspace_id="wsA")
    keys = [list(ix["key"].items()) for ix in jobs.list_indexes()]
    assert keys == [[("_id", 1)]], keys


def test_startup_builds_the_sorted_indexes_and_the_query_uses_them(live):
    from services.naming_history import ensure_naming_history_indexes

    jobs = live.get_database()["transfer_jobs"]
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    jobs.insert_many([{"workspace_id": "wsA", "destination_collection": f"c{i % 7}",
                       "status": "completed", "records_processed": 1,
                       "created_at": t0 + timedelta(minutes=i)} for i in range(300)])
    assert ensure_naming_history_indexes(live) is True
    keys = [[(k, int(v)) for k, v in ix["key"].items()] for ix in jobs.list_indexes()]
    for wanted in WANTED:
        assert wanted in keys, keys
    rows = live.naming_history_jobs("c3", workspace_id="wsA", limit=5)
    assert [r["created_at"] for r in rows] == sorted((r["created_at"] for r in rows),
                                                     reverse=True)
    from services.workspace_filter import WorkspaceFilter

    plan = jobs.find({"$and": [WorkspaceFilter.only("wsA").mongo_query(),
                               {"destination_collection": "c3"}]}).sort(
        [("created_at", -1)]).explain()
    text = str(plan.get("queryPlanner", {}).get("winningPlan"))
    assert "IXSCAN" in text and "'stage': 'SORT'" not in text, text


def test_startup_schedules_the_index_build_in_the_background():
    from pathlib import Path

    main = (Path(__file__).resolve().parents[1] / "src" / "main.py").read_text()
    assert "start_in_background(ensure_naming_history_indexes)" in main
