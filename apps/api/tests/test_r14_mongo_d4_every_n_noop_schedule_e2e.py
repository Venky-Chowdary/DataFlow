"""QA Mongo D4 guard: a no-change every-N incremental schedule into MongoDB.

QA (build 8f7d246d): PostgreSQL -> MongoDB ``incremental_append`` on an
every-N-minutes cadence; the no-change run failed "Append delta mismatch: 0
appended, 7 expected", went approval_pending with no next run, and later
inserts never loaded. 8c89f77d1 fixed the BSON-date tie lookup; this pins
QA's own shape on top of the r12 test: every row loaded in one statement, so
all 7 share one ``now()`` value (TIMESTAMPTZ(3): BSON keeps milliseconds) (the whole table is the tie set),
a unique index on the collection, an ``every_5m`` cadence, two no-change beats,
then later inserts — exact counts, no duplicates, and the schedule firing
(``state=scheduled``, ``next_run_at`` set) after every beat.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r12_mongo_incremental_schedule_e2e import (
    MONGO_URI,
    _assert_firing,
    _beat,
    _completed,
    _mongo,
)

pytestmark = pytest.mark.timeout(400)

_NAMES = {1: "Ada", 2: None, 3: "", 4: "   ", 5: "Zoë ✓", 6: "日本語", 7: "max"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _rows(coll) -> dict[int, object]:
    docs = list(coll.find({}))
    ids = [int(d["id"]) for d in docs]
    assert len(ids) == len(set(ids)), ("duplicate keys", sorted(ids))
    return {int(d["id"]): d.get("name") for d in docs}


def test_every_n_noop_runs_succeed_and_later_inserts_flow(client):
    from services.connector_store import create_connector

    mongo = _mongo()
    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table, dest, db = f"md4_src_{token}", f"md4_dst_{token}", f"df_md4_{token}"
    try:
        cur.execute(f"CREATE TABLE {src_table} (id BIGINT PRIMARY KEY, name TEXT, "  # nosec B608
                    "updated_at TIMESTAMPTZ(3) NOT NULL DEFAULT now())")
        # One statement: every row carries the same now() (milliseconds: BSON's precision).
        cur.execute(f"INSERT INTO {src_table} (id, name) VALUES "  # nosec B608
                    + ", ".join(["(%s, %s)"] * len(_NAMES)),
                    [v for item in _NAMES.items() for v in item])
        src = create_connector({"name": f"md4-src-{token}", "type": "postgresql", "role": "source",
                                "workspace_id": "", "ssl": False, **h.PG, "schema": "public"})
        dst = create_connector({"name": f"md4-dst-{token}", "role": "destination", "workspace_id": "",
                                "ssl": False, "type": "mongodb", "connection_string": MONGO_URI,
                                "database": db})
        resp = client.post("/api/v1/schedules/", json={
            "name": f"md4 {token}", "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest, "interval": "every_5m",
            "sync_mode": "incremental_append", "cursor_column": "updated_at",
            "cursor_semantics": "monotonic_sequence", "primary_key": "id",
            "mappings": [{"source": c, "target": c, "confidence": 0.99}
                         for c in ("id", "name", "updated_at")],
            "enabled": True,
        })
        assert resp.status_code == 201, resp.text
        sid = resp.json()["id"]
        coll = mongo[db][dest]
        _completed(_beat(sid), "first load")
        assert _rows(coll) == _NAMES
        coll.create_index("id", unique=True)
        _assert_firing(client, sid, "after first load")

        for n in (1, 2):
            job = _completed(_beat(sid), f"no-change {n}")
            assert int(job.get("records_processed") or 0) == 0, job.get("records_processed")
            assert _rows(coll) == _NAMES
            _assert_firing(client, sid, f"after no-change {n}")

        cur.execute(f"INSERT INTO {src_table} (id, name) VALUES (8, 'later'), (9, '')")  # nosec B608
        _completed(_beat(sid), "later inserts")
        assert _rows(coll) == {**_NAMES, 8: "later", 9: ""}
        _completed(_beat(sid), "no-change after inserts")
        assert _rows(coll) == {**_NAMES, 8: "later", 9: ""}
        _assert_firing(client, sid, "end")
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        mongo.drop_database(db)
