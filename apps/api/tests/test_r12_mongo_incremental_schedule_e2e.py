"""QA r12 MongoDB D4 / D7 / D9: an incremental_append schedule, through the real runner.

QA (build 8f7d246d, PG -> Mongo, incremental_append on a cadence):

* D7 — every run re-appended the row holding the max cursor value (it was in
  the collection three times). The run seed is inclusive on purpose
  (``cursor >= watermark``, rounds 5/10: a row that arrives later sharing the
  max cursor value must still load), and the already-delivered tie peers are
  dropped against the destination. Against Mongo that lookup asked
  ``updated_at $in ['2024-01-01T07:00:00']`` — text — while the collection
  holds a BSON date, so it matched nothing, failed open, and the peer was
  written again on every run.
* D4 — with a unique key on the collection the same re-read peers were
  swallowed as duplicate keys, the delta check read "0 appended, 7 expected",
  and the schedule parked for approval with ``next_run_at=null``.
* D9 — a parked schedule stayed parked after a PATCH that fixed its mappings
  (enabled=true, next_run_at=null) until it was disabled and re-enabled.

This drives the schedule a user drives: ``POST /api/v1/schedules/`` with saved
connectors, scheduler beats (``_run_due_schedules``) on live PostgreSQL ->
MongoDB and PostgreSQL -> PostgreSQL. First load, two no-op beats, then a row
that ties the max cursor plus a newer one, then another no-op: exact counts, no
duplicate keys, NULL / '' / whitespace / unicode kept, and the schedule still
firing (``next_run_at`` set, ``state=scheduled``). Then the parked -> PATCH ->
auto-resumed path, with an explicit ``state=stalled`` while parked.
"""

from __future__ import annotations

import datetime as dt
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(400)

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")

_NAMES = {1: "Ada", 2: None, 3: "", 4: "   ", 5: "Zoë ✓", 6: "日本語", 7: "max-a"}
_BASE = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)


def _mongo():
    pymongo = pytest.importorskip("pymongo")
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MongoDB not reachable: {exc}")
    return client


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _beat(schedule_id: str) -> dict:
    from services import schedule_runner
    from services.schedule_store import get_schedule, update_schedule

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    update_schedule(schedule_id, {"next_run_at": past, "retry_at": None})
    started = schedule_runner._run_due_schedules()
    sched = get_schedule(schedule_id)
    job_id = str(getattr(sched, "running_job_id", "") or getattr(sched, "last_job_id", "") or "")
    if not started:
        return {"started": 0, "schedule": sched, "job": {}}
    job = h.wait_job(job_id)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if not str(getattr(get_schedule(schedule_id), "running_job_id", "") or "").strip():
            break
        schedule_runner._finalize_finished_schedule_claims()
        time.sleep(0.2)
    return {"started": started, "job": job, "schedule": get_schedule(schedule_id)}


def _completed(run: dict, label: str) -> dict:
    sched = run["schedule"]
    assert run["started"] == 1, (label, getattr(sched, "last_status", None),
                                 getattr(sched, "approval_request", None))
    job = run["job"]
    assert str(job.get("status")) == "completed", (label, job.get("status"),
                                                   job.get("error") or job.get("message"))
    return job


def _view(client, schedule_id: str) -> dict:
    resp = client.get(f"/api/v1/schedules/{schedule_id}")
    assert resp.status_code == 200, resp.text
    return resp.json()


def _assert_firing(client, schedule_id: str, label: str) -> None:
    view = _view(client, schedule_id)
    assert view["enabled"] is True, (label, view)
    assert view["next_run_at"], (label, view["next_run_at"], view.get("approval_request"))
    assert view["state"] == "scheduled", (label, view["state"], view["stalled_reason"])
    assert not view.get("approval_request") or view["approval_request"].get("status") != "open", (
        label, view["approval_request"])


class _Dest:
    """Reads the destination the schedule writes, whichever engine it is."""

    def __init__(self, kind: str, table: str, mongo_db: str = ""):
        self.kind, self.table, self.mongo_db = kind, table, mongo_db

    def rows(self) -> list[tuple[int, object]]:
        if self.kind == "mongodb":
            coll = _mongo()[self.mongo_db][self.table]
            return sorted((int(d["id"]), d.get("name")) for d in coll.find({}))
        got = h.query("postgresql", f'SELECT id, name FROM public."{self.table}"')  # nosec B608
        return sorted((int(r[0]), r[1]) for r in got)

    def assert_exact(self, expected: dict[int, object], label: str) -> None:
        rows = self.rows()
        ids = [i for i, _n in rows]
        assert len(ids) == len(set(ids)), (label, "duplicate keys", ids)
        assert ids == sorted(expected), (label, ids)
        assert dict(rows) == expected, (label, dict(rows))


def _seed(cur, table: str) -> None:
    cur.execute(
        f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT, "  # nosec B608
        "seq BIGINT, updated_at TIMESTAMPTZ NOT NULL)"
    )
    # Rows 6 and 7 already share the max cursor value on the first load.
    hours = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 7, 7: 7}
    cur.executemany(
        f"INSERT INTO {table} VALUES (%s, %s, %s, %s)",  # nosec B608
        [(i, _NAMES[i], i, _BASE + dt.timedelta(hours=hours[i])) for i in _NAMES],
    )


@pytest.mark.parametrize("dest_kind", ["mongodb", "postgresql"])
def test_incremental_append_schedule_noop_ties_and_plan_change_resume(client, dest_kind):
    from services.connector_store import create_connector

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table = f"r12m_src_{token}"
    dest_table = f"r12m_dst_{token}"
    mongo_db = f"df_r12m_{token}"
    if dest_kind == "mongodb":
        mongo = _mongo()
        dst_cfg = {"type": "mongodb", "connection_string": MONGO_URI, "database": mongo_db}
    else:
        dst_cfg = h.dest_config("postgresql")
    dest = _Dest(dest_kind, dest_table, mongo_db)
    try:
        _seed(cur, src_table)
        src = create_connector({"name": f"r12m-src-{token}", "type": "postgresql",
                                "role": "source", "workspace_id": "", "ssl": False,
                                **h.PG, "schema": "public"})
        dst = create_connector({"name": f"r12m-dst-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **dst_cfg})
        mappings = [{"source": c, "target": c, "confidence": 0.99}
                    for c in ("id", "name", "seq", "updated_at")]
        resp = client.post("/api/v1/schedules/", json={
            "name": f"r12m {dest_kind} {token}",
            "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest_table,
            "interval": "hourly", "timezone": "UTC",
            "sync_mode": "incremental_append",
            "cursor_column": "updated_at", "cursor_semantics": "monotonic_sequence",
            "primary_key": "id",
            "mappings": mappings, "enabled": True,
        })
        assert resp.status_code == 201, resp.text
        schedule_id = resp.json()["id"]

        expected = dict(_NAMES)
        _completed(_beat(schedule_id), "first load")
        dest.assert_exact(expected, "first load")
        if dest_kind == "mongodb":
            # D4's shape: a unique key on the collection turned re-read peers
            # into swallowed duplicate keys and a failed delta check.
            mongo[mongo_db][dest_table].create_index("id", unique=True)

        for n in (1, 2):
            job = _completed(_beat(schedule_id), f"no-op {n}")
            assert int(job.get("records_processed") or 0) == 0, (n, job.get("records_processed"))
            dest.assert_exact(expected, f"no-op {n}")
            _assert_firing(client, schedule_id, f"after no-op {n}")

        # A row arriving later on the max cursor value must still load, once.
        cur.execute(f"INSERT INTO {src_table} VALUES (8, 'tie ü', 8, %s), (9, NULL, 9, %s)",  # nosec B608
                    (_BASE + dt.timedelta(hours=7), _BASE + dt.timedelta(hours=9)))
        expected.update({8: "tie ü", 9: None})
        job = _completed(_beat(schedule_id), "tie + new")
        assert int(job.get("records_processed") or 0) == 2, job.get("records_processed")
        dest.assert_exact(expected, "tie + new")
        _completed(_beat(schedule_id), "no-op 3")
        dest.assert_exact(expected, "no-op 3")
        _assert_firing(client, schedule_id, "after no-op 3")

        # D9 — a mapping edit that leaves a source column unaccounted for:
        # the next beat is refused and parks the schedule on a finding.
        broken = [m for m in mappings if m["source"] != "seq"]
        resp = client.patch(f"/api/v1/schedules/{schedule_id}", json={"mappings": broken})
        assert resp.status_code == 200, resp.text
        refused = _beat(schedule_id)
        assert refused["started"] == 1 and str(refused["job"].get("status")) != "completed", refused
        parked = _view(client, schedule_id)
        assert parked["enabled"] is True and parked["next_run_at"] is None, parked
        assert parked["approval_request"]["status"] == "open", parked["approval_request"]
        assert parked["state"] == "stalled", parked["state"]
        assert "seq" in parked["stalled_reason"], parked["stalled_reason"]
        assert _beat(schedule_id)["started"] == 0
        dest.assert_exact(expected, "while parked")

        # Re-sending the same mappings is not a plan change: still parked.
        same = client.patch(f"/api/v1/schedules/{schedule_id}", json={"mappings": broken})
        assert same.status_code == 200, same.text
        assert same.json()["state"] == "stalled", same.json()["state"]
        assert same.json()["next_run_at"] is None

        # The PATCH that fixes the mapping re-arms it — no disable/enable toggle.
        resp = client.patch(f"/api/v1/schedules/{schedule_id}", json={"mappings": mappings})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["enabled"] is True
        assert body["state"] == "scheduled", (body["state"], body["stalled_reason"])
        assert body["next_run_at"], body
        assert body["approval_request"]["status"] == "approved", body["approval_request"]
        _assert_firing(client, schedule_id, "after the resolving PATCH")

        cur.execute(f"INSERT INTO {src_table} VALUES (10, '', 10, %s)",  # nosec B608
                    (_BASE + dt.timedelta(hours=10),))
        expected[10] = ""
        _completed(_beat(schedule_id), "after resume")
        dest.assert_exact(expected, "after resume")
        _assert_firing(client, schedule_id, "after resume")
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        if dest_kind == "mongodb":
            _mongo().drop_database(mongo_db)
        else:
            h.drop_table("postgresql", dest_table)
        pg.close()
