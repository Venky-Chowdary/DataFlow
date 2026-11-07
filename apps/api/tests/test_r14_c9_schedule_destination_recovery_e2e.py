"""QA C9: a scheduled append whose destination was dropped is recoverable, and says so.

QA: dropping a scheduled destination produced "destination could not be
re-read … rebaseline" on every beat; the schedule stayed enabled with no next
run time and nothing on the schedule offered a way out. The route-level
rebaseline moved the baseline but left the schedule parked.

Live PostgreSQL -> PostgreSQL ``incremental_append`` schedule through the
routes a user drives (REST ``POST /api/v1/schedules/``, scheduler beats):

* drop the destination -> the first beat parks on ``DESTINATION_CONTINUITY``:
  ``state=stalled``, the reason names Recover destination, ``next_run_at`` is
  null only while stalled;
* REST ``POST /schedules/{id}/recover {"action": "rebuild"}`` reloads every
  source row, re-arms, and later inserts append once;
* drop again -> MCP ``recover_schedule_destination`` (rebaseline) -> Confirm
  re-arms; the next beat starts a new verified history from the cursor (the
  D100 cursor gate reads the audited acceptance instead of refusing);
* drop again -> the route-level MCP ``rebaseline_transfer`` releases the
  parked schedule too.

Values cover NULL, '', whitespace and unicode.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r12_mongo_incremental_schedule_e2e import _beat, _view

pytestmark = pytest.mark.timeout(400)

_NAMES = {1: "Ada", 2: None, 3: "", 4: "   ", 5: "Zoë ✓", 6: "日本語"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _dest(table: str) -> dict[int, object]:
    rows = h.query("postgresql", f'SELECT id, name FROM public."{table}" ORDER BY id')  # nosec B608
    ids = [int(r[0]) for r in rows]
    assert len(ids) == len(set(ids)), ("duplicate keys", ids)
    return {int(r[0]): r[1] for r in rows}


def _completed(run, label):
    assert run["started"] == 1, (label, run["schedule"].approval_request)
    assert run["job"].get("status") == "completed", (label, run["job"].get("error"))


def _assert_parked_on_destination(client, sid: str, label: str) -> dict:
    import time

    # The run record is finalized (claim released) a moment before the finding
    # is written; wait for the park itself, bounded.
    deadline = time.monotonic() + 5
    while True:
        view = _view(client, sid)
        req = view.get("approval_request") or {}
        if req.get("status") == "open" or time.monotonic() > deadline:
            break
        time.sleep(0.2)
    assert view["enabled"] is True, (label, view)
    assert req.get("status") == "open" and req.get("code") == "DESTINATION_CONTINUITY", (label, req)
    assert view["state"] == "stalled", (label, view["state"])
    assert "Recover the destination" in view["stalled_reason"], (label, view["stalled_reason"])
    assert f"/api/v1/schedules/{sid}/recover" in view["stalled_reason"], view["stalled_reason"]
    assert view["next_run_at"] is None, (label, view["next_run_at"])
    return req


def _drop_and_park(client, cur, sid: str, dest_table: str, label: str) -> dict:
    """Drop the destination; the schedule parks on it within two beats.

    A catalog that proves the table is gone parks on the first beat. One that
    does not answer in time is "could not be re-read" — retried once (a blip is
    not a drop, D266), and the identical second verdict parks.
    """
    cur.execute(f'DROP TABLE public."{dest_table}"')  # nosec B608
    errors = []
    for _attempt in (1, 2):
        run = _beat(sid)
        if run["started"]:
            assert run["job"].get("status") == "failed", (label, run["job"].get("status"))
            errors.append(str(run["job"].get("error") or "")[:160])
        try:
            return _assert_parked_on_destination(client, sid, label)
        except AssertionError:
            if _attempt == 2:
                raise AssertionError((label, errors))
    raise AssertionError(label)


def _assert_firing(client, sid: str, label: str) -> None:
    view = _view(client, sid)
    assert view["state"] == "scheduled", (label, view["state"], view["stalled_reason"])
    assert view["next_run_at"], (label, view)


def test_dropped_destination_parks_once_and_each_recovery_resumes(client):
    from services.connector_store import create_connector

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table, dest_table = f"c9_src_{token}", f"c9_dst_{token}"
    try:
        cur.execute(f"CREATE TABLE {src_table} (id BIGINT PRIMARY KEY, name TEXT, seq BIGINT)")  # nosec B608
        cur.executemany(f"INSERT INTO {src_table} VALUES (%s, %s, %s)",  # nosec B608
                        [(i, n, i) for i, n in _NAMES.items()])
        src = create_connector({"name": f"c9-src-{token}", "type": "postgresql", "role": "source",
                                "workspace_id": "", "ssl": False, **h.PG, "schema": "public"})
        dst = create_connector({"name": f"c9-dst-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
        resp = client.post("/api/v1/schedules/", json={
            "name": f"c9 {token}", "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest_table, "interval": "hourly",
            "sync_mode": "incremental_append", "cursor_column": "seq",
            "cursor_semantics": "monotonic_sequence", "primary_key": "id",
            "mappings": [{"source": c, "target": c, "confidence": 0.99} for c in ("id", "name", "seq")],
            "enabled": True,
        })
        assert resp.status_code == 201, resp.text
        sid = resp.json()["id"]
        _completed(_beat(sid), "first load")
        assert _dest(dest_table) == _NAMES

        # 1. Dropped: the schedule parks with the recovery named.
        _drop_and_park(client, cur, sid, dest_table, "dropped")
        assert _beat(sid)["started"] == 0  # parked: no beat grinds the same refusal

        refused = client.post(f"/api/v1/schedules/{sid}/recover",
                              json={"action": "repair", "reason": "x"},
                              headers={"X-Actor": "qa@example.com"})
        assert refused.status_code == 409, refused.text

        rebuilt = client.post(f"/api/v1/schedules/{sid}/recover",
                              json={"action": "rebuild", "reason": "table dropped by mistake"},
                              headers={"X-Actor": "qa@example.com"})
        assert rebuilt.status_code == 200, rebuilt.text
        body = rebuilt.json()
        assert body["job_id"], body
        job = h.wait_job(body["job_id"])
        assert job.get("status") == "completed", job.get("error")
        _beat_settle(sid)
        assert _dest(dest_table) == _NAMES
        _assert_firing(client, sid, "after rebuild")

        cur.execute(f"INSERT INTO {src_table} VALUES (7, 'new ✓', 7)")  # nosec B608
        _completed(_beat(sid), "append after rebuild")
        assert _dest(dest_table) == {**_NAMES, 7: "new ✓"}
        _assert_firing(client, sid, "after append")

        # 2. Dropped again: MCP recover (rebaseline = accept the removal).
        _drop_and_park(client, cur, sid, dest_table, "dropped again")
        ok, staged = h.mcp_call(client, "recover_schedule_destination", {
            "schedule_id": sid, "action": "rebaseline", "reason": "accept the removal"})
        assert ok, staged
        confirmed = client.post("/api/v1/copilot/confirm",
                                json={"ack_id": staged["ack_id"], "reason": "c9"},
                                headers={"X-Actor": "qa@example.com"})
        assert confirmed.status_code == 200, confirmed.text
        assert confirmed.json().get("ok") is True, confirmed.json()
        _assert_firing(client, sid, "after MCP rebaseline")
        cur.execute(f"INSERT INTO {src_table} VALUES (8, '', 8)")  # nosec B608
        _completed(_beat(sid), "append after accepted removal")
        # The read is inclusive at the watermark (late ties must load), and the
        # new table does not hold the boundary row, so it lands once with 8.
        assert _dest(dest_table) == {7: "new ✓", 8: ""}

        # 3. Dropped a third time: the route-level rebaseline releases it too.
        _drop_and_park(client, cur, sid, dest_table, "dropped a third time")
        ok, staged = h.mcp_call(client, "rebaseline_transfer", {
            "connector_id": dst.id, "table": dest_table, "reason": "accept the removal again"})
        assert ok, staged
        confirmed = client.post("/api/v1/copilot/confirm",
                                json={"ack_id": staged["ack_id"], "reason": "c9"})
        assert confirmed.status_code == 200, confirmed.text
        assert sid in (confirmed.json().get("released_schedules") or []), confirmed.json()
        _assert_firing(client, sid, "after route rebaseline")
        cur.execute(f"INSERT INTO {src_table} VALUES (9, '   ', 9)")  # nosec B608
        _completed(_beat(sid), "append after route rebaseline")
        assert _dest(dest_table) == {8: "", 9: "   "}
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        h.drop_table("postgresql", dest_table)


def _beat_settle(sid: str) -> None:
    """Record a Run-now job's completion the way the next beat does."""
    import time

    from services import schedule_runner
    from services.schedule_store import get_schedule

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if not str(getattr(get_schedule(sid), "running_job_id", "") or "").strip():
            return
        schedule_runner._finalize_finished_schedule_claims()
        time.sleep(0.2)
