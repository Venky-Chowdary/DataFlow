"""QA C9 review: a route rebaseline releases only the caller's own schedules.

03b263d9d's release_continuity_parks walked every workspace's schedules and
matched the shared ``host:<h>:<p>|db|schema|table`` alt key, so workspace A's
rebaseline re-armed workspace B's parked schedule (A's actor in B's audit),
and the accepted-reset marker under that shared key let B's next append past
its D100 gate. Live PostgreSQL: two workspaces, two connectors to the same
physical table, both parked; A rebaselines; B stays parked and unaccepted.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r12_mongo_incremental_schedule_e2e import _beat

pytestmark = pytest.mark.timeout(300)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _wait_parked(sid: str):
    import time

    from services.schedule_store import get_schedule, has_open_approval

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if has_open_approval(get_schedule(sid)):
            return get_schedule(sid)
        time.sleep(0.2)
    return get_schedule(sid)


def _park(sid: str) -> None:
    from services.schedule_store import has_open_approval

    for _attempt in (1, 2):  # an unreadable catalog is retried once (D266)
        _beat(sid)
        if has_open_approval(_wait_parked(sid)):
            return
    raise AssertionError(f"{sid} did not park")


def test_workspace_a_rebaseline_leaves_workspace_b_parked(client):
    from services import append_continuity as ac
    from services.connector_store import create_connector
    from services.schedule_recovery import _destination_endpoint
    from services.schedule_store import create_schedule, get_schedule, has_open_approval

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_a, src_b, dest = f"c9w_a_{token}", f"c9w_b_{token}", f"c9w_dst_{token}"
    try:
        for table, ids in ((src_a, (1, 2, 3)), (src_b, (11, 12))):
            cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT, seq BIGINT)")  # nosec B608
            cur.executemany(f"INSERT INTO {table} VALUES (%s, %s, %s)",  # nosec B608
                            [(i, ("Ada", "", "Zoë ✓")[i % 3], i) for i in ids])
        scheds = {}
        for ws, src_table in (("ws-a", src_a), ("ws-b", src_b)):
            src = create_connector({"name": f"c9w-src-{ws}-{token}", "type": "postgresql",
                                    "role": "source", "workspace_id": ws, "ssl": False,
                                    **h.PG, "schema": "public"})
            dst = create_connector({"name": f"c9w-dst-{ws}-{token}", "role": "destination",
                                    "workspace_id": ws, "ssl": False,
                                    **h.dest_config("postgresql")})
            scheds[ws] = create_schedule({
                "name": f"c9w {ws} {token}", "workspace_id": ws,
                "source_connector_id": src.id, "source_table": src_table,
                "dest_connector_id": dst.id, "dest_table": dest, "interval": "hourly",
                "sync_mode": "incremental_append", "cursor_column": "seq",
                "cursor_semantics": "monotonic_sequence", "primary_key": "id",
                "mappings": [{"source": c, "target": c, "confidence": 0.99}
                             for c in ("id", "name", "seq")],
                "enabled": True,
            }).id
        for ws in ("ws-a", "ws-b"):
            run = _beat(scheds[ws])
            assert run["job"].get("status") == "completed", (ws, run["job"].get("error"))

        cur.execute(f'DROP TABLE public."{dest}"')  # nosec B608
        for ws in ("ws-a", "ws-b"):
            _park(scheds[ws])

        endpoint_a = _destination_endpoint(get_schedule(scheds["ws-a"]))
        out = ac.rebaseline(endpoint_a, actor="a-admin@example.com",
                            reason="workspace A dropped its copy", workspace_id="ws-a")
        assert out["released_schedules"] == [scheds["ws-a"]], out

        sched_b = get_schedule(scheds["ws-b"])
        assert has_open_approval(sched_b), sched_b.approval_request
        assert not str((sched_b.approval_request or {}).get("resolved_by") or "")
        # B's own route still refuses: A's acceptance is not B's.
        endpoint_b = _destination_endpoint(sched_b)
        assert ac.destination_reset_accepted(endpoint_a) is True
        assert ac.destination_reset_accepted(endpoint_b) is False
        assert not has_open_approval(get_schedule(scheds["ws-a"]))

        # No workspace named: nothing is released.
        assert ac.rebaseline(endpoint_b, actor="x@example.com", reason="probe",
                             release_schedules=True)["released_schedules"] == []
        assert has_open_approval(get_schedule(scheds["ws-b"]))
    finally:
        for table in (src_a, src_b):
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        h.drop_table("postgresql", dest)
