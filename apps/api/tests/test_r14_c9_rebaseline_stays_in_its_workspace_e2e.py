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


def test_workspace_a_rebaseline_does_not_move_workspace_b_baseline(client, monkeypatch):
    """Review 2: rebaseline() saved under every key, including the shared
    host:<h>:<p>|db|schema|table alt key, so A's rebaseline moved B's baseline."""
    import services.audit_log as audit_log
    from services import append_continuity as ac
    from services.connector_store import create_connector
    from src.transfer.models import EndpointConfig

    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: kw)
    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    table = f"c9b_{token}"
    try:
        cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)")  # nosec B608
        cur.executemany(f"INSERT INTO {table} VALUES (%s, %s)",  # nosec B608
                        [(1, "Ada"), (2, ""), (3, "Zoë ✓")])

        def _endpoint(ws):
            conn = create_connector({"name": f"c9b-{ws}-{token}", "role": "destination",
                                     "workspace_id": ws, "ssl": False,
                                     **h.dest_config("postgresql")})
            return EndpointConfig(kind="database", format="postgresql", connector_id=conn.id,
                                  table=table)

        endpoint_b = _endpoint("ws-b")
        target_b = ac.resolve_target(endpoint_b)
        assert target_b.alt_keys, "a connector endpoint carries the host alt key"
        host_key = target_b.alt_keys[0]
        ac.rebaseline(endpoint_b, actor="b@example.com", reason="B accepts 3 rows",
                      workspace_id="ws-b")
        b_id_before = ac.load_baseline(target_b.key)
        host_before = ac.load_baseline(host_key)
        assert b_id_before and host_before and host_before["rows"] == 3

        endpoint_a = _endpoint("ws-a")
        target_a = ac.resolve_target(endpoint_a)
        assert host_key in target_a.alt_keys  # the same physical table
        cur.execute(f"INSERT INTO {table} VALUES (4, '   ')")  # nosec B608
        ac.rebaseline(endpoint_a, actor="a@example.com", reason="A accepts 4 rows",
                      workspace_id="ws-a")
        assert ac.load_baseline(target_a.key)["rows"] == 4
        assert ac.load_baseline(target_b.key) == b_id_before
        assert ac.load_baseline(host_key) == host_before  # B's view did not move
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608


def test_a_rebaseline_reports_only_its_own_previous_baseline_and_names_the_shared_keys_it_left(
    client, monkeypatch,
):
    """Review 3: ``previous`` was read from every key (so A's result could
    report B's host-key baseline), and the shared host key left alone was not
    reported — runs keyed on the inline host keep the old baseline."""
    import services.audit_log as audit_log
    from services import append_continuity as ac
    from services.connector_store import create_connector
    from src.transfer.models import EndpointConfig

    events: list[dict] = []
    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: events.append(kw) or kw)
    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    table = f"c9p_{token}"
    try:
        cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)")  # nosec B608
        cur.executemany(f"INSERT INTO {table} VALUES (%s, %s)", [(1, "Ada"), (2, "")])  # nosec B608

        def _endpoint(ws):
            conn = create_connector({"name": f"c9p-{ws}-{token}", "role": "destination",
                                     "workspace_id": ws, "ssl": False,
                                     **h.dest_config("postgresql")})
            return EndpointConfig(kind="database", format="postgresql", connector_id=conn.id,
                                  table=table)

        endpoint_b = _endpoint("ws-b")
        ac.rebaseline(endpoint_b, actor="b@example.com", reason="B accepts 2", workspace_id="ws-b")
        host_key = ac.resolve_target(endpoint_b).alt_keys[0]
        endpoint_a = _endpoint("ws-a")
        out = ac.rebaseline(endpoint_a, actor="a@example.com", reason="A accepts 2",
                            workspace_id="ws-a")
        assert out["previous"] is None, out["previous"]  # A had none; B's is not A's
        assert out["skipped_shared_keys"] == [host_key], out
        details = events[-1]["details"]
        assert details["skipped_shared_keys"] == [host_key], details
        assert details["previous"] is None
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608


def test_an_unresolvable_foreign_connector_keeps_the_host_key_shared_and_is_logged(monkeypatch, caplog):
    """A connector in another workspace on the same host that cannot be resolved
    makes the host key undecidable. The rebaseline must leave it alone (fail safe)
    and say why in the log, not swallow the error silently in the loop."""
    import logging
    from types import SimpleNamespace

    from services import append_continuity as ac
    from services import connector_store

    mine = SimpleNamespace(id="c-a", workspace_id="ws-a", host="db.example", type="postgresql")
    theirs = SimpleNamespace(id="c-b", workspace_id="ws-b", host="DB.example", type="postgresql")
    monkeypatch.setattr(connector_store, "_load_all", lambda: [mine, theirs])

    def _unresolvable(_endpoint):
        raise RuntimeError("connector secret unavailable")

    monkeypatch.setattr(ac, "resolve_target", _unresolvable)
    target = ac.ContinuityTarget(
        db_type="postgresql", cfg={"host": "db.example"}, schema="public", table="orders",
        key="id:c-a", alt_keys=("host:db.example:5432",),
    )
    with caplog.at_level(logging.WARNING, logger=ac.logger.name):
        owned = ac._keys_this_rebaseline_owns(target, SimpleNamespace(connector_id="c-a"))
    assert owned == ("id:c-a",)
    assert any("c-b" in r.getMessage() and "shared" in r.getMessage() for r in caplog.records)
