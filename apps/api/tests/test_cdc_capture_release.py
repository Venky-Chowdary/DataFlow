"""Schedule delete releases a PostgreSQL CDC slot/publication — with guards.

Live half runs against localhost PostgreSQL (wal_level=logical); the guard
half uses fakes so it runs everywhere.
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import psycopg2
import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.postgresql_change_stream import (
    _publication_name,
    _slot_name,
    release_pg_capture,
)
from connectors.postgresql_conn import get_connection
from services import cdc_capture_release as mod
from services.cdc_multi_table import shared_route_cursor_key
from services.schedule_store import PipelineSchedule

CFG = {
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}


def _logical_ready() -> bool:
    try:
        socket.create_connection(("localhost", 5432), timeout=1).close()
        with get_connection(**CFG) as conn, conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            row = cur.fetchone()
            return bool(row) and row[0] == "logical"
    except (OSError, psycopg2.Error):
        return False


live = pytest.mark.skipif(not _logical_ready(), reason="no logical PostgreSQL on :5432")


def _sched(**over) -> PipelineSchedule:
    base = dict(
        id="s1",
        name="s1",
        source_connector_id="src",
        source_table="orders",
        dest_connector_id="dst",
        dest_table="orders",
        interval="hourly",
        sync_mode="cdc",
        last_job_id="job1",
        workspace_id="ws",
    )
    base.update(over)
    return PipelineSchedule(**base)


def test_shared_route_key_is_job_independent() -> None:
    a = shared_route_cursor_key(
        engine="postgresql", database="app", tables=["orders", "users"],
        dest_type="mysql", dest_database="dw",
    )
    b = shared_route_cursor_key(
        engine="postgresql", database="app", tables=["users", "orders"],
        dest_type="mysql", dest_database="dw",
    )
    c = shared_route_cursor_key(
        engine="postgresql", database="app", tables=["orders", "users"],
        dest_type="mysql", dest_database="other",
    )
    assert a == b  # table order is irrelevant; the run id never enters the key
    assert a != c  # a different destination is a different capture route


def test_non_cdc_and_never_ran_are_noops() -> None:
    assert mod.release_schedule_cdc_capture(_sched(sync_mode="incremental")) == {
        "released": False,
        "reason": "not_cdc",
    }
    assert mod.release_schedule_cdc_capture(_sched(last_job_id=None))["reason"] == "never_ran"


def test_route_shared_by_another_cdc_schedule_keeps_slot(monkeypatch) -> None:
    twin = _sched(id="s2")
    monkeypatch.setattr(mod, "list_schedules", lambda: [_sched(), twin])
    out = mod.release_schedule_cdc_capture(_sched())
    assert out == {"released": False, "reason": "route_shared"}

    other_ws = _sched(id="s3", workspace_id="tenant-b")
    monkeypatch.setattr(mod, "list_schedules", lambda: [_sched(), other_ws])
    non_cdc = _sched(id="s4", sync_mode="full_refresh_append")
    assert mod.route_still_in_use(_sched()) is False
    monkeypatch.setattr(mod, "list_schedules", lambda: [_sched(), non_cdc])
    assert mod.route_still_in_use(_sched()) is False


class _Connector:
    type = "postgresql"

    def to_dict(self):
        return dict(CFG)


def _wire(monkeypatch, *, job: dict, release):
    import services.connector_store as cs
    import services.mongodb_service as ms
    import connectors.postgresql_change_stream as pcs

    class _Svc:
        def get_job(self, _id):
            return job

    monkeypatch.setattr(ms, "get_mongodb_service", lambda: _Svc())
    monkeypatch.setattr(cs, "get_connector", lambda _id, _ws=None: _Connector())
    monkeypatch.setattr(pcs, "release_pg_capture", release)
    monkeypatch.setattr(mod, "list_schedules", lambda: [_sched()])
    cleared: list[str] = []
    monkeypatch.setattr(
        mod, "clear_watermark", lambda k: cleared.append(k) or {"cleared": True}
    )
    return cleared


def test_active_slot_is_never_dropped_and_watermark_kept(monkeypatch) -> None:
    cleared = _wire(
        monkeypatch,
        job={"cursor_key": "k", "cursor_value": "slot=df_x|phase=streaming|lsn=0/1"},
        release=lambda cfg, *, slot_name, publication_name: {
            "slot_name": slot_name,
            "publication_name": publication_name,
            "slot": "active",
            "publication": "absent",
        },
    )
    out = mod.release_schedule_cdc_capture(_sched())
    assert out["released"] is False
    assert out["reason"] == "slot_active"
    assert out["slot_name"] == "df_x"  # slot identity comes from the run's token
    assert cleared == []


def test_unreachable_source_reports_manual_next_action(monkeypatch) -> None:
    def boom(cfg, *, slot_name, publication_name):
        raise psycopg2.OperationalError("connection refused")

    cleared = _wire(
        monkeypatch,
        job={"cursor_key": "k", "cursor_value": "slot=df_x|phase=streaming|lsn=0/1"},
        release=boom,
    )
    out = mod.release_schedule_cdc_capture(_sched())
    assert out["reason"] == "source_unreachable"
    assert "pg_drop_replication_slot('df_x')" in out["next_action"]
    assert cleared == []


def test_release_drops_slot_publication_and_watermark(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def rel(cfg, *, slot_name, publication_name):
        calls.append((slot_name, publication_name))
        return {"slot": "dropped", "publication": "dropped"}

    cleared = _wire(monkeypatch, job={"cursor_key": "route-k", "cursor_value": ""}, release=rel)
    out = mod.release_schedule_cdc_capture(_sched())
    assert out["released"] is True and out["reason"] == "ok"
    assert cleared == ["route-k"]
    # No token on the job → the reader's default naming is what gets dropped.
    assert calls == [
        (
            _slot_name("dataflow", "orders", "route-k"),
            _publication_name("dataflow", "orders", "route-k"),
        )
    ]


@live
def test_live_release_is_idempotent_and_leaves_active_slots() -> None:
    suffix = uuid.uuid4().hex[:8]
    slot = f"df_test_release_{suffix}"
    pub = f"df_pub_test_release_{suffix}"
    table = f"df_rel_{suffix}"
    setup = get_connection(**CFG)
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(f'CREATE TABLE "{table}"(id int primary key)')
        cur.execute(f'CREATE PUBLICATION "{pub}" FOR TABLE "{table}"')
        cur.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", (slot,)
        )
    setup.close()
    try:
        first = release_pg_capture(CFG, slot_name=slot, publication_name=pub)
        assert first["slot"] == "dropped" and first["publication"] == "dropped"
        second = release_pg_capture(CFG, slot_name=slot, publication_name=pub)
        assert second["slot"] == "absent" and second["publication"] == "absent"
        with get_connection(**CFG) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", (slot,))
            assert cur.fetchone() is None
            cur.execute("SELECT 1 FROM pg_publication WHERE pubname=%s", (pub,))
            assert cur.fetchone() is None
    finally:
        with get_connection(**CFG) as conn, conn.cursor() as cur:
            conn.autocommit = True
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
