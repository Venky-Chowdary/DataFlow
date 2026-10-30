"""D187 — a plan logs in to each Snowflake endpoint once.

``plan_transfer`` / ``start_transfer`` read the destination from several steps
(two catalog reads, the connection probe, the privilege probe). Each opened its
own Snowflake session; on real Snowflake a login costs seconds, so the plan
outlasted the 45s tool deadline. Inside ``snowflake_plan_scope`` they share one
login (each consumer keeps its own cursor); outside it nothing is shared.
"""

from __future__ import annotations

import contextlib
import os
import uuid

import psycopg2
import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import connectors.snowflake_conn as sc  # noqa: E402

KW = dict(
    account="acct", username="u", password="p", database="D", schema="public",
    warehouse="W", connection_string="",
)


class _FakeConn:
    opened = 0

    def __init__(self) -> None:
        type(self).opened += 1
        self.closed = False
        self.cursors: list[object] = []

    def cursor(self):
        cur = object()
        self.cursors.append(cur)
        return cur

    def is_closed(self):
        return self.closed

    def close(self):
        self.closed = True


@pytest.fixture()
def fake_open(monkeypatch):
    _FakeConn.opened = 0
    made: list[_FakeConn] = []

    def open_(**kwargs):
        conn = _FakeConn()
        made.append(conn)
        return conn

    monkeypatch.setattr(sc, "_open_connection", open_)
    return made


def test_outside_a_scope_every_call_opens_and_owns_its_session(fake_open):
    a, b = sc.get_connection(**KW), sc.get_connection(**KW)
    assert a is not b and len(fake_open) == 2


def test_inside_a_scope_one_login_serves_every_consumer(fake_open):
    with sc.snowflake_plan_scope():
        a, b = sc.get_connection(**KW), sc.get_connection(**{**KW, "schema": "PUBLIC"})
        cur_a, cur_b = a.cursor(), b.cursor()
        a.close()  # a consumer finishing must not end the shared login
        c = sc.get_connection(**KW)
        assert c.cursor() is not cur_a and cur_a is not cur_b
        assert len(fake_open) == 1 and not fake_open[0].closed
        # another endpoint is another login
        sc.get_connection(**{**KW, "warehouse": "OTHER"})
        assert len(fake_open) == 2
    assert all(conn.closed for conn in fake_open)


def test_a_lost_session_is_reopened_once(fake_open):
    with sc.snowflake_plan_scope():
        conn = sc.get_connection(**KW)
        conn.cursor()
        fake_open[0].closed = True
        conn.cursor()
        conn.cursor()
        assert len(fake_open) == 2


PG = dict(host="127.0.0.1", port=5432, database="dataflow", username="dataflow", password="dataflow")


def _plan(monkeypatch, tmp_path, scoped: bool):
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import get_pilot_tools

    try:
        pg = psycopg2.connect(connect_timeout=3, dbname=PG["database"], user=PG["username"],
                              password=PG["password"], host=PG["host"], port=PG["port"])
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    pg.autocommit = True
    table = "d187_src_" + uuid.uuid4().hex[:8]
    with pg.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, code VARCHAR(5), amt NUMERIC(12,2))")
        cur.execute(f"INSERT INTO {table} VALUES (1,'ab',1.5),(2,NULL,2.0)")
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    logins = []
    real_open = sc._open_connection

    def counting(**kwargs):
        logins.append(1)
        return real_open(**kwargs)

    monkeypatch.setattr(sc, "_open_connection", counting)
    if not scoped:
        import services.endpoint_session as endpoint_session

        # The scope is now opened for every deadline-bound tool call.
        monkeypatch.setattr(sc, "snowflake_plan_scope", contextlib.nullcontext)
        monkeypatch.setattr(endpoint_session, "endpoint_session_scope", contextlib.nullcontext)
    src = create_connector({"name": "pg-" + table, "type": "postgresql", "schema": "public", **PG})
    dst = create_connector({
        "name": "sf-" + table, "type": "snowflake", "host": "localhost", "port": 443,
        "database": "dataflow", "username": "test", "password": "test", "schema": "public",
    })
    try:
        result = get_pilot_tools().execute("plan_transfer", {
            "source_connector_id": src.id, "source_table": table,
            "dest_connector_id": dst.id, "dest_table": "d187_dst_" + table[-8:],
            "sync_mode": "full_refresh_overwrite",
        })
    finally:
        delete_connector(src.id)
        delete_connector(dst.id)
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        pg.close()
    pre = ((result.output or {}).get("preflight") or {}) if result.success else {}
    gates = {g.get("id"): g.get("status") for g in pre.get("gates", []) if isinstance(g, dict)}
    return len(logins), result.success, pre.get("passed"), gates


def test_pg_to_snowflake_plan_logs_in_once_with_the_same_verdict(monkeypatch, tmp_path):
    pytest.importorskip("fakesnow")
    shared = _plan(monkeypatch, tmp_path, scoped=True)
    monkeypatch.undo()
    separate = _plan(monkeypatch, tmp_path, scoped=False)
    assert separate[0] >= 4, separate
    assert shared[0] == 1, shared
    assert shared[1:] == separate[1:]
