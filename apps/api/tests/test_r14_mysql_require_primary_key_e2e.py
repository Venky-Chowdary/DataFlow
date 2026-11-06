"""QA MySQL D2: sql_require_primary_key=ON — Validate refuses a keyless append create-new.

QA (jobs 6ae6a429…, 6ae6a44d…): PostgreSQL -> MySQL with
``sql_require_primary_key=ON``, ``pk=id``, ``full_refresh_append`` and
``incremental_append``: preflight passed and the write failed with MySQL 3750.
Upsert and overwrite worked (they create the key).

DataFlow creates an append destination without a key on purpose (a second
copy of a row is legal on append). On a server that forces a PRIMARY KEY that
CREATE can never succeed, so Validate (G6) now reads the server variable and
refuses before anything is written, naming the variable and the fixes; upsert
and overwrite still create PRIMARY KEY (id) and run.

The live half sets ``SET GLOBAL sql_require_primary_key=ON`` (restored after)
and needs an account allowed to do so: ``DATAFLOW_TEST_MYSQL_ROOT_PASSWORD``
(CI's mysql container has root). The planner half drives the same route with
the server answer stubbed, so the rule is exercised without that privilege.
"""

from __future__ import annotations

import os
import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r14_mysql_latin1_cp1252_strict_e2e import _cfg, _connector, _pg_cfg


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _source_table() -> str:
    t = f"r14d2_{uuid.uuid4().hex[:8]}"
    h.execute("postgresql", f'CREATE TABLE "{t}" (id int PRIMARY KEY, name text, amt numeric(8,2))')
    h.execute("postgresql", f"INSERT INTO \"{t}\" VALUES (1, 'a', 1.50), (2, NULL, 0), "
                            f"(3, '', NULL), (4, ' 東京 ', 2.25)")
    return t


def _stage(client, t: str, mode: str):
    src = _connector("postgresql", _pg_cfg(), "source")
    dst = _connector("mysql", _cfg("mysql"), "destination")
    return h.mcp_call(client, "start_transfer", {
        "source_connector_id": src, "dest_connector_id": dst, "source_table": t,
        "dest_table": t, "sync_mode": mode, "primary_key": "id",
        "validation_mode": "strict",
        **({"cursor_column": "id"} if mode == "incremental_append" else {})})


def _mysql_exists(t: str) -> bool:
    return bool(h.query("mysql", "SELECT COUNT(*) FROM information_schema.tables WHERE "
                                 f"table_schema = DATABASE() AND table_name = '{t}'")[0][0])


@pytest.mark.parametrize("mode", ["full_refresh_append", "incremental_append"])
def test_append_create_new_is_refused_at_validate_naming_the_variable(client, monkeypatch, mode):
    from services import dest_require_primary_key as rpk

    monkeypatch.setattr(rpk, "server_requires_primary_key", lambda cfg: "sql_require_primary_key")
    t = _source_table()
    try:
        ok, out = _stage(client, t, mode)
        assert not ok, out
        text = str(out)
        assert "sql_require_primary_key=ON" in text and "upsert" in text, text
        assert not _mysql_exists(t)
    finally:
        h.drop_table("postgresql", t)
        h.drop_table("mysql", t)


@pytest.mark.parametrize("mode", ["upsert", "full_refresh_overwrite"])
def test_keyed_modes_are_not_refused(client, monkeypatch, mode):
    from services import dest_require_primary_key as rpk

    monkeypatch.setattr(rpk, "server_requires_primary_key", lambda cfg: "sql_require_primary_key")
    t = _source_table()
    try:
        ok, out = _stage(client, t, mode)
        assert ok and not h.blockers_of(out), out
    finally:
        h.drop_table("postgresql", t)
        h.drop_table("mysql", t)


def _root():
    pymysql = pytest.importorskip("pymysql")
    pw = os.environ.get("DATAFLOW_TEST_MYSQL_ROOT_PASSWORD", "")
    if not pw or not h.port_up(3306):
        pytest.skip("needs DATAFLOW_TEST_MYSQL_ROOT_PASSWORD to SET GLOBAL sql_require_primary_key")
    return pymysql.connect(host="127.0.0.1", port=3306, user="root", password=pw,
                           autocommit=True, connect_timeout=5)


def test_live_server_variable_on_refuses_append_and_lets_upsert_run(client):
    root = _root()
    t = _source_table()
    with root.cursor() as cur:
        cur.execute("SELECT @@GLOBAL.sql_require_primary_key")
        before = cur.fetchone()[0]
        cur.execute("SET GLOBAL sql_require_primary_key = ON")
    try:
        from services.dest_require_primary_key import server_requires_primary_key

        assert server_requires_primary_key(_cfg("mysql")) == "sql_require_primary_key"
        for mode in ("full_refresh_append", "incremental_append"):
            ok, out = _stage(client, t, mode)
            assert not ok and "sql_require_primary_key=ON" in str(out), out
            assert not _mysql_exists(t)
        ok, out = _stage(client, t, "upsert")
        assert ok, out
        job = h.confirm(client, out["ack_id"], reason="r14-d2-upsert")
        assert job.get("status") == "completed", job
        pk = h.query("mysql", "SELECT column_name FROM information_schema.key_column_usage "
                              f"WHERE table_schema = DATABASE() AND table_name = '{t}' "
                              "AND constraint_name = 'PRIMARY'")
        assert [r[0] for r in pk] == ["id"]
        assert h.query("mysql", f"SELECT COUNT(*) FROM `{t}`")[0][0] == 4
    finally:
        with root.cursor() as cur:
            cur.execute(f"SET GLOBAL sql_require_primary_key = {int(before)}")
        root.close()
        h.drop_table("postgresql", t)
        h.drop_table("mysql", t)


def test_probe_reads_off_on_this_server():
    from services.dest_require_primary_key import server_requires_primary_key

    if not h.port_up(3306):
        pytest.skip("MySQL not running")
    with h.mysql_conn().cursor() as cur:
        cur.execute("SELECT @@sql_require_primary_key")
        on = str(cur.fetchone()[0]) in {"1", "ON"}
    assert server_requires_primary_key(_cfg("mysql")) == ("sql_require_primary_key" if on else "")
    assert server_requires_primary_key(None) == ""
