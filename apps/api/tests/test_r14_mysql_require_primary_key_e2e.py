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

The live half sets ``SET GLOBAL sql_require_primary_key=ON`` (restored in a
finally block) on a PRIVATE MySQL only — never the shared server other suites
use: ``DATAFLOW_TEST_MYSQL_GLOBALS_HOST/_PORT/_USER/_PASSWORD/_ROOT_PASSWORD``
(``tests.r4_route_harness.globals_mysql_server``). The planner half drives the same route with
the server answer stubbed, so the rule is exercised without that privilege.
"""

from __future__ import annotations

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

    monkeypatch.setattr(rpk, "server_requires_primary_key",
                        lambda cfg: rpk.PrimaryKeyRequirement(variable="sql_require_primary_key"))
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

    monkeypatch.setattr(rpk, "server_requires_primary_key",
                        lambda cfg: rpk.PrimaryKeyRequirement(variable="sql_require_primary_key"))
    t = _source_table()
    try:
        ok, out = _stage(client, t, mode)
        assert ok and not h.blockers_of(out), out
    finally:
        h.drop_table("postgresql", t)
        h.drop_table("mysql", t)


# Server-global variables are flipped only on a PRIVATE MySQL server, never on
# the shared one other suites use: DATAFLOW_TEST_MYSQL_GLOBALS_HOST / _PORT /
# _USER / _PASSWORD (a DataFlow login) and _ROOT_PASSWORD (to SET GLOBAL).
def _globals_server() -> dict:
    return h.globals_mysql_server()


def _root():
    return h.globals_mysql_root()


def _gq(sql: str) -> list[tuple]:
    import pymysql

    srv = _globals_server()
    conn = pymysql.connect(host=srv["host"], port=srv["port"], user=srv["username"],
                           password=srv["password"], database=srv["database"], autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall())
    finally:
        conn.close()


def _g_exists(t: str) -> bool:
    return bool(_gq("SELECT COUNT(*) FROM information_schema.tables WHERE "
                    f"table_schema = DATABASE() AND table_name = '{t}'")[0][0])


def _g_pk(t: str) -> list[str]:
    return [r[0] for r in _gq("SELECT column_name FROM information_schema.key_column_usage "
                              f"WHERE table_schema = DATABASE() AND table_name = '{t}' "
                              "AND constraint_name = 'PRIMARY'")]


def test_live_server_variable_on_refuses_append_and_lets_upsert_run(client, require_pk_on):
    srv = _globals_server()
    t = _source_table()
    try:
        from services.dest_require_primary_key import server_requires_primary_key

        assert server_requires_primary_key(srv).variable == "sql_require_primary_key"
        for mode in ("full_refresh_append", "incremental_append"):
            ok, out = _stage_args(client, t, mode, dest_cfg=srv)
            assert not ok and "sql_require_primary_key=ON" in str(out), out
            assert not _g_exists(f"{t}_d")
        ok, out = _stage_args(client, t, "upsert", dest_cfg=srv)
        assert ok, out
        job = h.confirm(client, out["ack_id"], reason="r14-d2-upsert")
        assert job.get("status") == "completed", job
        assert _g_pk(f"{t}_d") == ["id"]
        assert _gq(f"SELECT COUNT(*) FROM `{t}_d`")[0][0] == 4
    finally:
        h.drop_table("postgresql", t)
        _gq(f"DROP TABLE IF EXISTS `{t}_d`")


def test_probe_reads_off_on_this_server():
    from services.dest_require_primary_key import server_requires_primary_key

    if not h.port_up(3306):
        pytest.skip("MySQL not running")
    with h.mysql_conn().cursor() as cur:
        cur.execute("SELECT @@sql_require_primary_key")
        on = str(cur.fetchone()[0]) in {"1", "ON"}
    got = server_requires_primary_key(_cfg("mysql"))
    assert got.variable == ("sql_require_primary_key" if on else "") and not got.unreadable
    # MariaDB has only innodb_force_primary_key; the MySQL name is 1193 there, not a failure.
    from tests.test_r14_mysql_latin1_cp1252_strict_e2e import _cfg as cfg_of

    maria = server_requires_primary_key(cfg_of("mariadb"))
    assert not maria.unreadable, maria
    assert server_requires_primary_key(None).unreadable


def test_an_unreadable_variable_is_a_finding_not_a_pass(client, monkeypatch):
    from services import dest_require_primary_key as rpk

    monkeypatch.setattr(rpk, "server_requires_primary_key",
                        lambda cfg: rpk.PrimaryKeyRequirement(unreadable="sql_require_primary_key unreadable (1227)"))
    t = _source_table()
    try:
        ok, out = _stage(client, t, "full_refresh_append")
        assert not ok and "Could not read whether the destination forces" in str(out), out
        ok, out = _stage(client, t, "upsert")  # keyed create: the question does not arise
        assert ok, out
    finally:
        h.drop_table("postgresql", t)
        h.drop_table("mysql", t)


# ---------------------------------------------------- every mode that CREATEs


def _keyless_source_table() -> str:
    t = f"r14d2k_{uuid.uuid4().hex[:8]}"
    h.execute("postgresql", f'CREATE TABLE "{t}" (id int NOT NULL, name text, amt numeric(8,2))')
    h.execute("postgresql", f"INSERT INTO \"{t}\" VALUES (1, 'a', 1.50), (2, NULL, 0), "
                            f"(3, '', NULL), (4, ' 東京 ', 2.25)")
    return t


def _stage_args(client, t: str, mode: str, dest_cfg: dict | None = None, **extra):
    src = _connector("postgresql", _pg_cfg(), "source")
    dst = _connector("mysql", dest_cfg or _cfg("mysql"), "destination")
    args = {"source_connector_id": src, "dest_connector_id": dst, "source_table": t,
            "dest_table": f"{t}_d", "sync_mode": mode, "primary_key": "id",
            "validation_mode": "strict", **extra}
    if mode in {"incremental_append", "incremental_upsert", "scd2"}:
        args.setdefault("cursor_column", "id")
    return h.mcp_call(client, "start_transfer", args)


@pytest.fixture
def require_pk_on():
    """sql_require_primary_key=ON on the private server only; restored in finally."""
    root = _root()
    with root.cursor() as cur:
        cur.execute("SELECT @@GLOBAL.sql_require_primary_key")
        before = int(cur.fetchone()[0])
        cur.execute("SET GLOBAL sql_require_primary_key = ON")
    try:
        yield
    finally:
        with root.cursor() as cur:
            cur.execute(f"SET GLOBAL sql_require_primary_key = {before}")
            cur.execute("SELECT @@GLOBAL.sql_require_primary_key")
            assert int(cur.fetchone()[0]) == before
        root.close()


# (source has a PK?, mode, extra args, refused?)
_MODES = [
    (False, "full_refresh_overwrite", {}, True),     # keyless source: no key to carry
    (True, "full_refresh_overwrite", {}, False),     # source PK carried
    (False, "upsert", {}, False),                    # operator key is the PK
    (False, "incremental_upsert", {"cursor_semantics": "monotonic_sequence"}, False),
    (True, "full_refresh_append", {}, True),         # append withholds keys
    (True, "scd2", {"cursor_semantics": "modification_timestamp"}, True),  # history table
]


@pytest.mark.parametrize("keyed, mode, extra, refused", _MODES)
def test_live_every_create_mode_is_judged_at_validate(client, require_pk_on, keyed, mode, extra,
                                                      refused):
    t = _source_table() if keyed else _keyless_source_table()
    srv = _globals_server()
    try:
        ok, out = _stage_args(client, t, mode, dest_cfg=srv, **extra)
        if refused:
            assert not ok and "sql_require_primary_key=ON" in str(out), out
            assert not _g_exists(f"{t}_d")
            return
        assert ok, out
        job = h.confirm(client, out["ack_id"], reason=f"r14-d2-{mode}")
        assert job.get("status") == "completed", job
        assert _g_pk(f"{t}_d") == ["id"]
    finally:
        h.drop_table("postgresql", t)
        _gq(f"DROP TABLE IF EXISTS `{t}_d`")


def test_live_query_source_overwrite_is_refused(client, require_pk_on):
    t = _keyless_source_table()
    try:
        ok, out = _stage_args(client, "", "full_refresh_overwrite", dest_cfg=_globals_server(),
                              source_query=f'SELECT id, name FROM "{t}"')
        assert not ok and "sql_require_primary_key=ON" in str(out), out
    finally:
        h.drop_table("postgresql", t)


def test_planned_key_is_the_writers_planner_answer():
    from services.dest_require_primary_key import planned_create_primary_key
    from services.schema_fidelity import build_catalog_from_introspect, catalog_to_payload

    maps = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]

    def catalog(pk):
        return catalog_to_payload(build_catalog_from_introspect(
            dialect="postgresql", columns=["id", "name"],
            column_types={"id": "integer", "name": "text"},
            nullable={"id": False, "name": True},
            keys={"primary_key_columns": pk, "unique_keys": []}))

    keyed, keyless = catalog(["id"]), catalog([])
    cases = {
        ("full_refresh_overwrite", "keyed"): ["id"],
        ("full_refresh_overwrite", "keyless"): [],
        ("full_refresh_overwrite", None): [],
        ("upsert", "keyless"): ["id"],
        ("upsert", None): ["id"],
        ("incremental_deduped", "keyless"): ["id"],
        ("full_refresh_append", "keyed"): [],
        ("incremental_append", "keyed"): [],
        ("scd2", "keyed"): [],
        ("cdc", None): ["id"],
    }
    cats = {"keyed": keyed, "keyless": keyless, None: None}
    for (mode, cat), want in cases.items():
        got = planned_create_primary_key(sync_mode=mode, mappings=maps,
                                         operator_key_columns=["id"],
                                         source_schema_catalog=cats[cat])
        assert got == want, (mode, cat, got)
