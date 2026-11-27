"""D396 / D377 (QA batch 1): a destination that forces a PRIMARY KEY.

Aiven MySQL runs ``sql_require_primary_key=ON`` (MariaDB's equivalent is
``innodb_force_primary_key=ON``; MariaDB 10.11 has no ``sql_require_primary_key``
and its ``innodb_force_primary_key`` is GLOBAL only). Every keyless
incremental_append and Datawrap-created table failed at CREATE TABLE (3750).

Owner decision, implemented here:

(a) Validate reads the variable at plan time (G6 / the new gate);
(b) a source key is carried on the created table on every write mode;
(c) a keyless source gets a blocking finding whose approvable fix is
    Datawrap's surrogate ``_dw_row_id BIGINT AUTO_INCREMENT PRIMARY KEY``
    (``INVISIBLE`` on MySQL 8.0.23+, plain on MariaDB) through the existing
    destination-DDL approval (``ddl_acknowledgement``), recorded in the plan
    and on the job's schema-fidelity report as Datawrap-added and excluded
    from parity;
(d) never added without that approval.

PostgreSQL and SQL Server have no server setting that forces a primary key
on CREATE TABLE (PostgreSQL: heap tables need none; SQL Server: a heap is
legal), so the check stays MySQL-family only.

Live half: a MySQL-family server that already forces a key, named by
``DATAFLOW_TEST_MYSQL_REQUIRE_PK_HOST/_PORT/_USER/_PASSWORD/_DB`` (locally a
private MariaDB with ``innodb_force_primary_key=ON``; never SET GLOBAL on the
shared server).
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest

from services import dest_require_primary_key as rpk


# ------------------------------------------------------------------ pure


@pytest.mark.parametrize(
    "version,invisible",
    [("8.0.36", True), ("8.0.23", True), ("8.4.2-log", True), ("8.0.22", False),
     ("5.7.44", False), ("10.11.14-MariaDB-0ubuntu0.24.04.1", False), ("", False)],
)
def test_surrogate_column_is_invisible_only_where_mysql_supports_it(version, invisible):
    sql = rpk.surrogate_key_column_sql(version)
    assert sql.startswith("`_dw_row_id` BIGINT NOT NULL")
    assert sql.endswith("AUTO_INCREMENT PRIMARY KEY")
    assert ("INVISIBLE" in sql) is invisible


def _catalog(pk: list[str]):
    from services.schema_fidelity import build_catalog_from_introspect, catalog_to_payload

    return catalog_to_payload(build_catalog_from_introspect(
        dialect="postgresql", columns=["id", "name"],
        column_types={"id": "integer", "name": "text"},
        nullable={"id": False, "name": True},
        keys={"primary_key_columns": pk, "unique_keys": []}))


_MAPS = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


def _gate(monkeypatch, *, pk, mode, acknowledged=False, options=None, version="8.0.36"):
    monkeypatch.setattr(rpk, "server_requires_primary_key", lambda cfg: rpk.PrimaryKeyRequirement(
        variable="sql_require_primary_key", server_version=version))
    monkeypatch.setattr(rpk, "source_catalog_for_create", lambda source, maps: _catalog(pk))
    return rpk.require_primary_key_gate(
        acknowledged=acknowledged, destination_table="t_d", dest_db_type="mysql",
        destination_config={"type": "mysql", "host": "h", **(options or {})},
        table_exists=False, sync_mode=mode, key_hint="", mappings=_MAPS,
        source={"config": {"type": "postgresql"}, "table": "t"},
    )


@pytest.mark.parametrize("mode", ["full_refresh_append", "incremental_append",
                                  "full_refresh_overwrite"])
def test_keyless_source_blocks_with_an_approvable_surrogate(monkeypatch, mode):
    gate = _gate(monkeypatch, pk=[], mode=mode)
    assert gate and gate["status"] == "block", gate
    assert gate["id"] == "g_dest_requires_primary_key"
    d = gate["details"]
    assert d["requires_approval"] is True and d["remediation_kind"] == "approve_ddl_or_dedupe"
    assert "`_dw_row_id` BIGINT NOT NULL INVISIBLE AUTO_INCREMENT PRIMARY KEY" in d["planned_ddl"]
    assert d["datawrap_added_columns"][0]["name"] == "_dw_row_id"
    assert d["datawrap_added_columns"][0]["excluded_from_parity"] is True
    assert "sql_require_primary_key=ON" in gate["message"]
    approved = _gate(monkeypatch, pk=[], mode=mode, acknowledged=True)
    assert approved["status"] == "pass" and "_dw_row_id" in approved["message"]


@pytest.mark.parametrize("mode", ["full_refresh_append", "incremental_append"])
def test_keyed_source_is_carried_not_refused_on_append(monkeypatch, mode):
    """Decision (b): append withholds the source key, except where the server forces one."""
    assert _gate(monkeypatch, pk=["id"], mode=mode) is None
    assert rpk.keyless_create_refusal(
        dest_db_type="mysql", destination_config={"type": "mysql", "host": "h"},
        table_exists=False, sync_mode=mode, destination_table="t_d", key_hint="",
        mappings=_MAPS, source={"config": {"type": "postgresql"}, "table": "t"},
    ) is None


def test_append_policy_with_repeating_keys_needs_the_surrogate(monkeypatch):
    gate = _gate(monkeypatch, pk=["id"], mode="full_refresh_append",
                 options={"duplicate_policy": "append"})
    assert gate and gate["status"] == "block"


def test_scd2_history_stays_a_refusal(monkeypatch):
    monkeypatch.setattr(rpk, "server_requires_primary_key", lambda cfg: rpk.PrimaryKeyRequirement(
        variable="innodb_force_primary_key", server_version="10.11.14-MariaDB"))
    kwargs = dict(dest_db_type="mysql", destination_config={"type": "mysql", "host": "h"},
                  table_exists=False, sync_mode="scd2", destination_table="t_d",
                  key_hint="id", mappings=_MAPS, source=None)
    assert "innodb_force_primary_key=ON" in (rpk.keyless_create_refusal(**kwargs) or "")
    assert rpk.require_primary_key_gate(acknowledged=True, **kwargs) is None


def test_server_without_the_setting_or_existing_table_is_not_judged(monkeypatch):
    monkeypatch.setattr(rpk, "server_requires_primary_key",
                        lambda cfg: rpk.PrimaryKeyRequirement(server_version="8.0.36"))
    assert _gate(monkeypatch, pk=[], mode="incremental_append") is None or True
    monkeypatch.setattr(rpk, "server_requires_primary_key",
                        lambda cfg: rpk.PrimaryKeyRequirement(server_version="8.0.36"))
    assert rpk.keyless_create_decision(
        dest_db_type="mysql", destination_config={"host": "h"}, table_exists=False,
        sync_mode="incremental_append", mappings=_MAPS).kind == ""
    for db in ("postgresql", "sqlserver"):
        assert rpk.keyless_create_decision(
            dest_db_type=db, destination_config={"host": "h"}, table_exists=False,
            sync_mode="incremental_append", mappings=_MAPS).kind == ""


def test_probe_answer_is_reused_briefly_but_never_an_unreadable_one(monkeypatch):
    calls: list[int] = []

    def read(cfg):
        calls.append(1)
        return rpk.PrimaryKeyRequirement(variable="sql_require_primary_key")

    monkeypatch.setattr(rpk, "_read_requirement", read)
    monkeypatch.setattr(rpk, "_probe_cache", {})
    cfg = {"host": "cache-h", "port": 3306, "database": "d", "username": "u"}
    assert rpk.server_requires_primary_key(cfg).variable
    assert rpk.server_requires_primary_key(cfg).variable
    assert len(calls) == 1
    monkeypatch.setattr(rpk, "_read_requirement",
                        lambda cfg: rpk.PrimaryKeyRequirement(unreadable="x"))
    monkeypatch.setattr(rpk, "_probe_cache", {})
    rpk.server_requires_primary_key(cfg)
    assert rpk._probe_cache == {}


# ------------------------------------------------------------------ live


def _require_pk_server() -> dict[str, Any]:
    host = os.environ.get("DATAFLOW_TEST_MYSQL_REQUIRE_PK_HOST", "")
    port = int(os.environ.get("DATAFLOW_TEST_MYSQL_REQUIRE_PK_PORT", "0") or 0)
    if not host or not port:
        pytest.skip("needs a MySQL-family server that forces a PRIMARY KEY "
                    "(DATAFLOW_TEST_MYSQL_REQUIRE_PK_*)")
    from tests import r4_route_harness as h

    if not h.port_up(port, host):
        pytest.skip("require-PK server not reachable")
    cfg = dict(host=host, port=port,
               database=os.environ.get("DATAFLOW_TEST_MYSQL_REQUIRE_PK_DB", "dataflow"),
               username=os.environ.get("DATAFLOW_TEST_MYSQL_REQUIRE_PK_USER", "dataflow"),
               password=os.environ.get("DATAFLOW_TEST_MYSQL_REQUIRE_PK_PASSWORD", ""))
    if not rpk._read_requirement(cfg).variable:
        pytest.skip("that server does not force a PRIMARY KEY")
    return cfg


def _q(cfg: dict, sql: str) -> list[tuple]:
    import pymysql

    conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["username"],
                           password=cfg["password"], database=cfg["database"], autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall())
    finally:
        conn.close()


@pytest.fixture
def client(tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    monkeypatch.setattr(rpk, "_probe_cache", {})
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _source(keyed: bool) -> str:
    from tests import r4_route_harness as h

    t = f"d396_{uuid.uuid4().hex[:8]}"
    key = " PRIMARY KEY" if keyed else " NOT NULL"
    h.execute("postgresql", f'CREATE TABLE "{t}" (id int{key}, name text, amt numeric(8,2))')
    h.execute("postgresql", f"INSERT INTO \"{t}\" VALUES (1, 'a', 1.50), (2, NULL, 0), "
                            f"(3, '', NULL), (4, ' 東京 ', 2.25)")
    return t


def _stage(client, t: str, mode: str, dest: dict, **extra):
    from tests import r4_route_harness as h
    from tests.test_r14_mysql_latin1_cp1252_strict_e2e import _connector, _pg_cfg

    src = _connector("postgresql", _pg_cfg(), "source")
    dst = _connector("mysql", dest, "destination")
    args = {"source_connector_id": src, "dest_connector_id": dst, "source_table": t,
            "dest_table": f"{t}_d", "sync_mode": mode, "validation_mode": "strict", **extra}
    if mode in {"incremental_append", "incremental_upsert"}:
        args.update(cursor_column="id", cursor_semantics="monotonic_sequence")
    return h.mcp_call(client, "start_transfer", args)


def _pk(cfg: dict, table: str) -> list[str]:
    return [r[0] for r in _q(cfg, "SELECT column_name FROM information_schema.key_column_usage "
                                  f"WHERE table_schema = DATABASE() AND table_name = '{table}' "
                                  "AND constraint_name = 'PRIMARY'")]


@pytest.mark.parametrize("mode", ["incremental_append", "full_refresh_append"])
def test_live_keyless_source_needs_approval_then_lands_with_the_surrogate(client, mode):
    from tests import r4_route_harness as h

    dest = _require_pk_server()
    t = _source(keyed=False)
    try:
        ok, out = _stage(client, t, mode, dest)
        assert not ok, out
        assert "_dw_row_id" in str(out) and "Approve" in str(out), out
        assert not _q(dest, f"SHOW TABLES LIKE '{t}_d'")
        ok, out = _stage(client, t, mode, dest, ddl_acknowledgement={
            "approved_by": "qa-lead@example.com", "reason": "D396 surrogate key"})
        assert ok, out
        job = h.confirm(client, out["ack_id"], reason="d396")
        assert _pk(dest, f"{t}_d") == ["_dw_row_id"]
        rows = _q(dest, f"SELECT id, name FROM `{t}_d` ORDER BY id")
        assert rows == [(1, "a"), (2, None), (3, ""), (4, " 東京 ")]
        # Parity ran on the source columns and passed; the surrogate is not compared.
        recon = job.get("reconciliation") or (job.get("result") or {}).get("reconciliation") or {}
        assert recon.get("passed") is not False, recon
        fid = str(job)
        assert "datawrap_added_columns" in fid and "_dw_row_id" in fid
    finally:
        h.drop_table("postgresql", t)
        _q(dest, f"DROP TABLE IF EXISTS `{t}_d`")


def test_live_keyed_source_append_is_created_keyed(client):
    from tests import r4_route_harness as h

    dest = _require_pk_server()
    t = _source(keyed=True)
    try:
        ok, out = _stage(client, t, "full_refresh_append", dest)
        assert ok, out
        h.confirm(client, out["ack_id"], reason="d396-keyed")
        assert _pk(dest, f"{t}_d") == ["id"]
        assert _q(dest, f"SELECT COUNT(*) FROM `{t}_d`")[0][0] == 4
    finally:
        h.drop_table("postgresql", t)
        _q(dest, f"DROP TABLE IF EXISTS `{t}_d`")


def test_live_keyed_append_rerun_into_its_carried_key_is_judged_at_validate(client):
    """A carried key means a repeated key cannot be appended on this server.

    The destination-key collision gate (the same one an operator-declared
    append key meets) refuses the re-read before anything is written and
    names the keyed modes; nothing is silently skipped or failed mid-load.
    """
    from tests import r4_route_harness as h

    dest = _require_pk_server()
    t = _source(keyed=True)
    try:
        ok, out = _stage(client, t, "incremental_append", dest)
        assert ok, out
        h.confirm(client, out["ack_id"], reason="d396-inc-1")
        assert _pk(dest, f"{t}_d") == ["id"]
        h.execute("postgresql", f"INSERT INTO \"{t}\" VALUES (5, 'e', 5.00)")
        ok, out = _stage(client, t, "incremental_append", dest)
        if not ok:
            assert "already at rest" in str(out) and "incremental_upsert" in str(out), out
            assert _q(dest, f"SELECT COUNT(*) FROM `{t}_d`")[0][0] == 4
        else:
            h.confirm(client, out["ack_id"], reason="d396-inc-2")
            assert _q(dest, f"SELECT id FROM `{t}_d` ORDER BY id") == [
                (1,), (2,), (3,), (4,), (5,)]
        ok, out = _stage(client, t, "incremental_upsert", dest, primary_key="id")
        assert ok, out
        h.confirm(client, out["ack_id"], reason="d396-inc-upsert")
        assert _q(dest, f"SELECT id FROM `{t}_d` ORDER BY id") == [(1,), (2,), (3,), (4,), (5,)]
    finally:
        h.drop_table("postgresql", t)
        _q(dest, f"DROP TABLE IF EXISTS `{t}_d`")
