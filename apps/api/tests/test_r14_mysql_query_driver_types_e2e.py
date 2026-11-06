"""QA MySQL D5: a query's result type comes from the driver, not from samples.

QA (job 6ae6a378…): ``SELECT id, JSON_UNQUOTE(JSON_EXTRACT(doc, '$.v')) AS v``
whose text values are mostly digits and once ``'ü1'`` was planned DECIMAL and
Validate blocked INVALID_NUMERIC. MySQL 8 describes JSON_UNQUOTE as type code
251 (LONG_BLOB, utf8mb4 = LONGTEXT) with size slots 4294967295/31; those were
read as a DECIMAL declaration, and the MCP plan then re-profiled the text
column as BIGINT off fifty digit samples. MariaDB describes it as VAR_STRING.

Route: MCP ``start_transfer`` -> confirm -> job, MySQL and MariaDB sources,
PostgreSQL and MySQL destinations, the 'ü1' row outside the 50-row peek, NULL,
'' and whitespace values, run 2 into the table run 1 created.
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r14_mysql_latin1_cp1252_strict_e2e import _cfg, _connector, _pg_cfg, _sql


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _values() -> list[tuple[int, str | None]]:
    vals: list[tuple[int, str | None]] = [(i, str(i)) for i in range(1, 80)]
    vals += [(200, "ü1"), (201, ""), (202, "  "), (203, None), (204, "東京")]
    return vals


def _seed(engine: str, table: str, rows: list[tuple[int, str | None]]) -> None:
    _sql(engine, f"CREATE TABLE `{table}` (id int PRIMARY KEY, doc json) "
                 "DEFAULT CHARSET=utf8mb4")
    for i, v in rows:
        doc = json.dumps({"v": v}, ensure_ascii=False) if v is not None else '{"w": 1}'
        _sql(engine, f"INSERT INTO `{table}` VALUES (%s, %s)", (i, doc))


def _expected(engine: str, table: str) -> dict[int, str | None]:
    return {r[0]: r[1] for r in _sql(
        engine, f"SELECT id, JSON_UNQUOTE(JSON_EXTRACT(doc, '$.v')) FROM `{table}`")}


@pytest.mark.parametrize("dest", ["postgresql", "mysql"])
@pytest.mark.parametrize("engine", ["mysql", "mariadb"])
def test_json_unquote_text_is_text_run1_and_run2(client, engine, dest):
    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r14d5_{token}", f"r14d5d_{token}"
    try:
        _seed(engine, src_t, _values())
        src = _connector("mariadb" if engine == "mariadb" else "mysql", _cfg(engine), "source")
        dst = _connector(dest, _pg_cfg() if dest == "postgresql" else _cfg("mysql"),
                         "destination")
        args = {"source_connector_id": src, "dest_connector_id": dst, "dest_table": dst_t,
                "source_query": f"SELECT id, JSON_UNQUOTE(JSON_EXTRACT(doc, '$.v')) AS v "
                                f"FROM `{src_t}`",
                "sync_mode": "full_refresh_overwrite", "primary_key": "id",
                "validation_mode": "strict"}
        for run in (1, 2):
            ok, staged = h.mcp_call(client, "start_transfer", args)
            assert ok, {"error": staged}
            types = (staged.get("plan") or {}).get("column_types") or {}
            assert "TEXT" in str(types.get("v")).upper() or "CHAR" in str(types.get("v")).upper(), types
            h.confirm(client, staged["ack_id"], reason=f"r14-d5-run{run}")
            got = {int(r[0]): r[1] for r in h.query(
                dest, f"SELECT {h.quote(dest, 'id')}, {h.quote(dest, 'v')} "
                      f"FROM {h.quote(dest, dst_t)}")}
            assert got == _expected(engine, src_t)
            dtype = h.query(dest, "SELECT data_type FROM information_schema.columns WHERE "
                                  f"table_name = '{dst_t}' AND column_name = 'v'")[0][0]
            assert str(dtype).lower() in {"text", "longtext", "mediumtext", "character varying",
                                          "varchar"}, dtype
            if run == 1:
                _sql(engine, f"INSERT INTO `{src_t}` VALUES (300, '{{\"v\": \"9.5x\"}}')")
    finally:
        _sql(engine, f"DROP TABLE IF EXISTS `{src_t}`")
        h.drop_table(dest, dst_t)


def test_lob_type_codes_are_not_a_decimal_declaration():
    from services.decimal_observe import (
        cursor_declared_carriers,
        cursor_declared_numeric_types,
        mysql_lob_carrier,
    )

    desc = (("a", 251, None, 4294967295, 4294967295, 31, True),
            ("b", 250, None, 16777215, 16777215, 31, True),
            ("c", 249, None, 255, 255, 31, True),
            ("d", 246, None, 12, 12, 2, True))
    names = ["a", "b", "c", "d"]
    assert cursor_declared_numeric_types(names, desc, "mysql") == {"d": "DECIMAL(10,2)"}
    assert "a" not in cursor_declared_carriers(names, desc, "mysql")
    assert mysql_lob_carrier(desc[0], "VARCHAR") == "LONGTEXT"
    assert mysql_lob_carrier(desc[1], "VARCHAR") == "MEDIUMTEXT"
    assert mysql_lob_carrier(desc[2], "BYTEA") == "TINYBLOB"
    assert mysql_lob_carrier(("g", 252, None, 16777216, 16777216, 0, True), "VARCHAR") == "LONGTEXT"
    assert mysql_lob_carrier(("t", 252, None, 65535, 65535, 0, True), "VARCHAR") == "TEXT"
    assert mysql_lob_carrier(desc[3], "DECIMAL") == ""
    assert mysql_lob_carrier(desc[0], "") == ""


@pytest.mark.parametrize("fmt, mode, declared, want", [
    ("mysql", "", False, True), ("mysql", "query", False, True),
    ("postgresql", "query", False, True), ("sqlserver", "query", False, True),
    ("oracle", "query", False, True), ("snowflake", "query", False, False),
    ("mysql", "procedure", False, False), ("mysql", "procedure", True, True),
    ("postgresql", "procedure", False, False),
])
def test_mcp_and_rest_share_one_type_authority_decision(fmt, mode, declared, want):
    """MCP (_plan_source_types_authoritative) and REST / Execute
    (data_profiler.source_types_are_authoritative_for, used by transfer_plan_service
    and engine_automap) answer the same question the same way."""
    from services.data_profiler import source_types_are_authoritative_for
    from src.ai.copilot.transfer_tools import _plan_source_types_authoritative

    schema = {"id": "INTEGER"} if declared else {}
    plan = {"mode": mode, "text": "CALL p()" if mode == "procedure" else "SELECT 1",
            "procedure_result_schema": schema} if mode else None
    mcp = _plan_source_types_authoritative({"kind": "database", "type": fmt}, {"db_type": fmt}, plan)
    rest_src = {"kind": "database", "format": fmt, "extra": {
        "source_read_mode": mode, "source_query": "SELECT 1" if mode == "query" else "",
        "procedure_call": "CALL p()" if mode == "procedure" else "",
        "procedure_result_schema": schema}}
    rest = source_types_are_authoritative_for("database", fmt, rest_src)
    assert mcp == rest == want, (mcp, rest)


def test_a_procedure_at_execute_is_typed_by_its_cursor_and_stable_across_runs(client):
    """At Execute the CALL has run: its cursor.description types the result the
    way a query's does, so a saved procedure pipeline keeps its types run to run
    (sample typing made 'ü1' beyond the peek an INVALID_NUMERIC BIGINT)."""
    token = uuid.uuid4().hex[:8]
    src_t, dst_t, proc = f"r14d5p_{token}", f"r14d5pd_{token}", f"r14d5proc_{token}"
    # Run 1 holds only digit text: a sample would call it BIGINT. Run 2 adds 'ü1'.
    _seed("mysql", src_t, [(i, str(i)) for i in range(1, 80)])
    _sql("mysql", f"CREATE PROCEDURE `{proc}`() SELECT id, JSON_UNQUOTE(JSON_EXTRACT(doc, '$.v')) "
                  f"AS v FROM `{src_t}`")
    src = _connector("mysql", _cfg("mysql"), "source")
    dst = _connector("postgresql", _pg_cfg(), "destination")
    try:
        for run in (1, 2):
            resp = client.post("/api/v1/transfer/execute", json={
                "source": {"kind": "database", "format": "mysql", "connector_id": src,
                           "table": "", "source_read_mode": "procedure",
                           "procedure_call": f"CALL {proc}()"},
                "destination": {"kind": "database", "format": "postgresql", "connector_id": dst,
                                "table": dst_t, "schema": "public"},
                "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
                "async_mode": False})
            assert resp.status_code == 200, resp.text
            job = h.wait_job(str(resp.json().get("job_id") or ""))
            assert job.get("status") == "completed", (run, job.get("error"))
            dtype = h.query("postgresql", "SELECT data_type FROM information_schema.columns "
                                          f"WHERE table_name = '{dst_t}' AND column_name = 'v'")[0][0]
            assert dtype == "text", (run, dtype)
            if run == 1:
                _sql("mysql", f"INSERT INTO `{src_t}` VALUES (500, %s)",
                     (json.dumps({"v": "ü1"}, ensure_ascii=False),))
    finally:
        _sql("mysql", f"DROP PROCEDURE IF EXISTS `{proc}`")
        _sql("mysql", f"DROP TABLE IF EXISTS `{src_t}`")
        h.drop_table("postgresql", dst_t)


def test_a_saved_procedure_schedule_with_a_declared_result_schema_keeps_its_types(client):
    """Third review: the same guard on a saved pipeline. The source connector carries a
    design-time ``procedure_result_schema``, and two schedule beats (digit text,
    then 'ü1') keep v as text with every value intact."""
    from tests.test_r12_mongo_incremental_schedule_e2e import _beat

    token = uuid.uuid4().hex[:8]
    src_t, dst_t, proc = f"r14d5s_{token}", f"r14d5sd_{token}", f"r14d5sp_{token}"
    _seed("mysql", src_t, [(i, str(i)) for i in range(1, 80)])
    _sql("mysql", f"CREATE PROCEDURE `{proc}`() SELECT id, JSON_UNQUOTE(JSON_EXTRACT(doc, '$.v')) "
                  f"AS v FROM `{src_t}`")
    src = _connector("mysql", {**_cfg("mysql"), "extra": {
        "procedure_result_schema": {"id": "INTEGER", "v": "TEXT"}}}, "source")
    dst = _connector("postgresql", _pg_cfg(), "destination")
    try:
        resp = client.post("/api/v1/schedules/", json={
            "name": f"r14d5s {token}", "source_connector_id": src, "source_table": "",
            "source_read_mode": "procedure", "procedure_call": f"CALL {proc}()",
            "dest_connector_id": dst, "dest_table": dst_t, "interval": "hourly",
            "sync_mode": "full_refresh_overwrite",
            "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "v")],
            "enabled": True})
        assert resp.status_code == 201, resp.text
        sid = resp.json()["id"]
        for run in (1, 2):
            beat = _beat(sid)
            job = beat["job"]
            assert job.get("status") == "completed", (run, beat["started"], job.get("error"),
                                                      getattr(beat["schedule"], "last_status", None))
            dtype = h.query("postgresql", "SELECT data_type FROM information_schema.columns "
                                          f"WHERE table_name = '{dst_t}' AND column_name = 'v'")[0][0]
            assert dtype == "text", (run, dtype)
            got = {r[0]: r[1] for r in h.query("postgresql", f'SELECT id, v FROM "{dst_t}"')}
            assert got == _expected("mysql", src_t), run
            if run == 1:
                _sql("mysql", f"INSERT INTO `{src_t}` VALUES (500, %s)",
                     (json.dumps({"v": "ü1"}, ensure_ascii=False),))
    finally:
        _sql("mysql", f"DROP PROCEDURE IF EXISTS `{proc}`")
        _sql("mysql", f"DROP TABLE IF EXISTS `{src_t}`")
        h.drop_table("postgresql", dst_t)
