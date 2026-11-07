"""MXD08-proc: staging must not leave procedure side effects before confirm.

Plan, preflight sample and schedule probes all peek a callable source with
``read_callable_batch(..., peek=True)``. Before this fix the peek simply ran
the CALL. On MySQL, a procedure that COMMITs, or that writes a non-transactional
(MyISAM) table, kept that write although the operator never confirmed.

Policy now:
* PostgreSQL proper: run the peek inside an explicit transaction and always
  roll it back. A procedure CALLed inside a transaction block cannot COMMIT,
  so PostgreSQL itself guarantees the rollback.
* Every other engine (MySQL implicit/explicit commits, MyISAM, SQL Server,
  Oracle autonomous transactions, Snowflake, ...): refuse to execute before
  confirm. A declared ``procedure_result_schema`` gives the shape without
  running anything.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.procedure_source import ProcedureSourceError, read_callable_batch  # noqa: E402
from tests.test_mx2_04_mysql_lob_tier_dialect import _MYSQL_PORT, _mysql  # noqa: E402
from tests.test_mx2_16_overwrite_create_new_carries_keys import _pg  # noqa: E402

_LOGGER = "services.procedure_source"


@pytest.fixture
def mysql_procs():
    conn = _mysql()
    cur = conn.cursor()
    for sql in (
        "DROP TABLE IF EXISTS qa_mxd08_audit",
        "DROP TABLE IF EXISTS qa_mxd08_audit_myisam",
        "CREATE TABLE qa_mxd08_audit (id INT AUTO_INCREMENT PRIMARY KEY, note TEXT) ENGINE=InnoDB",
        "CREATE TABLE qa_mxd08_audit_myisam (id INT AUTO_INCREMENT PRIMARY KEY, note TEXT) ENGINE=MyISAM",
        "DROP PROCEDURE IF EXISTS qa_mxd08_commit",
        "DROP PROCEDURE IF EXISTS qa_mxd08_myisam",
        "CREATE PROCEDURE qa_mxd08_commit() BEGIN INSERT INTO qa_mxd08_audit(note) "
        "VALUES ('peeked'); COMMIT; SELECT 1 AS id, 'a' AS name; END",
        "CREATE PROCEDURE qa_mxd08_myisam() BEGIN INSERT INTO qa_mxd08_audit_myisam(note) "
        "VALUES ('peeked'); SELECT 1 AS id, 'a' AS name; END",
    ):
        cur.execute(sql)
    conn.commit()

    def audit() -> list[str]:
        conn.commit()  # fresh snapshot
        cur.execute(
            "SELECT note FROM qa_mxd08_audit UNION ALL SELECT note FROM qa_mxd08_audit_myisam"
        )
        return [r[0] for r in cur.fetchall()]

    cfg = {
        "type": "mysql", "host": "127.0.0.1", "port": _MYSQL_PORT, "database": "dataflow",
        "username": "root", "password": "dataflow", "ssl": False,
        "source_read_mode": "procedure",
    }
    yield cfg, audit
    for sql in (
        "DROP PROCEDURE IF EXISTS qa_mxd08_commit", "DROP PROCEDURE IF EXISTS qa_mxd08_myisam",
        "DROP TABLE IF EXISTS qa_mxd08_audit", "DROP TABLE IF EXISTS qa_mxd08_audit_myisam",
    ):
        cur.execute(sql)
    conn.commit()
    conn.close()


@pytest.mark.parametrize("call", ["CALL qa_mxd08_commit()", "CALL qa_mxd08_myisam()"])
def test_live_mysql_procedure_peek_refuses_and_leaves_no_side_effect(mysql_procs, call, caplog):
    cfg, audit = mysql_procs
    caplog.set_level(logging.WARNING, logger=_LOGGER)
    with pytest.raises(ProcedureSourceError) as info:
        read_callable_batch({**cfg, "procedure_call": call}, offset=0, limit=50, peek=True)
    assert "procedure_result_schema" in str(info.value)
    assert audit() == []
    assert any("not executed before confirm" in r.getMessage() for r in caplog.records)


def test_live_mysql_declared_schema_peek_does_not_execute(mysql_procs):
    cfg, audit = mysql_procs
    batch = read_callable_batch(
        {
            **cfg,
            "procedure_call": "CALL qa_mxd08_commit()",
            "procedure_result_schema": {"id": "INTEGER", "name": "VARCHAR(10)"},
        },
        offset=0, limit=50, peek=True,
    )
    assert batch.headers == ["id", "name"]
    assert batch.rows == []
    assert batch.meta["native_types"] == {"id": "INTEGER", "name": "VARCHAR(10)"}
    assert audit() == []


def test_live_mysql_confirmed_extract_still_runs(mysql_procs):
    from services.procedure_source import close_callable_spool

    cfg, _audit = mysql_procs
    try:
        batch = read_callable_batch(
            {**cfg, "procedure_call": "CALL qa_mxd08_myisam()"}, offset=0, limit=50, peek=False,
        )
    finally:
        close_callable_spool()
    assert batch.headers == ["id", "name"] and len(batch.rows) == 1


@pytest.fixture
def pg_procs():
    conn = _pg()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS qa_mxd08_audit")
    cur.execute("CREATE TABLE qa_mxd08_audit (id serial, note text)")
    cur.execute(
        "CREATE OR REPLACE FUNCTION qa_mxd08_fn() RETURNS TABLE(id int, name text) "
        "LANGUAGE plpgsql AS $$ BEGIN INSERT INTO qa_mxd08_audit(note) VALUES ('fn'); "
        "RETURN QUERY SELECT 1, 'a'::text; END $$"
    )
    cur.execute(
        "CREATE OR REPLACE PROCEDURE qa_mxd08_p(INOUT n int DEFAULT 0) LANGUAGE plpgsql AS "
        "$$ BEGIN INSERT INTO qa_mxd08_audit(note) VALUES ('proc'); n := 7; END $$"
    )
    cur.execute(
        "CREATE OR REPLACE PROCEDURE qa_mxd08_commit(INOUT n int DEFAULT 0) LANGUAGE plpgsql "
        "AS $$ BEGIN INSERT INTO qa_mxd08_audit(note) VALUES ('commit'); COMMIT; n := 8; END $$"
    )

    def audit() -> list[str]:
        cur.execute("SELECT note FROM qa_mxd08_audit")
        return [r[0] for r in cur.fetchall()]

    cfg = {
        "type": "postgresql", "host": "127.0.0.1", "port": 5432, "database": "dataflow",
        "username": "dataflow", "password": "dataflow", "ssl": False,
        "source_read_mode": "procedure",
    }
    yield cfg, audit
    cur.execute("DROP FUNCTION IF EXISTS qa_mxd08_fn()")
    cur.execute("DROP PROCEDURE IF EXISTS qa_mxd08_p(int)")
    cur.execute("DROP PROCEDURE IF EXISTS qa_mxd08_commit(int)")
    cur.execute("DROP TABLE IF EXISTS qa_mxd08_audit")
    conn.close()


@pytest.mark.parametrize("call", ["SELECT * FROM qa_mxd08_fn()", "CALL qa_mxd08_p()"])
def test_live_pg_procedure_peek_is_rolled_back_explicitly(pg_procs, call, caplog):
    cfg, audit = pg_procs
    caplog.set_level(logging.INFO, logger=_LOGGER)
    batch = read_callable_batch({**cfg, "procedure_call": call}, offset=0, limit=50, peek=True)
    assert batch.rows  # schema discovery still works on PostgreSQL
    assert audit() == []
    assert any("rolled back" in r.getMessage() for r in caplog.records)


def test_live_pg_procedure_that_commits_is_refused_and_leaves_nothing(pg_procs):
    cfg, audit = pg_procs
    with pytest.raises(ProcedureSourceError):
        read_callable_batch(
            {**cfg, "procedure_call": "CALL qa_mxd08_commit()"}, offset=0, limit=50, peek=True,
        )
    assert audit() == []
