"""D195 / D112 — QA's route: start_transfer -> /api/v1/copilot/confirm -> job.

QA (prod ac612203): PG -> an existing PG table with CHECK (n > 0),
full_refresh_overwrite, source holds n = -5. Preflight said nothing (the CHECK
finding was only a warning outside strict), the PG -> PG COPY route dropped
and re-created the table from the source's shape, the job reported "Row
fidelity verified", and the table held -5 with only its PRIMARY KEY left.

Acceptance: the violating row is refused before Confirm, and a clean overwrite
(run twice) leaves the destination's constraint list identical. Same-engine
pairs on PostgreSQL, MySQL and SQL Server, default validation mode.
"""

from __future__ import annotations

import os
import socket
import uuid
from typing import Any

import pytest

from tests.test_upsert_infile_duplicate_start_transfer_e2e import (  # noqa: F401
    _wait_job,
    transfer_client,
)

_MSSQL_PW = os.getenv("DATAFLOW_TEST_MSSQL_PASSWORD") or os.getenv("MSSQL_SA_PASSWORD") or ""


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1.5):
            return True
    except OSError:
        return False


def _pg():
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    return conn


def _mysql():
    import pymysql

    return pymysql.connect(
        host="127.0.0.1", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )


def _mssql():
    import pyodbc

    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
        f"UID=sa;PWD={_MSSQL_PW};TrustServerCertificate=yes",
        autocommit=True,
        timeout=10,
    )


_SETUP: dict[str, dict[str, Any]] = {
    "postgresql": {
        "connect": _pg,
        "connector": {"type": "postgresql", "port": 5432, "username": "dataflow",
                      "password": "dataflow", "schema": "public"},
        "dest_ddl": [
            "CREATE TABLE {d} (id INT PRIMARY KEY, n INT CONSTRAINT {d}_n CHECK (n > 0), "
            "label VARCHAR(20) NOT NULL DEFAULT 'z')",
            "CREATE UNIQUE INDEX {d}_ux ON {d} (label)",
            "COMMENT ON TABLE {d} IS 'keep me'",
        ],
        "constraints": [
            "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = '{d}'::regclass",
            "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{d}'",
            "SELECT column_name, is_nullable, column_default FROM information_schema.columns "
            "WHERE table_name = '{d}'",
            "SELECT obj_description('{d}'::regclass, 'pg_class')",
        ],
    },
    "mysql": {
        "connect": _mysql,
        "connector": {"type": "mysql", "port": 3306, "username": "dataflow", "password": "dataflow"},
        "dest_ddl": [
            "CREATE TABLE {d} (id INT PRIMARY KEY, n INT, label VARCHAR(20) NOT NULL DEFAULT 'z', "
            "CONSTRAINT {d}_n CHECK (n > 0), UNIQUE KEY {d}_ux (label)) COMMENT='keep me'",
        ],
        "constraints": ["SHOW CREATE TABLE {d}"],
    },
    "sqlserver": {
        "connect": _mssql,
        "connector": {"type": "sqlserver", "port": 1433, "username": "sa", "password": _MSSQL_PW,
                      "schema": "dbo", "trust_server_certificate": True},
        "dest_ddl": [
            "CREATE TABLE {d} (id INT PRIMARY KEY, n INT CONSTRAINT {d}_n CHECK (n > 0), "
            "label VARCHAR(20) NOT NULL CONSTRAINT {d}_df DEFAULT 'z')",
            "CREATE UNIQUE INDEX {d}_ux ON {d} (label)",
        ],
        "constraints": [
            "SELECT name, definition FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID('{d}')",
            "SELECT name, definition FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID('{d}')",
            "SELECT name, is_unique, is_primary_key FROM sys.indexes WHERE object_id = OBJECT_ID('{d}') "
            "AND name IS NOT NULL",
            "SELECT name, is_nullable FROM sys.columns WHERE object_id = OBJECT_ID('{d}')",
        ],
    },
}

_PORTS = {"postgresql": 5432, "mysql": 3306, "sqlserver": 1433}


def _q(conn, sql: str) -> list[tuple]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        cur.close()


@pytest.fixture(params=sorted(_SETUP))
def engine(request):
    name = request.param
    if not _reachable(_PORTS[name]) or (name == "sqlserver" and not _MSSQL_PW):
        pytest.skip(f"{name} not reachable")
    setup = _SETUP[name]
    conn = setup["connect"]()
    s, d = "qa195s_" + uuid.uuid4().hex[:6], "qa195d_" + uuid.uuid4().hex[:6]
    yield name, setup, conn, s, d
    for table in (s, d):
        _q(conn, f"DROP TABLE IF EXISTS {table}")
    conn.close()


def _snapshot(setup, conn, d: str) -> list:
    return [sorted(_q(conn, sql.format(d=d)), key=repr) for sql in setup["constraints"]]


def _start(setup, s: str, d: str):
    from services.connector_store import create_connector
    from src.ai.copilot.transfer_tools import start_transfer

    def connector(role: str) -> str:
        return create_connector(
            {
                "name": f"qa195-{role}-{uuid.uuid4().hex[:6]}",
                "role": role,
                "host": "127.0.0.1",
                "database": "dataflow",
                "ssl": False,
                **setup["connector"],
            }
        ).id

    # Default validation mode: QA ran without asking for strict.
    return start_transfer(
        source_connector_id=connector("source"),
        source_table=s,
        dest_connector_id=connector("destination"),
        dest_table=d,
        sync_mode="full_refresh_overwrite",
    )


def _confirm_and_wait(client, staged) -> dict:
    confirmed = client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": staged.output["ack_id"], "reason": "qa-d195"},
    )
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body.get("ok") is True, body
    return _wait_job(str(body["job_id"]))


def test_check_violation_is_refused_before_confirm_and_nothing_changes(transfer_client, engine):
    name, setup, conn, s, d = engine
    _q(conn, f"CREATE TABLE {s} (id INT PRIMARY KEY, n INT, label VARCHAR(20))")
    _q(conn, f"INSERT INTO {s} VALUES (1, 3, 'a'), (2, -5, 'b'), (3, 7, 'c')")
    for ddl in setup["dest_ddl"]:
        _q(conn, ddl.format(d=d))
    _q(conn, f"INSERT INTO {d} (id, n, label) VALUES (9, 1, 'old')")
    before = _snapshot(setup, conn, d)

    staged = _start(setup, s, d)
    error = str(staged.error or "")
    assert not staged.success, f"{name}: preflight let n=-5 through to Confirm"
    assert "CHECK" in error and f"{d}_n" in error and "-5" in error, error
    assert sorted(_q(conn, f"SELECT id, n, label FROM {d}")) == [(9, 1, "old")]
    assert _snapshot(setup, conn, d) == before


def test_clean_overwrite_twice_keeps_the_constraint_list(transfer_client, engine):
    name, setup, conn, s, d = engine
    _q(conn, f"CREATE TABLE {s} (id INT PRIMARY KEY, n INT, label VARCHAR(20))")
    _q(conn, f"INSERT INTO {s} VALUES (1, 3, 'a'), (2, 5, 'b'), (3, 7, 'c')")
    for ddl in setup["dest_ddl"]:
        _q(conn, ddl.format(d=d))
    _q(conn, f"INSERT INTO {d} (id, n, label) VALUES (9, 1, 'old')")
    before = _snapshot(setup, conn, d)

    for run in (1, 2):
        staged = _start(setup, s, d)
        assert staged.success, f"{name} run {run}: {staged.error}"
        job = _confirm_and_wait(transfer_client, staged)
        assert str(job.get("status")) == "completed", (run, job.get("error") or job.get("message"))
        assert sorted(_q(conn, f"SELECT id, n, label FROM {d}")) == [
            (1, 3, "a"), (2, 5, "b"), (3, 7, "c"),
        ]
        assert _snapshot(setup, conn, d) == before, f"{name} run {run} changed the constraints"
