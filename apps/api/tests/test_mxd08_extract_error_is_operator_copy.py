"""MXD08 — a refused source extract reads as operator copy, not a driver trace.

QA (PG query mode, staging): ``Could not execute the procedure extract:
Procedure extract failed: (psycopg2.errors.InsufficientPrivilege) permission
denied for schema rnacen LINE 1: … [SQL: …] (Background on this error at:
https://sqlalche.me/e/20/f405)``. The driver's own sentence is kept; the
exception class, SQL echo, caret line and SQLAlchemy link stay in server logs.

Staging does run the query: a read-only SELECT/WITH (query mode admits nothing
else), capped at PEEK_ROW_LIMIT under PEEK_TIMEOUT_S, to discover the result
columns. That part is by design and is pinned below.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.procedure_source import (  # noqa: E402
    PEEK_ROW_LIMIT,
    ProcedureSourceError,
    parse_callable_source,
    read_callable_batch,
)
from tests.test_mx2_16_overwrite_create_new_carries_keys import _pg  # noqa: E402

_NOISE = ("psycopg2.errors", "[SQL:", "sqlalche.me", "Background on this error", "LINE 1:")


@pytest.fixture
def restricted_role():
    conn = _pg()
    cur = conn.cursor()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS qa_mxd08_priv")
        cur.execute("CREATE TABLE IF NOT EXISTS qa_mxd08_priv.rnc_database (id int)")
        cur.execute(
            "DO $$BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='qa_mxd08_ro') "
            "THEN CREATE ROLE qa_mxd08_ro LOGIN PASSWORD 'qa_mxd08_ro'; END IF; END$$"
        )
        cur.execute("REVOKE ALL ON SCHEMA qa_mxd08_priv FROM PUBLIC")
    except Exception as exc:  # noqa: BLE001 — skip reason, not swallowed
        conn.close()
        pytest.skip(f"cannot create a restricted PostgreSQL role: {exc}")
    yield {
        "type": "postgresql", "host": "127.0.0.1", "port": 5432, "database": "dataflow",
        "username": "qa_mxd08_ro", "password": "qa_mxd08_ro", "ssl": False,
        "source_read_mode": "query",
        "source_query": "SELECT * FROM qa_mxd08_priv.rnc_database LIMIT 10",
    }
    cur.execute("DROP SCHEMA IF EXISTS qa_mxd08_priv CASCADE")
    cur.execute("DROP ROLE IF EXISTS qa_mxd08_ro")
    conn.close()


def test_live_permission_denied_peek_is_clean_and_logged(restricted_role, caplog):
    caplog.set_level(logging.WARNING, logger="services.procedure_source")
    with pytest.raises(ProcedureSourceError) as info:
        read_callable_batch(restricted_role, offset=0, limit=50, peek=True)
    message = str(info.value)
    assert "permission denied for schema qa_mxd08_priv" in message
    assert not [n for n in _NOISE if n in message], message
    # Full driver detail stays server-side, with context.
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert "InsufficientPrivilege" in logged and "postgresql" in logged


def test_wrapped_dbapi_error_keeps_the_driver_sentence_only():
    sa = pytest.importorskip("sqlalchemy")
    from services.procedure_source import _operator_extract_error

    class InsufficientPrivilege(Exception):
        pass

    orig = InsufficientPrivilege(
        "permission denied for schema rnacen\nLINE 1: SELECT * FROM rnacen.rnc_database LIMIT 10\n"
        "                      ^\n"
    )
    exc = sa.exc.ProgrammingError("SELECT * FROM rnacen.rnc_database LIMIT 10", {}, orig)
    out = _operator_extract_error(exc)
    assert out.startswith("The source refused the extract: permission denied for schema rnacen")
    assert not [n for n in _NOISE if n in out], out


def test_staging_peek_is_read_only_and_bounded():
    with pytest.raises(ProcedureSourceError):
        parse_callable_source("DELETE FROM t", dialect="postgresql", mode="query")
    assert PEEK_ROW_LIMIT <= 100
