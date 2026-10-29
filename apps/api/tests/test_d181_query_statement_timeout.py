"""D181 — interactive queries carry a server-side statement timeout.

The tool deadline abandons a caller's wait, but the database would keep
executing for nobody. Setting the dialect's statement timeout makes the server
cancel it and the worker returns by itself.
"""

from __future__ import annotations

import pytest

from src.routers.query_router import _apply_statement_timeout


class _Conn:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, stmt, *a, **k):
        self.sql.append(str(stmt))


@pytest.mark.parametrize(
    ("db", "expected"),
    [
        ("postgresql", "SET LOCAL statement_timeout = 120000"),
        ("mysql", "SET SESSION MAX_EXECUTION_TIME = 120000"),
        ("sqlserver", "SET LOCK_TIMEOUT 120000"),
    ],
)
def test_statement_timeout_is_applied_per_dialect(monkeypatch, db, expected):
    monkeypatch.delenv("DATAFLOW_QUERY_STATEMENT_TIMEOUT_SECONDS", raising=False)
    conn = _Conn()
    _apply_statement_timeout(conn, db)
    assert conn.sql == [expected]


def test_statement_timeout_is_configurable_and_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("DATAFLOW_QUERY_STATEMENT_TIMEOUT_SECONDS", "30")
    conn = _Conn()
    _apply_statement_timeout(conn, "postgresql")
    assert conn.sql == ["SET LOCAL statement_timeout = 30000"]
    monkeypatch.setenv("DATAFLOW_QUERY_STATEMENT_TIMEOUT_SECONDS", "0")
    off = _Conn()
    _apply_statement_timeout(off, "postgresql")
    assert off.sql == []


def test_statement_timeout_failure_never_blocks_the_query(monkeypatch):
    class _Boom:
        def execute(self, *a, **k):
            raise RuntimeError("unsupported")

    _apply_statement_timeout(_Boom(), "postgresql")
