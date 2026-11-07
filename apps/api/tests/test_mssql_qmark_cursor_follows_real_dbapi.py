"""The qmark adapter rewrites placeholders for the cursor's real DBAPI.

A pooled pymssql engine can be reused after the driver pick turned to
pyodbc (the pool keys on the endpoint). Rewriting ``%s`` to ``?`` for that
pymssql cursor failed every Oracle->SQL Server copy with "Incorrect syntax
near '?'"; a pyodbc cursor needs ``?`` whatever the current pick says.
"""

from __future__ import annotations

import connectors.generic_sql as gs


def _fake_cursor(module: str):
    seen: list[str] = []

    def execute(self, sql, *args, **kwargs):
        seen.append(sql)

    cls = type("Cursor", (), {"execute": execute, "executemany": execute, "__module__": module})
    return cls(), seen


def test_pymssql_cursor_keeps_percent_s_when_pyodbc_is_the_pick(monkeypatch):
    monkeypatch.setattr(gs, "_mssql_drivername", lambda: "mssql+pyodbc")
    raw, seen = _fake_cursor("pymssql._pymssql")
    cur = gs._MssqlQmarkCursor(raw)
    cur.execute("INSERT INTO t VALUES (%s, %s)", (1, 2))
    cur.executemany("INSERT INTO t VALUES (%s)", [(1,)])
    assert seen == ["INSERT INTO t VALUES (%s, %s)", "INSERT INTO t VALUES (%s)"]


def test_pyodbc_cursor_gets_qmarks_when_pymssql_is_the_pick(monkeypatch):
    monkeypatch.setattr(gs, "_mssql_drivername", lambda: "mssql+pymssql")
    raw, seen = _fake_cursor("pyodbc")
    gs._MssqlQmarkCursor(raw).execute("SELECT %s", (1,))
    assert seen == ["SELECT ?"]


def test_unknown_cursor_follows_the_driver_pick(monkeypatch):
    monkeypatch.setattr(gs, "_mssql_drivername", lambda: "mssql+pyodbc")
    raw, seen = _fake_cursor("tests.fake_dbapi")
    gs._MssqlQmarkCursor(raw).execute("SELECT %s", (1,))
    assert seen == ["SELECT ?"]
