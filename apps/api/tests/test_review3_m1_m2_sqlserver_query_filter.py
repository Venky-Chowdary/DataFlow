"""Security review round 3 (M1, M2): the playground's SQL Server filter.

M1: the round-2 T-SQL statement-word guard ran on every dialect and refused
ordinary SELECTs (``SELECT TOP 10 open``, ``qty * open``, ``ELSE open END``,
aliases without ``AS``, ``doc->>'k' open`` on PostgreSQL). It is gone: no
dialect other than SQL Server is held to T-SQL's grammar.

M2: for SQL Server the deny-list is replaced by a parsed allowlist
(``services.tsql_select``, sqlglot's T-SQL parser, pinned in
requirements.txt): exactly one SELECT / WITH … SELECT and nothing after it,
no unquoted reserved keyword as a name. The reviewer's 20 follow-on
statements are all refused.
"""

from __future__ import annotations

import pytest

from src.routers.query_router import _is_safe_sql

ORDINARY = [  # the reviewer's 15 (sql3.out)
    "SELECT TOP 10 open, close FROM prices ORDER BY ts DESC",
    "SELECT TOP (5) begin, finish FROM shifts",
    "SELECT qty * open AS notional FROM trades",
    "SELECT CASE WHEN close IS NULL THEN 0 ELSE open END AS px FROM prices",
    "SELECT p.open_px open FROM prices p",
    "SELECT COALESCE(a, 0) disable FROM flags",
    "SELECT 1 kill FROM t",
    "SELECT DISTINCT open FROM prices",
    "SELECT ROW_NUMBER() OVER (ORDER BY begin) FROM shifts",
    "SELECT open FROM prices LIMIT 5",
    "SELECT 'x' declare FROM t",
    "SELECT doc->>'k' open FROM t",
    "SELECT EXTRACT(year FROM ts) yr, open FROM prices",
    "SELECT * FROM shifts WHERE finish IS NULL ORDER BY begin",
    "SELECT * FROM t WHERE ts BETWEEN 1 AND begin",
]
FOLLOW_ON = [  # the reviewer's 20 (sql3.out)
    "select 1 use master",
    "select 1 set context_info 0x01",
    "select 1 set transaction isolation level read uncommitted",
    "select 1 while 1=1 select 1",
    "select 1 l: goto l",
    "select 1 receive top(1) * from q",
    "select 1 end conversation @h",
    "select 1 move conversation @h to @g",
    "select 1 get conversation group @g from q",
    "select 1 commit",
    "select 1 rollback",
    "select 1 save tran s",
    "select 1 throw 50000, 'x', 1",
    "select 1 print 'x'",
    "select 1 close c deallocate c",
    "select 1 fetch next from c",
    "select 1 readtext t.c @p 0 0",
    "select 1 update statistics t",
    "select 1 add signature to p by certificate c",
    "select 1 if exists(select 1) select 2",
]
SQLSERVER_ORDINARY = [  # valid T-SQL: reserved names quoted, as SQL Server itself requires
    "SELECT TOP 10 [open], [close] FROM prices ORDER BY ts DESC",
    "SELECT TOP (5) [begin], finish FROM shifts",
    "SELECT qty * [open] AS notional FROM trades",
    "SELECT CASE WHEN [close] IS NULL THEN 0 ELSE [open] END AS px FROM prices",
    "SELECT p.open_px [open] FROM prices p",
    "SELECT COALESCE(a, 0) disable FROM flags",
    "SELECT 1 [kill] FROM t",
    "SELECT ROW_NUMBER() OVER (ORDER BY [begin]) AS rn FROM shifts",
    "WITH c AS (SELECT 1 AS a) SELECT * FROM c",
    "SELECT a FROM t WITH (NOLOCK) OPTION (RECOMPILE)",
    "SELECT a FROM t FOR XML PATH('')",
    "SELECT * FROM t CROSS APPLY OPENJSON(t.j)",
    "SELECT a FROM t UNION ALL SELECT b FROM u",
    "SELECT CAST(a AS VARCHAR(10)), CONVERT(varchar, getdate(), 120) FROM t",
    "SELECT a FROM t ORDER BY a OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY",
    "SELECT IIF(a > 1, 'x', 'y') FROM t;",
    "select merge_count, merged from t",
]
OTHER_DIALECTS = ["postgresql", "mysql", "snowflake", "clickhouse", "oracle", "duckdb", "sqlite", ""]


@pytest.mark.parametrize("dialect", OTHER_DIALECTS)
@pytest.mark.parametrize("sql", ORDINARY)
def test_ordinary_selects_pass_on_every_other_dialect(sql, dialect):
    assert _is_safe_sql(sql, dialect=dialect), (dialect, sql)


@pytest.mark.parametrize("sql", SQLSERVER_ORDINARY)
@pytest.mark.parametrize("dialect", ["sqlserver", "mssql", "mssql+pyodbc", "azure_sql"])
def test_ordinary_tsql_selects_pass_on_sql_server(sql, dialect):
    assert _is_safe_sql(sql, dialect=dialect), (dialect, sql)


@pytest.mark.parametrize("sql", FOLLOW_ON)
@pytest.mark.parametrize("dialect", ["sqlserver", "mssql+pyodbc"])
def test_every_follow_on_statement_is_refused_on_sql_server(sql, dialect):
    assert not _is_safe_sql(sql, dialect=dialect), sql


@pytest.mark.parametrize("sql", [
    "select * into x from t",
    "select 1; select 2",
    "select 1 waitfor delay '0:0:5'",
    "update t set a = 1",
    "select 1 kill 53",
    "select 1 open c",
])
def test_writes_and_second_statements_are_refused_on_sql_server(sql):
    assert not _is_safe_sql(sql, dialect="sqlserver"), sql


def test_the_route_passes_the_connectors_dialect(monkeypatch):
    """/query/execute: a SQL Server connector's follow-on statement is refused before any connect."""
    from fastapi import HTTPException

    from src.routers import query_router

    class _Conn:
        type = "sqlserver"
        connection_string = ""

    class _Body:
        query = "select 1 use master"

    def no_engine(*_a, **_k):
        raise AssertionError("connected before the filter")

    monkeypatch.setattr("connectors.generic_sql.get_sqlalchemy_engine", no_engine)
    with pytest.raises(HTTPException) as caught:
        query_router._run_sql_query(_Conn(), _Body())
    assert caught.value.status_code == 400
    _Conn.type, _Conn.connection_string = "generic_sql", "mssql+pyodbc://u:p@h/db"
    with pytest.raises(HTTPException):
        query_router._run_sql_query(_Conn(), _Body())


def test_sqlglot_is_a_declared_pinned_dependency():
    from pathlib import Path

    requirements = (Path(__file__).resolve().parents[1] / "requirements.txt").read_text(encoding="utf-8")
    assert any(line.strip().startswith("sqlglot~=") or line.strip().startswith("sqlglot==")
               for line in requirements.splitlines())
