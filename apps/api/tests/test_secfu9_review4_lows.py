"""Security follow-up 9: the lows from PR 403 review 4.

* The SQL Server playground accepted ``NEXT VALUE FOR <sequence>``, which
  advances the sequence (a write), and four-part names
  (``server.database.schema.table``), which reach a linked server with the
  connection's credentials.
* It refused ``;WITH cte AS (...) SELECT ...``: the leading semicolon T-SQL
  idiom (a CTE must follow a terminated statement) read as a second statement.
* ``connector_request_limits.FILE_PATH_TYPES`` held the bare ``sqlite`` and
  ``duckdb`` spellings, so a connector typed with another spelling of the same
  file engine (``sqlite3``, ``sqlite_file``, ``duck_db``, ``motherduck``) had
  its file path held to the 128-character identifier bound.
"""

from __future__ import annotations

import pytest

from src.routers.query_router import _is_safe_sql


@pytest.mark.parametrize(
    "sql",
    [
        "select next value for dbo.order_seq",
        "SELECT NEXT VALUE FOR order_seq AS n FROM dbo.t",
        "select a, next value for s over (order by a) from t",
        "select * from srv.sales.dbo.orders",
        "select o.id from [srv].[sales].[dbo].[orders] o",
        "select * from t join srv.sales.dbo.orders o on o.id = t.id",
        "with c as (select * from srv.sales.dbo.orders) select * from c",
    ],
)
def test_sqlserver_refuses_sequence_side_effects_and_linked_server_names(sql):
    assert _is_safe_sql(sql, "sqlserver") is False


@pytest.mark.parametrize(
    "sql",
    [
        ";WITH cte AS (SELECT 1 AS a) SELECT a FROM cte",
        ";with c as (select 1 a) select a from c;",
        "  ;  WITH cte AS (SELECT id FROM dbo.t) SELECT id FROM cte",
        ";  ;WITH cte AS (SELECT 1 AS a) SELECT a FROM cte",
        "select * from sales.dbo.orders",  # three-part: this server, another database
        "select id from dbo.orders",
    ],
)
def test_sqlserver_accepts_the_leading_semicolon_cte_and_local_names(sql):
    assert _is_safe_sql(sql, "sqlserver") is True


@pytest.mark.parametrize(
    "sql",
    [
        ";drop table t",
        "; select 1; drop table t",
        ";WITH cte AS (SELECT 1 AS a) SELECT a FROM cte; delete from t",
        ";;",
    ],
)
def test_a_leading_semicolon_does_not_open_a_second_statement(sql):
    assert _is_safe_sql(sql, "sqlserver") is False
    assert _is_safe_sql(sql, "postgresql") is False


_LONG_PATH = (
    "/var/lib/datawrap/data/sqlite/_workspaces/"
    "0f3c9b1e-6a2d-4c8e-9b7a-1d2e3f4a5b6c/finance/2026/q3/ledger_reconciliation_snapshot_eu_west.db"
)


@pytest.mark.parametrize("db_type", ["sqlite3", "SQLite3", "sqlite_file", "duck_db", "motherduck", "DuckDB"])
def test_every_spelling_of_a_file_engine_gets_the_path_bound(db_type):
    from services.connector_request_limits import IDENT_MAX, connector_request_errors

    assert len(_LONG_PATH) > IDENT_MAX
    assert connector_request_errors(db_type=db_type, database=_LONG_PATH) == ""


@pytest.mark.parametrize("db_type", ["postgresql", "sqlserver", "mysql", "clickhouse"])
def test_network_engines_keep_the_identifier_bound(db_type):
    from services.connector_request_limits import IDENT_MAX, connector_request_errors

    assert connector_request_errors(db_type=db_type, database="d" * (IDENT_MAX + 1)) == (
        f"Database is longer than {IDENT_MAX} characters."
    )
