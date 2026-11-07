"""X-19/X-20: SQL Server sources were silently excluded from the G-1d
independent SQL oracle (``_ORACLE_DIALECTS`` lacked sqlserver/mssql/
azure_sql) — every T-SQL route reported "recipe re-applied" with no
independent recompute. The oracle now emits T-SQL fragments for the ops
that have faithful equivalents; regex ops stay honestly uncovered."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import src.transfer.reconcile_step as _rs  # noqa: E402

# Defensive attribute reads: on pre-fix code these symbols do not exist and
# every test must FAIL, not error at collection.
_RECIPE_ORACLE_DIALECTS = getattr(_rs, "_RECIPE_ORACLE_DIALECTS", frozenset())
_recipe_step_sql = getattr(_rs, "_recipe_step_sql", None)


def _need_fn():
    assert _recipe_step_sql is not None, "_recipe_step_sql missing (pre-fix)"
    return _recipe_step_sql


def _call(op, opts, col, src_type):
    return _need_fn()(op, opts, col, {}, src_type=src_type)


def test_sqlserver_in_oracle_dialects():
    for d in ("sqlserver", "mssql", "azure_sql"):
        assert d in _RECIPE_ORACLE_DIALECTS


def test_tsql_trim_strips_what_the_engine_strips():
    # The engine's trim is str.strip(); LTRIM/RTRIM strip spaces only, so a
    # tab or NBSP read as a broken step (r5 multi-step, provisional D203).
    sql = _call("trim", {}, "[name]", "sqlserver")
    assert sql == "TRIM(:ov0 FROM [name])"
    assert "::text" not in sql and "BTRIM" not in sql


def test_tsql_case_and_null_if_portable():
    assert _call("case", {"mode": "upper"}, "[n]", "sqlserver") == "UPPER([n])"
    assert _call("null_if", {"values": ["x"]}, "[n]", "mssql") == "NULLIF([n], :ov0)"


def test_tsql_default_if_null_no_pg_cast():
    sql = _call("default_if_null", {"value": "d"}, "[n]", "azure_sql")
    assert sql is not None
    assert "::text" not in sql
    assert "CAST([n] AS nvarchar(max))" in sql
    assert "LTRIM(RTRIM(" in sql


def test_tsql_regex_ops_honestly_uncovered():
    assert _call("collapse_whitespace", {}, "[n]", "sqlserver") is None
    assert _call(
        "replace", {"regex": True, "search": "a+", "replacement": "b"}, "[n]", "sqlserver"
    ) is None


def test_tsql_literal_replace_and_cast_covered():
    sql = _call("replace", {"search": "a", "replacement": "b"}, "[n]", "sqlserver")
    assert sql is not None and sql.startswith("REPLACE(")
    sql = _call("cast_column", {"to_type": "text"}, "[n]", "sqlserver")
    assert sql == "CAST([n] AS nvarchar(max))"


def test_pg_and_mysql_emission_unchanged():
    assert _call("trim", {}, '"n"', "postgresql") == 'BTRIM("n", :ov0)'
    assert _call("cast_column", {"to_type": "text"}, '"n"', "postgresql") == 'CAST("n" AS TEXT)'
    assert "REGEXP_REPLACE" in _call("collapse_whitespace", {}, "`n`", "mysql")
    sql = _call("default_if_null", {"value": "d"}, "`n`", "mysql")
    assert "CAST(`n` AS CHAR)" in sql
