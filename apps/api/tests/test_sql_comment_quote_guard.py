"""A quote inside a comment must not hide a second statement (code scan).

``SELECT 1 -- '\\nDROP TABLE t; -- '`` used to pass every read-only gate: the
literal mask ran first, paired the two comment quotes, and swallowed the
DROP as string data.
"""

from __future__ import annotations

import pytest

from services.procedure_source import ProcedureSourceError, _query_is_safe, parse_callable_source
from services.sql_literals import (
    escape_sqlalchemy_literal_colons,
    mask_sql_string_literals,
    sql_call_views,
    sql_code_views,
    sql_outside_literals,
)
from src.routers.query_router import (
    _is_safe_sql,
    _is_safe_sql_fallback,
    _is_safe_sql_flat,
)

_GATES = (_is_safe_sql, _is_safe_sql_flat, _is_safe_sql_fallback, _query_is_safe)

SMUGGLED = [
    "SELECT 1 -- '\nDROP TABLE t; -- '",
    "SELECT 1 -- '\nDELETE FROM t; -- '",
    "SELECT 1 -- ';\nUPDATE t SET a = 1 -- '",
    "SELECT 1 /* ' */; DROP TABLE t; /* ' */",
    "SELECT 1 /* ' */ ; INSERT INTO t VALUES (1) -- '",
    "SELECT 1 /* /* ' */ ; DROP TABLE t; -- */ '",
    "SELECT \"a'b\" FROM t; DROP TABLE t; -- '",
    "SELECT [a'b] FROM t; DROP TABLE t -- '",
    "SELECT `a'b` FROM t; DROP TABLE t -- '",
    "SELECT 1 # '\n; DROP TABLE t; # '",
    "SELECT 'a\\' ; DROP TABLE t; -- '",
    "SELECT $$ ' $$; DROP TABLE t; -- '",
    "SELECT 1 --'\nDROP TABLE t --'",
]

READ_ONLY = [
    "SELECT 'x;y' FROM t",
    "SELECT 1 -- don't\nFROM t",
    "SELECT 'O''Brien' AS n /* it's */ FROM t;",
    'SELECT "it\'s" FROM t',
    "SELECT 1; -- trailing comment",
    "SELECT '-- not a comment; DROP TABLE t' AS s FROM t",
    "SELECT '/* x */' AS s FROM t",
]


@pytest.mark.parametrize("sql", SMUGGLED)
@pytest.mark.parametrize("gate", _GATES, ids=lambda g: g.__name__)
def test_quote_inside_comment_cannot_hide_a_write(gate, sql: str) -> None:
    assert gate(sql) is False


@pytest.mark.parametrize("sql", READ_ONLY)
@pytest.mark.parametrize("gate", _GATES, ids=lambda g: g.__name__)
def test_literals_and_comments_stay_read_only(gate, sql: str) -> None:
    assert gate(sql) is True


@pytest.mark.parametrize("gate", (_is_safe_sql, _is_safe_sql_flat, _is_safe_sql_fallback))
def test_comment_quote_cannot_hide_side_effect_function(gate) -> None:
    assert gate("SELECT 1 -- '\n, pg_sleep(10) -- '") is False
    assert gate('SELECT "pg_sleep"(10)') is False


def test_procedure_source_refuses_comment_smuggled_statement() -> None:
    with pytest.raises(ProcedureSourceError):
        parse_callable_source("CALL p() -- '\n; DROP TABLE t; -- '", dialect="postgresql", mode="procedure")


def test_mask_leaves_comment_quotes_alone() -> None:
    masked, literals = mask_sql_string_literals("SELECT 'a' -- it's\nFROM t")
    assert literals == ["'a'"]
    assert "-- it's" in masked


def test_outside_literals_blanks_comments_and_literals() -> None:
    code = sql_outside_literals("SELECT 'x;y' -- ';\n; DROP TABLE t")
    assert "x;y" not in code
    assert code.count(";") == 1
    assert "DROP TABLE t" in code


def test_every_lexing_exposes_the_hidden_statement() -> None:
    sql = "SELECT 1 -- '\nDROP TABLE t; -- '"
    for view in sql_code_views(sql):
        assert "DROP TABLE t;" in view
    assert any("pg_sleep(" in v for v in sql_call_views('SELECT "pg_sleep"(1)'))


def test_colon_escape_ignores_comment_quotes() -> None:
    sql = "SELECT :since -- it's\n, 'a:b'"
    escaped = escape_sqlalchemy_literal_colons(sql)
    assert ":since" in escaped and r"\:since" not in escaped
    assert r"'a\:b'" in escaped


@pytest.mark.parametrize("sql", [
    "SELECT 1 /*! ; DROP TABLE t */",
    "SELECT 1 /*!50000 ; DROP TABLE t; */",
    "SELECT * FROM t /*!50000 INTO OUTFILE '/tmp/x' */",
    "SELECT 1 /*!50000 */ ; DROP TABLE t",
])
@pytest.mark.parametrize("gate", _GATES, ids=lambda g: g.__name__)
def test_mysql_conditional_comment_body_is_code(gate, sql: str) -> None:
    """MySQL executes ``/*! ... */``; the guard must judge the body as code."""
    assert gate(sql) is False


@pytest.mark.parametrize("sql", [
    "SELECT /*!40001 SQL_NO_CACHE */ * FROM t",
    "SELECT /*!50000 1 */ AS one",
    "SELECT /*+ INDEX(t i) */ * FROM t",
])
@pytest.mark.parametrize("gate", _GATES, ids=lambda g: g.__name__)
def test_mysql_conditional_comment_read_hints_stay_allowed(gate, sql: str) -> None:
    assert gate(sql) is True
