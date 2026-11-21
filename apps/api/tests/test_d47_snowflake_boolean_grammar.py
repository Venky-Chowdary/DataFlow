"""D47 (QA batch 1) — SCD2 and mirror into Snowflake failed at compile time.

Production (real Snowflake): SCD2 failed on the history merge with
``001003 SQL compilation error``; mirror with ``syntax error ... unexpected
'IS'``. Both engines filtered their lattice columns with the ANSI boolean
test ``"IS_CURRENT" IS TRUE`` / ``"_DELETED" IS NOT TRUE``. Snowflake's
``IS`` forms are only ``IS [NOT] NULL`` and ``IS [NOT] DISTINCT FROM``;
fakesnow runs on DuckDB, which accepts the boolean test, so the suite was
green. ``tests/snowflake_grammar.py`` now enforces the construct in the
strict Snowflake fixture, and these tests pin the generated SQL.
"""

from __future__ import annotations

import re
import uuid
from typing import Any

import pytest

from services.dialect_profiles import (
    lacks_boolean_test_predicate,
    sql_bool_is_not_true,
    sql_bool_is_true,
)
from tests.snowflake_grammar import snowflake_grammar_violations

sqlglot = pytest.importorskip("sqlglot")


def test_grammar_checker_flags_boolean_tests_but_not_literals_or_identifiers():
    assert snowflake_grammar_violations('SELECT 1 FROM t WHERE "C" IS TRUE')
    assert snowflake_grammar_violations('UPDATE t SET a = 1 WHERE "C" is not true')
    assert snowflake_grammar_violations("SELECT 1 WHERE x IS NOT FALSE")
    assert not snowflake_grammar_violations("SELECT 'x IS TRUE' FROM t")
    assert not snowflake_grammar_violations('SELECT "IS TRUE" FROM t')
    assert not snowflake_grammar_violations("SELECT 1 WHERE a IS NOT NULL")
    assert not snowflake_grammar_violations(
        "SELECT 1 WHERE a IS NOT DISTINCT FROM b"
    )


@pytest.mark.parametrize("driver", ["snowflake", "SNOWFLAKE"])
def test_snowflake_boolean_predicates_compile_as_snowflake(driver):
    assert lacks_boolean_test_predicate(driver)
    col = '"IS_CURRENT"'
    for pred in (sql_bool_is_true(driver, col), sql_bool_is_not_true(driver, col)):
        stmt = f'UPDATE "PUBLIC"."T" SET "X" = 1 WHERE "ID" = 1 AND {pred}'
        assert snowflake_grammar_violations(stmt) == []
        sqlglot.parse_one(stmt, read="snowflake")
    # NULL is neither current nor "true": the NOT TRUE form must keep NULLs.
    assert "IS NULL" in sql_bool_is_not_true(driver, col)


@pytest.mark.parametrize("driver", ["postgresql", "mysql", "bigquery", "redshift"])
def test_other_boolean_engines_keep_the_ansi_test(driver):
    assert not lacks_boolean_test_predicate(driver)
    assert sql_bool_is_true(driver, "c") == "c IS TRUE"
    assert sql_bool_is_not_true(driver, "c") == "c IS NOT TRUE"


def test_snowflake_upsert_merge_parses_as_snowflake():
    from connectors.snowflake_writer import build_snowflake_merge_sql

    sql = build_snowflake_merge_sql(
        "T", "_DF_UPSERT_X", ["id", "name", "_deleted"], ["id"],
    )
    assert snowflake_grammar_violations(sql) == []
    tree = sqlglot.parse_one(sql, read="snowflake")
    assert tree.key == "merge"


# --- every statement the engines send fakesnow during SCD2 + soft mirror ---

from tests import test_d47_snowflake_scd2_mirror as _d47  # noqa: E402

snowflake_semantics = _d47.snowflake_semantics
engine_env = _d47.engine_env
pg_source = _d47.pg_source

_DML = ("SELECT", "UPDATE", "DELETE", "INSERT", "MERGE", "WITH")


@pytest.fixture()
def captured(monkeypatch, snowflake_semantics):
    from fakesnow.cursor import FakeSnowflakeCursor

    seen: list[str] = []
    strict = FakeSnowflakeCursor.execute

    def execute(self, command: str, *args: Any, **kwargs: Any) -> Any:
        seen.append(command or "")
        return strict(self, command, *args, **kwargs)

    monkeypatch.setattr(FakeSnowflakeCursor, "execute", execute)
    return seen


def _lattice_statements(seen: list[str], marker: str) -> list[str]:
    return [
        s for s in seen
        if marker.upper() in s.upper() and s.lstrip().upper().startswith(_DML)
    ]


def _assert_snowflake_grammar(statements: list[str]) -> None:
    assert statements
    for stmt in statements:
        assert snowflake_grammar_violations(stmt) == [], stmt
        # The connector's pyformat binds, as Snowflake ``?`` placeholders.
        bound = re.sub(r"%\(\w+\)s|%s", "?", stmt)
        sqlglot.parse_one(bound, read="snowflake")


def test_scd2_into_snowflake_sends_only_snowflake_grammar(engine_env, pg_source, captured):
    conn, src = pg_source
    dst = "d47_scd2g_" + uuid.uuid4().hex[:8]
    _d47._run(engine_env, src, dst, "scd2")
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {src} SET name = 'b2' WHERE id = 2")
    _d47._run(engine_env, src, dst, "scd2")
    assert _d47._dst(dst, '"ID", "NAME"') == [(1, "a"), (2, "b"), (2, "b2"), (3, "c")]
    _assert_snowflake_grammar(_lattice_statements(captured, "IS_CURRENT"))


def test_soft_mirror_into_snowflake_tombstones_and_reactivates(engine_env, pg_source, captured):
    """Default (soft) mirror: the ``_deleted`` lattice path QA hit."""
    conn, src = pg_source
    dst = "d47_mirg_" + uuid.uuid4().hex[:8]
    _d47._run(engine_env, src, dst, "mirror")
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {src} WHERE id = 3")
    _d47._run(engine_env, src, dst, "mirror")
    rows = _d47._dst(dst, '"ID", "_DELETED"')
    assert rows == [(1, False), (2, False), (3, True)]
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {src} VALUES (3, 'c', 3.25)")
    _d47._run(engine_env, src, dst, "mirror")
    assert _d47._dst(dst, '"ID", "_DELETED"') == [(1, False), (2, False), (3, False)]
    _assert_snowflake_grammar(_lattice_statements(captured, "_DELETED"))
