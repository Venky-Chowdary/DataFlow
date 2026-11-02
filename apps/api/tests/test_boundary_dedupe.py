"""Regression: incremental COPY boundary dedupe (D60/D98 follow-up).

`_sql_text_eq` must bind the watermark literal in the column's own type
domain first (typed null-safe equality), with the separator-normalized text
form kept as a fallback. A MySQL ``datetime(6)`` column's CAST-AS-CHAR form
renders ``...:00.000000`` — a pure text compare never matches a
second-precision literal, so the append anti-join passed the boundary rows
through and the INSERT failed closed on ``Duplicate entry '1'``.
"""

from __future__ import annotations

from services.copy_incremental import _sql_text_eq, boundary_append_insert_sql


def test_sql_text_eq_binds_typed_equality_for_mysql():
    sql = _sql_text_eq("mysql", "s.`updated_at`", "2025-01-01T08:00:00")
    # datetime(6) columns: typed <=> must be attempted alongside the text form
    assert "<=>" in sql
    assert "CASE WHEN" in sql  # T-form staging text fallback kept


def test_sql_text_eq_binds_typed_equality_for_pg_and_sqlite():
    pg = _sql_text_eq("pg", 's."updated_at"', "2025-01-01T08:00:00")
    assert "IS NOT DISTINCT FROM" in pg
    lite = _sql_text_eq("sqlite", 's."updated_at"', "2025-01-01T08:00:00")
    assert " IS " in lite


def test_boundary_append_insert_keeps_boundary_predicates():
    sql = boundary_append_insert_sql(
        "`dst`",
        "`_df_stg_x`",
        ["id", "name", "updated_at"],
        lambda c: f"`{c}`",
        dialect="mysql",
        cursor_col="updated_at",
        watermark="2025-01-01T08:00:00\x1f2",
        pk_col="id",
    )
    assert "NOT EXISTS" in sql
    # composite bookmark must be split to the cursor part, never bound whole
    assert "\x1f" not in sql
    assert "'2025-01-01 08:00:00'" in sql
