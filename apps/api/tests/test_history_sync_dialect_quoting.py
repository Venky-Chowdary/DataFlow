"""Mirror / SCD2 SQL must be quoted for the destination engine, not ANSI-only.

MySQL parses ``"sp_dst"`` as a string literal, so a dialect-blind soft-delete
pass raised ``(1064, ... near '"sp_dst" SET "_deleted" = FALSE')`` and mirror
could never tombstone a MySQL destination.
"""

from __future__ import annotations

import pytest

from services import mirror_engine, scd2_engine
from src.transfer import stream_scd2


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        ("mysql", "`analytics`.`sp_dst`"),
        ("postgresql", '"analytics"."sp_dst"'),
        ("snowflake", '"analytics"."sp_dst"'),
    ],
)
def test_qualified_names_follow_the_destination_dialect(dialect, expected) -> None:
    assert mirror_engine._qualified_name("sp_dst", "analytics", dialect) == expected
    assert scd2_engine._qualified_name("sp_dst", "analytics", dialect) == expected
    assert stream_scd2._qualified("sp_dst", "analytics", dialect) == expected


def test_unqualified_name_still_quotes_per_dialect() -> None:
    assert mirror_engine._qualified_name("sp_dst", None, "mysql") == "`sp_dst`"
    assert stream_scd2._qualified("sp_dst", "", "mysql") == "`sp_dst`"


def test_pk_predicates_are_quoted_for_mysql() -> None:
    clause, params = mirror_engine._pk_or_clause(
        ["id"], ["7"], prefix="k", dialect="mysql"
    )
    assert "`id`" in clause and '"id"' not in clause
    assert params

    scd_clause, scd_params = scd2_engine._pk_or_clause(
        ["id"], {"7"}, prefix="k", dialect="mysql"
    )
    assert "`id`" in scd_clause and '"id"' not in scd_clause
    assert scd_params


def test_unknown_dialect_falls_back_to_ansi_quotes() -> None:
    assert mirror_engine._qchar("") == '"'
    assert scd2_engine._qchar("nonesuch") == '"'
