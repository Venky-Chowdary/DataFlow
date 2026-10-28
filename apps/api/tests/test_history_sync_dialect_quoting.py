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
        # Snowflake stores what DataFlow creates folded and its quoted
        # identifiers are case-sensitive (D47).
        ("snowflake", '"ANALYTICS"."SP_DST"'),
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
    from services.dialect_profiles import quote_stored_identifier

    assert quote_stored_identifier("", "sp_dst") == '"sp_dst"'
    assert quote_stored_identifier("nonesuch", "sp_dst") == '"sp_dst"'


def test_snowflake_identifiers_take_the_writer_fold() -> None:
    from connectors.snowflake_conn import snowflake_column_ref
    from services.dialect_profiles import quote_stored_identifier

    for name in ("id", "valid_from", "UserId", "ID"):
        assert quote_stored_identifier("snowflake", name) == snowflake_column_ref(name, {})
    # Every other engine keeps the verbatim name.
    assert quote_stored_identifier("postgresql", "ID") == '"ID"'
    assert quote_stored_identifier("mysql", "id") == "`id`"
