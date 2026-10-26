"""M5 — Snowflake column identifiers resolve live-first / folded.

``CREATE TABLE`` folded the table name (``USER_TABLE``) but quoted raw column
names — storing case-sensitive ``"user_id"`` columns no unquoted
``user_id``/``USER_ID`` read could resolve, while writes into pre-existing
lower-quoted tables need the stored name, not a blind fold. One resolver
(``_snowflake_column_ref``) now feeds every column projection.
"""

from __future__ import annotations

from connectors.snowflake_writer import (
    _snowflake_column_ref,
    build_snowflake_merge_sql,
)


def test_create_new_column_folds_like_unquoted_sql():
    # Probed catalog, table absent → fold so unquoted reads resolve.
    assert _snowflake_column_ref("user_id", {}) == '"USER_ID"'


def test_existing_lower_quoted_column_keeps_stored_name():
    physical = {"USER_ID": "user_id", "AMOUNT": "Amount"}
    assert _snowflake_column_ref("user_id", physical) == '"user_id"'
    assert _snowflake_column_ref("amount", physical) == '"Amount"'


def test_unprobed_catalog_keeps_raw_quoting():
    # No live evidence — do not guess a fold against an existing table.
    assert _snowflake_column_ref("user_id", None) == '"user_id"'


def test_merge_sql_resolves_both_sides_to_stored_names():
    sql = build_snowflake_merge_sql(
        "ORDERS",
        "_DF_UPSERT_x",
        ["user_id", "amount"],
        ["user_id"],
        physical_names={"USER_ID": "user_id", "AMOUNT": "Amount"},
    )
    assert 't."user_id" = s."user_id"' in sql
    assert 't."Amount" = s."Amount"' in sql
    assert 'INSERT ("user_id", "Amount")' in sql


def test_merge_sql_folds_on_missing_table():
    sql = build_snowflake_merge_sql(
        "ORDERS", "_DF_UPSERT_x", ["user_id"], ["user_id"], physical_names={}
    )
    assert '"USER_ID"' in sql
