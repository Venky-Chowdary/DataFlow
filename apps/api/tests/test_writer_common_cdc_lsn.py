"""Unit tests for CDC LSN merge helpers (monotonic apply contract)."""

from __future__ import annotations

from connectors.writer_common import (
    DF_LSN_COL,
    compare_lsn,
    dedupe_rows_by_pk_and_lsn,
    extract_cdc_lsn,
    lsn_sort_key,
    postgres_lsn_update_guard_sql,
    snowflake_lsn_match_predicate,
)


def test_lsn_sort_key_orders_pg_lsn():
    assert lsn_sort_key("0/16B3700") < lsn_sort_key("0/16B3748")
    assert compare_lsn("0/16B3748", "0/16B3700") == 1
    assert compare_lsn("0/16B3700", "0/16B3748") == -1
    assert compare_lsn("0/16B3700", "0/16B3700") == 0


def test_extract_cdc_lsn_from_encoded_token():
    token = "slot=df_test|phase=streaming|lsn=0/16B3748"
    assert extract_cdc_lsn(token) == "0/16B3748"
    assert extract_cdc_lsn({"file": "mysql-bin.000003", "pos": 1234}) == "mysql-bin.000003:1234"
    assert extract_cdc_lsn(None) is None


def test_dedupe_rows_by_pk_and_lsn_keeps_newest():
    cols = ["id", "amount", DF_LSN_COL]
    rows = [
        ("1", "10", "0/16B3700"),
        ("1", "99", "0/16B3748"),
        ("1", "11", "0/16B3710"),  # older than 99
        ("2", "20", "0/16B3700"),
    ]
    out = dedupe_rows_by_pk_and_lsn(rows, ["id"], cols)
    by_id = {r[0]: r for r in out}
    assert by_id["1"][1] == "99"
    assert by_id["1"][2] == "0/16B3748"
    assert by_id["2"][1] == "20"


def test_sql_guards_mention_lsn_column():
    assert DF_LSN_COL in postgres_lsn_update_guard_sql("orders")
    assert "pg_lsn" in postgres_lsn_update_guard_sql("orders")
    assert DF_LSN_COL in snowflake_lsn_match_predicate()
