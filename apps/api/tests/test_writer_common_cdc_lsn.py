"""Unit tests for CDC LSN merge helpers (monotonic apply contract)."""

from __future__ import annotations

from connectors.writer_common import (
    DF_LSN_COL,
    compare_lsn,
    dedupe_rows_by_pk_and_lsn,
    extract_cdc_lsn,
    lsn_is_newer,
    lsn_sort_key,
    mysql_lsn_values_newer_sql,
    postgres_lsn_update_guard_sql,
    snowflake_lsn_match_predicate,
    sqlite_lsn_update_guard_sql,
)


def test_lsn_sort_key_orders_pg_lsn():
    assert lsn_sort_key("0/16B3700") < lsn_sort_key("0/16B3748")
    assert compare_lsn("0/16B3748", "0/16B3700") == 1
    assert compare_lsn("0/16B3700", "0/16B3748") == -1
    assert compare_lsn("0/16B3700", "0/16B3700") == 0


def test_lsn_sort_key_orders_mysql_file_pos():
    """file:pos must compare by file then integer pos — not raw lexicographic."""
    older = extract_cdc_lsn({"file": "mysql-bin.000009", "pos": 999})
    newer = extract_cdc_lsn({"file": "mysql-bin.000009", "pos": 1000})
    assert older is not None and newer is not None
    assert compare_lsn(newer, older) == 1
    assert not lsn_is_newer(older, newer)
    assert lsn_is_newer(newer, older)
    # Later file wins even when pos is smaller.
    next_file = extract_cdc_lsn({"file": "mysql-bin.000010", "pos": 1})
    assert compare_lsn(next_file, newer) == 1


def test_extract_cdc_lsn_from_encoded_token():
    token = "slot=df_test|phase=streaming|lsn=0/16B3748"
    assert extract_cdc_lsn(token) == "0/16B3748"
    assert extract_cdc_lsn({"file": "mysql-bin.000003", "pos": 1234}) == "mysql-bin.000003:00000000000000001234"
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
    pg = postgres_lsn_update_guard_sql("orders")
    assert DF_LSN_COL in pg
    assert "pg_lsn" in pg
    # Older opaque stamps must not win via IS DISTINCT FROM.
    assert "IS DISTINCT FROM" not in pg
    assert ">" in pg
    assert DF_LSN_COL in snowflake_lsn_match_predicate()
    mysql = mysql_lsn_values_newer_sql()
    assert "VALUES(" in mysql and "SUBSTRING_INDEX" in mysql
    sqlite = sqlite_lsn_update_guard_sql("orders")
    assert "excluded." in sqlite and DF_LSN_COL in sqlite
