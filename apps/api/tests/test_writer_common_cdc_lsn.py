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
    sf = snowflake_lsn_match_predicate()
    assert DF_LSN_COL in sf
    # Family-aware — not bare s."_df_lsn" > COALESCE(...).
    assert "REGEXP_LIKE" in sf
    assert "SPLIT_PART" in sf
    assert "TRY_TO_NUMBER" in sf
    mysql = mysql_lsn_values_newer_sql()
    assert "VALUES(" in mysql and "SUBSTRING_INDEX" in mysql
    sqlite = sqlite_lsn_update_guard_sql("orders")
    assert "excluded." in sqlite and DF_LSN_COL in sqlite
    # Family-aware for file:pos and PG hi/lo (not bare text > only).
    assert "instr(" in sqlite and "CAST(" in sqlite
    assert "'%/%'" in sqlite
    assert "0000000000000000" in sqlite
    pg = postgres_lsn_update_guard_sql("orders")
    assert "split_part" in pg
    assert "file:pos" not in pg  # logic present, not a comment
    assert "bigint" in pg


def test_sqlite_lsn_guard_orders_pg_hex_not_lexicographic() -> None:
    """SQLite SQL must treat 0/100 as newer than 0/20 (bare text inverts)."""
    import sqlite3

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(
            f'CREATE TABLE orders (id TEXT PRIMARY KEY, v TEXT, "{DF_LSN_COL}" TEXT)'
        )
        conn.execute(
            f'INSERT INTO orders (id, v, "{DF_LSN_COL}") VALUES (?, ?, ?)',
            ("1", "old", "0/20"),
        )
        where_sql = sqlite_lsn_update_guard_sql("orders")
        conn.execute(
            f'INSERT INTO orders (id, v, "{DF_LSN_COL}") VALUES (?, ?, ?) '
            f"ON CONFLICT(id) DO UPDATE SET v=excluded.v, "
            f'"{DF_LSN_COL}"=excluded."{DF_LSN_COL}" WHERE {where_sql}',
            ("1", "new", "0/100"),
        )
        v, lsn = conn.execute(
            f'SELECT v, "{DF_LSN_COL}" FROM orders WHERE id = ?', ("1",)
        ).fetchone()
        assert v == "new"
        assert lsn == "0/100"
        conn.execute(
            f'INSERT INTO orders (id, v, "{DF_LSN_COL}") VALUES (?, ?, ?) '
            f"ON CONFLICT(id) DO UPDATE SET v=excluded.v, "
            f'"{DF_LSN_COL}"=excluded."{DF_LSN_COL}" WHERE {where_sql}',
            ("1", "stale", "0/20"),
        )
        v2, lsn2 = conn.execute(
            f'SELECT v, "{DF_LSN_COL}" FROM orders WHERE id = ?', ("1",)
        ).fetchone()
        assert v2 == "new"
        assert lsn2 == "0/100"
    finally:
        conn.close()


def test_mysql_lsn_predicate_covers_pg_hex_family():
    """A PG-sourced CDC stamp must be comparable at a MySQL destination.

    Without a hi/lo branch every ``hi/lo`` stamp fell through to a predicate
    that is always false, so ON DUPLICATE KEY UPDATE kept the old row and CDC
    updates vanished while the run reported success.
    """
    pred = mysql_lsn_values_newer_sql()
    assert "CONV(" in pred and "SUBSTRING_INDEX" in pred
    # Casted so the hex halves compare numerically, not as CONV's string result.
    assert "CAST(CONV(" in pred
    assert compare_lsn("0/14F23958", "0/140E5260") == 1


def test_snowflake_lsn_predicate_covers_pg_hex_family():
    pred = snowflake_lsn_match_predicate()
    # Must parse hex hi/lo — bare text would order 0/100 < 0/20 incorrectly.
    assert "SPLIT_PART" in pred and "/" in pred
    assert compare_lsn("0/100", "0/20") == 1


def test_redshift_caps_advertise_lsn_guard():
    from services.cdc_effectively_once import (
        SINK_EFFECTIVELY_ONCE_ELIGIBLE,
        classify_sink_delivery,
    )
    from services.connector_capability_registry import get_connector_capability

    caps = get_connector_capability("redshift")
    assert caps.get("supports_lsn_guard") is True
    posture = classify_sink_delivery(
        dest_type="redshift", has_primary_key=True, write_mode="upsert"
    )
    assert posture["class"] == SINK_EFFECTIVELY_ONCE_ELIGIBLE
    assert posture.get("has_lsn_guard") is True


def test_mongodb_and_iceberg_classify_as_lsn_eligible():
    from services.cdc_effectively_once import (
        SINK_EFFECTIVELY_ONCE_ELIGIBLE,
        classify_sink_delivery,
    )
    from services.connector_capability_registry import get_connector_capability

    for brand in ("mongodb", "iceberg", "postgresql", "mysql", "snowflake", "bigquery"):
        assert get_connector_capability(brand).get("supports_lsn_guard") is True, brand
        posture = classify_sink_delivery(
            dest_type=brand, has_primary_key=True, write_mode="upsert"
        )
        assert posture["class"] == SINK_EFFECTIVELY_ONCE_ELIGIBLE, brand
