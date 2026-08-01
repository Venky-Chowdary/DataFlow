"""Unit tests for BigQuery MERGE SQL (CDC upsert + LSN guard)."""

from __future__ import annotations

from connectors.bigquery_writer import build_bigquery_merge_sql
from connectors.writer_common import (
    DF_LSN_COL,
    bigquery_lsn_match_predicate,
    compare_lsn,
)


def test_bigquery_merge_sql_includes_composite_pk_and_lsn_guard():
    sql = build_bigquery_merge_sql(
        "proj.ds.orders",
        "proj.ds.orders_stg",
        ["id", "tenant", "amount", DF_LSN_COL],
        ["id", "tenant"],
        lsn_column=DF_LSN_COL,
    )
    assert "MERGE `proj.ds.orders` T" in sql
    assert "USING `proj.ds.orders_stg` S" in sql
    assert "T.`id` = S.`id`" in sql
    assert "T.`tenant` = S.`tenant`" in sql
    assert "(T.`id` IS NULL AND S.`id` IS NULL)" in sql
    assert "(T.`tenant` IS NULL AND S.`tenant` IS NULL)" in sql
    # Family-aware guard (PG hex + file:pos + text fallback).
    assert "REGEXP_CONTAINS" in sql
    assert "SAFE_CAST" in sql
    assert "SPLIT(" in sql
    assert f"S.`{DF_LSN_COL}`" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    # Must not be *only* bare text compare (old bug for 0/100 vs 0/20).
    assert sql.count("REGEXP_CONTAINS") >= 1


def test_bigquery_merge_sql_without_lsn():
    sql = build_bigquery_merge_sql(
        "proj.ds.t",
        "proj.ds.s",
        ["id", "amount"],
        ["id"],
    )
    assert "WHEN MATCHED THEN UPDATE SET T.`amount` = S.`amount`" in sql
    assert DF_LSN_COL not in sql


def test_compare_lsn_pg_hex_not_lexicographic():
    """``0/100`` must be newer than ``0/20`` — text ``>`` would reverse this."""
    assert compare_lsn("0/100", "0/20") == 1
    assert compare_lsn("0/20", "0/100") == -1


def test_bigquery_lsn_predicate_covers_pg_and_filepos():
    pred = bigquery_lsn_match_predicate()
    assert "REGEXP_CONTAINS" in pred
    assert "SPLIT(" in pred
    assert "SAFE_CAST" in pred
    assert "file" not in pred.lower() or "gtid" in pred.lower()
