"""Unit tests for BigQuery MERGE SQL (CDC upsert + LSN guard)."""

from __future__ import annotations

from connectors.bigquery_writer import build_bigquery_merge_sql
from connectors.writer_common import DF_LSN_COL


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
    assert f"S.`{DF_LSN_COL}` > COALESCE(T.`{DF_LSN_COL}`, '')" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql


def test_bigquery_merge_sql_without_lsn():
    sql = build_bigquery_merge_sql(
        "proj.ds.t",
        "proj.ds.s",
        ["id", "amount"],
        ["id"],
    )
    assert "WHEN MATCHED THEN UPDATE SET T.`amount` = S.`amount`" in sql
    assert DF_LSN_COL not in sql
