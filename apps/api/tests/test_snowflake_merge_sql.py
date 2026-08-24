"""Unit tests for Snowflake MERGE SQL (CDC upsert + LSN guard)."""

from __future__ import annotations

from connectors.snowflake_writer import build_snowflake_merge_sql
from connectors.writer_common import DF_LSN_COL, snowflake_lsn_match_predicate


def test_snowflake_merge_sql_includes_composite_pk_and_lsn_guard():
    sql = build_snowflake_merge_sql(
        "ORDERS",
        "_DF_UPSERT_abc",
        ["id", "tenant", "amount", DF_LSN_COL],
        ["id", "tenant"],
        lsn_column=DF_LSN_COL,
    )
    assert 'MERGE INTO "ORDERS" t' in sql
    assert 'USING "_DF_UPSERT_abc" s' in sql
    assert 't."id" = s."id"' in sql
    assert 't."tenant" = s."tenant"' in sql
    assert '(t."id" IS NULL AND s."id" IS NULL)' in sql
    assert '(t."tenant" IS NULL AND s."tenant" IS NULL)' in sql
    assert snowflake_lsn_match_predicate() in sql
    assert "WHEN MATCHED AND" in sql
    assert 'THEN UPDATE SET t."amount" = s."amount"' in sql
    assert f't."{DF_LSN_COL}" = s."{DF_LSN_COL}"' in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql


def test_snowflake_merge_sql_without_lsn():
    sql = build_snowflake_merge_sql(
        "T",
        "S",
        ["id", "amount"],
        ["id"],
    )
    assert 'WHEN MATCHED THEN UPDATE SET t."amount" = s."amount"' in sql
    assert DF_LSN_COL not in sql


def test_snowflake_merge_sql_pk_only_inserts_unmatched():
    sql = build_snowflake_merge_sql(
        "KEYS",
        "STG",
        ["id"],
        ["id"],
    )
    assert "WHEN MATCHED" not in sql
    assert 'WHEN NOT MATCHED THEN INSERT ("id") VALUES (s."id")' in sql


def test_snowflake_merge_sql_requires_conflict_columns():
    try:
        build_snowflake_merge_sql("T", "S", ["id", "amount"], ["missing"])
    except ValueError as exc:
        assert "conflict_columns" in str(exc)
    else:
        raise AssertionError("expected ValueError for missing conflict columns")
