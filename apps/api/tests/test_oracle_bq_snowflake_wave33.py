"""Wave 33: Oracle MERGE + BigQuery/Snowflake NULL-safe MERGE ON."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_null_safe_merge_on_helper():
    from connectors.writer_common import null_safe_merge_on

    on = null_safe_merge_on(
        ["id", "tenant"],
        left_alias="T",
        right_alias="S",
        quote_column=lambda c: f"`{c}`",
    )
    assert "T.`id` = S.`id`" in on
    assert "T.`id` IS NULL AND S.`id` IS NULL" in on
    assert "T.`tenant` IS NULL AND S.`tenant` IS NULL" in on


def test_bigquery_merge_sql_null_safe_on():
    from connectors.bigquery_writer import build_bigquery_merge_sql
    from connectors.writer_common import DF_LSN_COL

    sql = build_bigquery_merge_sql(
        "proj.ds.orders",
        "proj.ds.orders_stg",
        ["id", "tenant", "amount", DF_LSN_COL],
        ["id", "tenant"],
        lsn_column=DF_LSN_COL,
    )
    assert "T.`id` = S.`id`" in sql
    assert "(T.`id` IS NULL AND S.`id` IS NULL)" in sql
    assert "(T.`tenant` IS NULL AND S.`tenant` IS NULL)" in sql
    # Must not be equality-only (Airbyte #81635 class bug).
    assert "OR" in sql.split("ON ", 1)[1].split("\n", 1)[0]


def test_snowflake_merge_uses_null_safe_on():
    from connectors.snowflake_writer import _merge_batch_via_temp

    executed: list[str] = []

    class _Cur:
        def execute(self, sql, *a, **k):  # noqa: ANN001
            executed.append(str(sql))

    class _Conn:
        pass

    with patch(
        "connectors.snowflake_writer._load_rows_into_table",
        return_value=None,
    ):
        n = _merge_batch_via_temp(
            _Cur(),
            "ORDERS",
            ["ID", "TENANT", "AMOUNT"],
            ["NUMBER", "VARCHAR", "NUMBER"],
            # Dense MERGE quarantines null/empty conflict keys; null-safe ON is
            # proven by the MERGE SQL shape below (IS NULL / OR), not by sending
            # a NULL TENANT through the stage (that would mass-touch without
            # quarantine on non-null-safe engines).
            [(1, "b", 10), (2, "a", 20)],
            ["ID", "TENANT"],
            prefer_copy=False,
            conn=_Conn(),
        )
    assert n == 2
    merge = next(s for s in executed if s.upper().startswith("MERGE"))
    assert "IS NULL" in merge.upper()
    assert "OR" in merge.upper()


def test_oracle_merge_sql_null_safe_and_ptt_stage():
    from connectors.generic_sql import _oracle_merge_upsert

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            text = str(getattr(stmt, "text", stmt))
            executed.append(text.upper())
            # First CREATE PRIVATE succeeds.
            return MagicMock()

        def rollback(self) -> None:
            executed.append("ROLLBACK")

    import sqlalchemy as sa

    table = sa.table(
        "ORDERS",
        sa.column("ID"),
        sa.column("AMOUNT"),
        schema="HR",
    )
    n = _oracle_merge_upsert(
        _Conn(),
        table,
        [{"ID": 1, "AMOUNT": 10}, {"ID": None, "AMOUNT": 2}],
        ["ID"],
        ["ID", "AMOUNT"],
        ["AMOUNT"],
    )
    assert n == 2
    blob = " ".join(executed)
    assert "MERGE INTO" in blob
    assert "IS NULL" in blob
    assert "PRIVATE TEMPORARY TABLE" in blob or "GLOBAL TEMPORARY TABLE" in blob
    assert "WHEN NOT MATCHED THEN INSERT" in blob
    assert "BY TARGET" not in blob  # Oracle syntax — not T-SQL


def test_upsert_batch_oracle_prefers_merge_then_delete_insert():
    import sqlalchemy as sa

    from connectors.generic_sql import _upsert_batch

    table = sa.table("T", sa.column("ID"), sa.column("V"))
    calls: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            text = str(getattr(stmt, "text", stmt))
            calls.append(text)
            if "MERGE" in text.upper():
                raise sa.exc.SQLAlchemyError("merge unavailable")
            if "PRIVATE TEMPORARY" in text.upper() or "GLOBAL TEMPORARY" in text.upper():
                return MagicMock()
            if text.upper().startswith("INSERT") or "INSERT INTO" in text.upper():
                result = MagicMock()
                result.rowcount = 1
                return result
            result = MagicMock()
            result.rowcount = 1
            return result

        def rollback(self) -> None:
            calls.append("ROLLBACK")

    with patch(
        "connectors.generic_sql._delete_by_keys",
        side_effect=lambda *a, **k: calls.append("DELETE_KEYS"),
    ):
        # Force PTT create to fail so we exercise GTT then MERGE fail → fallback.
        # Simpler: make all execute raise on MERGE only; PTT create ok.
        n = _upsert_batch(
            _Conn(),
            table,
            [{"ID": 1, "V": "a"}],
            ["ID"],
            ["ID", "V"],
            "oracle",
        )
    assert n == 1
    assert any("MERGE" in c.upper() for c in calls)
    assert "ROLLBACK" in calls
    assert "DELETE_KEYS" in calls
