"""Wave 31: Stripe/Shopify/Zendesk/Notion Gate-8 + SQL Server MERGE shape."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_verify_target_routes_commerce_saas():
    from services.reconciliation import verify_target

    for driver, fn in (
        ("stripe", "verify_stripe_object"),
        ("shopify", "verify_shopify_object"),
        ("zendesk", "verify_zendesk_object"),
        ("notion", "verify_notion_object"),
    ):
        with patch(
            f"services.reconciliation.{fn}",
            return_value=(2, driver[:2]),
        ) as mocked:
            assert verify_target(
                driver,
                {"password": "tok", "database": "db1"},
                schema="",
                table_name="customers" if driver != "zendesk" else "tickets",
                fallback_rows=-1,
                fallback_checksum="",
            ) == (2, driver[:2])
            assert mocked.called


def test_gate8_writer_meta_shape():
    from connectors.writer_common import gate8_writer_meta

    meta = gate8_writer_meta(
        [{"id": "cus_1", "email": "a@x.com"}],
        ["id", "email"],
        ["cus_1"],
    )
    assert meta["source_row_count"] == 1
    assert meta["written_ids"] == ["cus_1"]
    assert meta["reconcile_sample"][0]["email"] == "a@x.com"


def test_read_target_sample_routes_stripe():
    from services.reconciliation import read_target_sample

    fake_batch = MagicMock()
    fake_batch.headers = ["id", "email"]
    fake_batch.rows = [{"id": "cus_1", "email": "a@x.com"}]

    with patch("connectors.stripe.read_object", return_value=fake_batch):
        rows = read_target_sample(
            "stripe",
            {"password": "sk_test"},
            schema="",
            table_name="customers",
            columns=["id", "email"],
            limit=10,
        )
    assert rows == [{"id": "cus_1", "email": "a@x.com"}]


def test_mssql_merge_sql_null_safe_holdlock_and_fallback():
    from connectors.generic_sql import _mssql_merge_upsert

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            text = str(getattr(stmt, "text", stmt))
            executed.append(text)

    import sqlalchemy as sa

    table = sa.table(
        "orders",
        sa.column("id"),
        sa.column("amount"),
        schema="dbo",
    )
    n = _mssql_merge_upsert(
        _Conn(),
        table,
        [{"id": 1, "amount": 10}, {"id": None, "amount": 2}],
        ["id"],
        ["id", "amount"],
        ["amount"],
    )
    assert n == 2
    merged = " ".join(executed).upper()
    assert "MERGE" in merged
    assert "HOLDLOCK" in merged
    assert "IS NULL" in merged
    assert "SELECT TOP 0" in merged
    assert "[DBO].[ORDERS]" in merged or "DBO" in merged


def test_upsert_batch_mssql_prefers_merge_then_delete_insert():
    import sqlalchemy as sa

    from connectors.generic_sql import _upsert_batch

    table = sa.table(
        "t",
        sa.column("id"),
        sa.column("v"),
    )
    calls: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            text = str(getattr(stmt, "text", stmt))
            calls.append(text)
            if "MERGE" in text.upper():
                raise sa.exc.SQLAlchemyError("merge unavailable")
            result = MagicMock()
            result.rowcount = 1
            return result

        def rollback(self) -> None:
            calls.append("ROLLBACK")

    with patch(
        "connectors.generic_sql._delete_by_keys",
        side_effect=lambda *a, **k: calls.append("DELETE_KEYS"),
    ):
        n = _upsert_batch(
            _Conn(),
            table,
            [{"id": 1, "v": "a"}],
            ["id"],
            ["id", "v"],
            "mssql",
        )
    assert n == 1
    assert any("MERGE" in c.upper() for c in calls)
    assert "ROLLBACK" in calls
    assert "DELETE_KEYS" in calls
