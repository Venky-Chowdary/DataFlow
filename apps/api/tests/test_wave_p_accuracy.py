"""Wave P accuracy: Stripe paging, SQL Server composite/TZ, DuckDB nested DDL, introspect."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import responses

sa = pytest.importorskip("sqlalchemy")


@responses.activate
def test_stripe_follows_has_more_pages():
    import connectors.stripe as stripe

    responses.add(
        responses.GET,
        re.compile(r"https://api\.stripe\.com/v1/customers"),
        json={
            "data": [{"id": "cus_1", "email": "a@x.com"}, {"id": "cus_2", "email": "b@x.com"}],
            "has_more": True,
        },
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://api\.stripe\.com/v1/customers"),
        json={
            "data": [{"id": "cus_3", "email": "c@x.com"}],
            "has_more": False,
        },
        status=200,
    )
    batch = stripe.read_object(cfg={"api_key": "sk_test_123"}, limit=10)
    assert len(batch.rows) == 3
    assert responses.calls[1].request.params["starting_after"] == "cus_2"
    assert int(responses.calls[0].request.params["limit"]) <= 100


@responses.activate
def test_stripe_numeric_offset_is_not_starting_after():
    import connectors.stripe as stripe

    responses.add(
        responses.GET,
        re.compile(r"https://api\.stripe\.com/v1/customers"),
        json={
            "data": [
                {"id": "cus_1", "email": "a@x.com"},
                {"id": "cus_2", "email": "b@x.com"},
                {"id": "cus_3", "email": "c@x.com"},
            ],
            "has_more": False,
        },
        status=200,
    )
    batch = stripe.read_object(cfg={"api_key": "sk_test_123"}, limit=10, offset=1)
    assert "starting_after" not in (responses.calls[0].request.params or {})
    assert len(batch.rows) == 2
    assert batch.rows[0][batch.headers.index("id")] == "cus_2"


def test_generic_sql_composite_cursor_uses_or_and_not_tuple():
    from connectors.generic_sql import read_table_cursor_batch

    captured: list[str] = []

    class FakeResult:
        def fetchall(self):
            return [("2024-01-01", "b")]

    class FakeConn:
        def execute(self, stmt):
            try:
                compiled = stmt.compile(
                    dialect=sa.dialects.mssql.dialect(),
                    compile_kwargs={"literal_binds": True},
                )
                captured.append(str(compiled))
            except Exception:
                captured.append(str(stmt))
            return FakeResult()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class FakeEngine:
        def connect(self):
            return FakeConn()

        def dispose(self):
            return None

        @property
        def dialect(self):
            return sa.dialects.mssql.dialect()

    table = sa.table(
        "orders",
        sa.column("updated_at", sa.String()),
        sa.column("id", sa.String()),
        sa.column("name", sa.String()),
    )

    with patch("connectors.generic_sql._engine", return_value=FakeEngine()), patch(
        "connectors.generic_sql._reflect_table", return_value=table
    ):
        batch = read_table_cursor_batch(
            host="h",
            port=1433,
            database="db",
            username="u",
            password="p",
            schema="dbo",
            connection_string="",
            ssl=False,
            table="orders",
            cursor_column="updated_at",
            cursor_after="2024-01-01|a",
            type="sqlserver",
            columns=["updated_at", "id", "name"],
            limit=10,
            cursor_primary_key="id",
        )
    sql = " ".join(captured).upper()
    assert "OR" in sql
    assert "TUPLE_" not in sql
    assert batch.rows


def test_sqlserver_timestamptz_bind_keeps_aware_utc():
    from connectors.generic_sql import _sa_type_for_logical, _to_sa_value

    sa_t = _sa_type_for_logical("TIMESTAMPTZ", "mssql", "sqlserver")
    assert getattr(sa_t, "timezone", False) is True
    raw = "2024-06-01T12:00:00-05:00"
    out = _to_sa_value(raw, "TIMESTAMPTZ", sa_t, dialect_name="mssql", db_type="sqlserver")
    assert isinstance(out, datetime)
    assert out.tzinfo is not None
    assert out.astimezone(timezone.utc).hour == 17


def test_sqlserver_bare_datetime_still_naive():
    from connectors.generic_sql import _sa_type_for_logical, _to_sa_value

    sa_t = _sa_type_for_logical("DATETIME", "mssql", "sqlserver")
    assert getattr(sa_t, "timezone", True) in (False, None) or not getattr(sa_t, "timezone", False)
    out = _to_sa_value(
        "2024-06-01T12:00:00+00:00",
        "DATETIME",
        sa_t,
        dialect_name="mssql",
        db_type="sqlserver",
    )
    assert isinstance(out, datetime)
    assert out.tzinfo is None


def test_duckdb_array_and_json_sa_types():
    from connectors.generic_sql import _sa_type_for_logical

    arr = _sa_type_for_logical("ARRAY<INTEGER>", "duckdb", "duckdb")
    assert isinstance(arr, sa.ARRAY)
    js = _sa_type_for_logical("JSON", "duckdb", "duckdb")
    assert isinstance(js, sa.JSON)


def test_clickhouse_introspect_dispatches_to_generic_sql():
    from services.schema_introspect import introspect_schema

    with patch(
        "connectors.generic_sql.introspect_table_schema",
        return_value={"ok": True, "columns": ["id"], "schema": {"id": "INTEGER"}},
    ) as introspect:
        out = introspect_schema(
            "clickhouse",
            host="localhost",
            port=8123,
            database="default",
            username="default",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table="events",
        )
    assert out["ok"] is True
    cfg = introspect.call_args[0][0]
    assert cfg["type"] == "clickhouse"
