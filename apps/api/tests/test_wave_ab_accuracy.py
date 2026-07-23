"""Wave AB accuracy: cursor totals, Excel honesty, HubSpot/Redis refuse, SaaS totals, int coerce."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_mysql_cursor_batch_total_rows_is_none():
    from connectors.mysql_reader import read_table_cursor_batch

    cur = MagicMock()
    cur.description = [("id",), ("ts",)]
    cur.fetchall.return_value = [(1, "a"), (2, "b")]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    with patch("connectors.mysql_reader.get_connection", return_value=conn):
        batch = read_table_cursor_batch(
            host="h",
            port=3306,
            database="d",
            username="u",
            password="p",
            schema="",
            connection_string="",
            ssl=False,
            table="t",
            columns=["id", "ts"],
            cursor_column="id",
            limit=2,
        )
    assert len(batch.rows) == 2
    assert batch.total_rows is None


def test_postgres_cursor_batch_total_rows_is_none():
    pytest.importorskip("psycopg2")
    from connectors.postgresql_reader import read_table_cursor_batch

    cur = MagicMock()
    cur.description = [("id",), ("ts",)]
    cur.fetchall.return_value = [(1, "a")]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    with patch("connectors.postgresql_reader.get_connection", return_value=conn):
        batch = read_table_cursor_batch(
            host="h",
            port=5432,
            database="d",
            username="u",
            password="p",
            schema="public",
            connection_string="",
            ssl=False,
            table="t",
            columns=["id", "ts"],
            cursor_column="id",
            limit=10,
        )
    assert batch.total_rows is None


def test_snowflake_cursor_batch_total_rows_is_none():
    from connectors.snowflake_reader import read_table_cursor_batch

    cur = MagicMock()
    cur.description = [("ID",), ("TS",)]
    cur.fetchall.return_value = [(1, "a")]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    with patch("connectors.snowflake_reader.normalize_account", return_value="acct"), patch(
        "connectors.snowflake_reader.get_connection", return_value=conn
    ), patch("connectors.snowflake_reader._use_warehouse"), patch(
        "connectors.snowflake_reader._table_ref", return_value='"PUBLIC"."T"'
    ):
        batch = read_table_cursor_batch(
            host="acct",
            port=443,
            database="DB",
            username="u",
            password="p",
            schema="PUBLIC",
            connection_string="",
            warehouse="WH",
            table="T",
            columns=["ID", "TS"],
            cursor_column="ID",
            limit=10,
        )
    assert batch.total_rows is None


def test_bigquery_cursor_batch_total_rows_is_none():
    from connectors.bigquery_reader import read_table_cursor_batch

    job = MagicMock()
    job.schema = [MagicMock(name="id"), MagicMock(name="ts")]
    job.schema[0].name = "id"
    job.schema[1].name = "ts"
    row = MagicMock()
    row.values.return_value = [1, "a"]
    job.result.return_value = [row]
    client = MagicMock()
    client.query.return_value = job

    with patch("connectors.bigquery_conn.get_client", return_value=client), patch(
        "google.cloud.bigquery.ScalarQueryParameter",
        side_effect=lambda name, typ, value: {"name": name, "type": typ, "value": value},
    ), patch(
        "google.cloud.bigquery.QueryJobConfig",
        side_effect=lambda **kwargs: kwargs,
    ):
        batch = read_table_cursor_batch(
            host="proj",
            port=443,
            database="proj",
            username="",
            password="",
            schema="ds",
            connection_string="",
            ssl=False,
            table="t",
            columns=["id", "ts"],
            cursor_column="id",
            limit=10,
        )
    assert batch.total_rows is None


def test_excel_over_max_rows_fail_closed(monkeypatch):
    from services.file_parser import FileParser
    import services.excel_parser as ep

    def fake_batches(content, chunk_size):
        yield [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}, {"id": "3", "name": "c"}]

    monkeypatch.setattr(ep, "iter_excel_batches", fake_batches)
    result = FileParser.parse_excel(b"fake", max_rows=2)
    assert result.success is False
    assert "streaming" in (result.error or "").lower()


def test_excel_stream_refuses_wider_data_row(monkeypatch):
    from src.transfer import file_stream as fs
    import types

    class FakeWs:
        def iter_rows(self, values_only=True):
            yield ("a", "b")
            yield (1, 2, 3)

    class FakeWb:
        active = FakeWs()

        def close(self):
            return None

    stub = types.ModuleType("openpyxl")
    stub.load_workbook = lambda *a, **k: FakeWb()
    monkeypatch.setitem(sys.modules, "openpyxl", stub)

    with pytest.raises(ValueError, match="refuse silent column drop"):
        list(fs._excel_batches(b"fake", chunk_size=10))


def test_hubspot_page_ceiling_refuses_partial():
    import connectors.hubspot as hs

    calls = {"n": 0}

    def fake_request(**kwargs):
        calls["n"] += 1
        r = MagicMock()
        r.raise_for_status = MagicMock()
        r.json.return_value = {
            "results": [{"id": str(calls["n"]), "properties": {"email": "a@b.c"}}],
            "paging": {"next": {"after": f"c{calls['n']}"}},
        }
        return r

    with (
        patch.object(hs, "request", side_effect=fake_request),
        patch.object(hs, "describe_properties", return_value=[{"name": "email"}]),
    ):
        # limit=1000 → max_pages=50; 1 row/page keeps records < limit at ceiling.
        with pytest.raises(RuntimeError, match="safety ceiling"):
            hs.read_object(
                cfg={"api_key": "fake-token", "host": "https://api.hubapi.com"},
                object="contacts",
                limit=1000,
            )


def test_redis_list_over_cap_refuses():
    from connectors.redis_reader import _REDIS_COLLECTION_CAP, _read_redis_value

    client = MagicMock()
    client.llen.return_value = _REDIS_COLLECTION_CAP + 1
    with pytest.raises(RuntimeError, match="refuse silent truncation"):
        _read_redis_value(client, "biglist", "list")


def test_rest_paginated_total_rows_none(monkeypatch):
    from connectors import rest_api as ra

    def fake_read_page(cfg, pagination, next_url=None):
        return [{"id": 1}], None, False

    monkeypatch.setattr(ra, "_read_page", fake_read_page)
    monkeypatch.setattr(
        ra,
        "_resolve_config",
        lambda cfg: {
            **cfg,
            "pagination_type": "offset",
            "offset_param": "offset",
            "limit_param": "limit",
            "page_param": "page",
            "cursor_param": "cursor",
            "data_path": "",
        },
    )
    batch = ra.read_object(cfg={"host": "http://example"}, limit=10, offset=0)
    assert len(batch.rows) == 1
    assert batch.total_rows is None


def test_stripe_total_rows_none():
    from connectors import stripe as st

    def fake_request(**kwargs):
        r = MagicMock()
        r.raise_for_status = MagicMock()
        r.json.return_value = {
            "data": [{"id": "cus_1", "email": "a@b.c"}],
            "has_more": True,
        }
        return r

    with patch.object(st, "request", side_effect=fake_request):
        batch = st.read_object(
            cfg={"api_key": "sk_test", "host": "https://api.stripe.com"},
            object="customers",
            limit=1,
        )
    assert len(batch.rows) == 1
    assert batch.total_rows is None


def test_integer_bind_refuses_non_integral_float():
    from connectors.generic_sql import _to_sa_value

    with pytest.raises(ValueError, match="non-integral"):
        _to_sa_value(3.7, "integer")
    with pytest.raises(ValueError, match="non-integral"):
        _to_sa_value(Decimal("3.7"), "integer")
    assert _to_sa_value(3.0, "integer") == 3
    assert _to_sa_value(42, "integer") == 42


def test_csv_strict_utf8_decode():
    from services.file_parser import FileParser

    bad = b"id,name\n1,\xff\xfe"
    result = FileParser.parse_csv(bad)
    assert result.success is False
    assert "utf-8" in (result.error or "").lower()
