"""Refuse SaaS default-id conflict invent and Mongo typed pass-through invent."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_snapshot_window_refuses_default_id_invent():
    from services.cdc_snapshot_window import SnapshotWindow

    with pytest.raises(ValueError, match="primary_key"):
        SnapshotWindow(window_id="w-empty")


def test_shopify_upsert_refuses_default_id_conflict_invent():
    from connectors.shopify_writer import write_mapped_rows

    with patch("connectors.shopify_writer.request") as req:
        with patch(
            "connectors.shopify.describe_metafield_definitions",
            return_value=[],
        ):
            result = write_mapped_rows(
                host="demo.myshopify.com",
                table_name="products",
                api_key="tok",
                headers=["title"],
                data_rows=[["x"]],
                mappings=[{"source": "title", "target": "title"}],
                column_types={"title": "VARCHAR"},
                write_mode="upsert",
                conflict_columns=[],
                error_policy="fail",
                port=0,
                database="",
                username="",
                password="",
                schema="",
                connection_string="",
                ssl=True,
            )
    assert result.ok is False
    assert "refuse inventing default 'id'" in (result.error or "")
    req.assert_not_called()


def test_zendesk_upsert_refuses_default_id_conflict_invent():
    from connectors.zendesk_writer import write_mapped_rows

    with patch("connectors.zendesk_writer.request") as req:
        result = write_mapped_rows(
            host="https://demo.zendesk.com",
            table_name="tickets",
            api_key="user@x.com:tok",
            headers=["subject"],
            data_rows=[["x"]],
            mappings=[{"source": "subject", "target": "subject"}],
            column_types={"subject": "VARCHAR"},
            write_mode="upsert",
            conflict_columns=[],
            error_policy="fail",
            port=0,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
        )
    assert result.ok is False
    assert "refuse inventing default 'id'" in (result.error or "")
    req.assert_not_called()


def test_notion_upsert_refuses_default_id_conflict_invent():
    from connectors.notion_writer import write_mapped_rows

    db_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    with patch(
        "connectors.notion_writer._fetch_database_properties",
        return_value=({"title": "title"}, {}),
    ):
        with patch("connectors.notion_writer.request") as req:
            result = write_mapped_rows(
                host="",
                table_name=db_id,
                api_key="tok",
                headers=["title"],
                data_rows=[["x"]],
                mappings=[{"source": "title", "target": "title"}],
                column_types={"title": "VARCHAR"},
                write_mode="upsert",
                conflict_columns=[],
                error_policy="fail",
                database=db_id,
                port=0,
                username="",
                password="",
                schema="",
                connection_string="",
                ssl=True,
            )
    assert result.ok is False
    assert "refuse inventing default 'id'" in (result.error or "")
    req.assert_not_called()


def test_shopify_upsert_refuses_secondary_conflict_as_id():
    """Empty id must not fall through to sku/handle as Admin REST identity."""
    from connectors.shopify_writer import write_mapped_rows

    with patch("connectors.shopify_writer.request") as req:
        with patch(
            "connectors.shopify.describe_metafield_definitions",
            return_value=[],
        ):
            result = write_mapped_rows(
                host="demo.myshopify.com",
                table_name="products",
                api_key="tok",
                headers=["id", "sku"],
                data_rows=[["", "SKU-9"]],
                mappings=[
                    {"source": "id", "target": "id"},
                    {"source": "sku", "target": "sku"},
                ],
                column_types={"id": "VARCHAR", "sku": "VARCHAR"},
                write_mode="upsert",
                conflict_columns=["id", "sku"],
                error_policy="fail",
                port=0,
                database="",
                username="",
                password="",
                schema="",
                connection_string="",
                ssl=True,
            )
    assert result.ok is False
    assert "missing id" in (result.error or "").lower()
    req.assert_not_called()


def test_zendesk_upsert_refuses_secondary_conflict_as_id():
    from connectors.zendesk_writer import write_mapped_rows

    with patch("connectors.zendesk_writer.request") as req:
        result = write_mapped_rows(
            host="https://demo.zendesk.com",
            table_name="tickets",
            api_key="user@x.com:tok",
            headers=["id", "external_id"],
            data_rows=[["", "99"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "external_id", "target": "external_id"},
            ],
            column_types={"id": "VARCHAR", "external_id": "VARCHAR"},
            write_mode="upsert",
            conflict_columns=["id", "external_id"],
            error_policy="fail",
            port=0,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
        )
    assert result.ok is False
    assert "missing numeric id" in (result.error or "").lower()
    req.assert_not_called()


def test_es_float_refuses_string_invent():
    from connectors.elasticsearch_writer import _to_es_value

    assert _to_es_value("1.5", "FLOAT") == 1.5
    with pytest.raises(ValueError, match="FLOAT refused"):
        _to_es_value("not-a-float", "FLOAT")


def test_hubspot_upsert_refuses_default_email_invent():
    from connectors.hubspot_writer import write_mapped_rows
    from unittest.mock import patch

    with patch("connectors.hubspot_writer.request") as req:
        result = write_mapped_rows(
            host="api.hubapi.com",
            table_name="contacts",
            api_key="tok",
            headers=["firstname"],
            data_rows=[["Ada"]],
            mappings=[{"source": "firstname", "target": "firstname"}],
            column_types={"firstname": "VARCHAR"},
            write_mode="upsert",
            conflict_columns=[],
            error_policy="fail",
            port=0,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
        )
    assert result.ok is False
    assert "refuse inventing default 'email'" in (result.error or "")
    req.assert_not_called()


def test_mongo_boolean_float_int_refuse_passthrough_invent():
    pytest.importorskip("bson")
    from connectors.mongodb_writer import write_mapped_rows

    client = MagicMock()
    coll = MagicMock()
    client.__getitem__.return_value.__getitem__.return_value = coll
    base = dict(
        host="localhost",
        port=27017,
        database="db",
        username="",
        password="",
        schema="",
        connection_string="mongodb://localhost:27017",
        ssl=False,
        table_name="t",
        create_table=True,
        write_mode="insert",
        error_policy="quarantine",
    )

    with patch("connectors.mongodb_common._mongo_client", return_value=client):
        bool_r = write_mapped_rows(
            **base,
            headers=["flag"],
            data_rows=[["maybe"]],
            mappings=[{"source": "flag", "target": "flag"}],
            column_types={"flag": "BOOLEAN"},
        )
        float_r = write_mapped_rows(
            **base,
            headers=["amt"],
            data_rows=[["not-a-float"]],
            mappings=[{"source": "amt", "target": "amt"}],
            column_types={"amt": "FLOAT"},
        )
        int_r = write_mapped_rows(
            **base,
            headers=["qty"],
            data_rows=[["12.5"]],
            mappings=[{"source": "qty", "target": "qty"}],
            column_types={"qty": "INTEGER"},
        )
        json_r = write_mapped_rows(
            **base,
            headers=["doc"],
            data_rows=[["not-json{"]],
            mappings=[{"source": "doc", "target": "doc"}],
            column_types={"doc": "JSON"},
        )
    assert bool_r.rejected_rows >= 1 and bool_r.rows_written == 0
    assert float_r.rejected_rows >= 1 and float_r.rows_written == 0
    assert int_r.rejected_rows >= 1 and int_r.rows_written == 0
    assert json_r.rejected_rows >= 1 and json_r.rows_written == 0
    assert coll.insert_many.call_count == 0
