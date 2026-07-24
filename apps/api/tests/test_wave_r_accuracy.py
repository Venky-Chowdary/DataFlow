"""Wave R accuracy: deny-create BigQuery/Mongo/ES, XML cap, REST cursor honesty."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_bigquery_create_table_false_does_not_create():
    from connectors.bigquery_writer import write_mapped_rows

    client = MagicMock()
    client.get_table.side_effect = Exception("Not found: Table")
    with patch("connectors.bigquery_writer.stub_writes_allowed", return_value=False), patch(
        "google.cloud.bigquery", create=True
    ), patch("connectors.bigquery_conn.get_client", return_value=client):
        result = write_mapped_rows(
            host="proj",
            port=443,
            database="proj",
            username="",
            password="",
            schema="ds",
            connection_string="",
            ssl=True,
            warehouse="",
            table_name="orders",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "INTEGER"},
            create_table=False,
        )
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    client.create_table.assert_not_called()
    client.create_dataset.assert_not_called()


def test_mongodb_create_table_false_does_not_create_collection():
    from connectors.mongodb_writer import write_mapped_rows

    db = MagicMock()
    db.list_collection_names.return_value = []
    client = MagicMock()
    client.__getitem__.return_value = db
    with patch("connectors.mongodb_writer._connection_string", return_value="mongodb://x"), patch(
        "connectors.mongodb_common._mongo_client", return_value=client
    ):
        result = write_mapped_rows(
            host="localhost",
            port=27017,
            database="db",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "INTEGER"},
            create_table=False,
        )
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    # Must not open the missing collection (would auto-create on write).
    assert not any(
        isinstance(c.args, tuple) and c.args and c.args[0] == "orders"
        for c in db.method_calls
        if c[0] == "__getitem__"
    )


def test_elasticsearch_create_table_false_refuses_missing_index():
    from connectors.elasticsearch_writer import write_mapped_rows

    client = MagicMock()
    client.indices.exists.return_value = False
    with patch("connectors.elasticsearch_writer._client", return_value=client):
        result = write_mapped_rows(
            host="localhost",
            port=9200,
            database="orders",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "INTEGER"},
            create_table=False,
        )
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    client.indices.create.assert_not_called()


def test_xml_over_max_rows_fail_closed(monkeypatch):
    import sys
    import types

    from services.file_parser import FileParser

    records = [{"id": i} for i in range(5)]
    fake = types.ModuleType("xmltodict")
    fake.parse = lambda text: {"items": {"item": records}}
    monkeypatch.setitem(sys.modules, "xmltodict", fake)
    monkeypatch.setattr(
        FileParser,
        "_extract_xml_records",
        staticmethod(lambda root: (records, "/items/item", None)),
    )
    result = FileParser.parse_xml("<root/>", max_rows=3)
    assert result.success is False
    assert result.row_count == 5
    assert "exceeding" in (result.error or "").lower()


def test_rest_cursor_repeated_token_fail_closed():
    from connectors.rest_api import read_object

    pages = [
        ([{"id": 1}], "cur_a", True),
        ([{"id": 2}], "cur_a", True),  # repeated cursor
    ]
    calls = {"n": 0}

    def fake_page(cfg, pagination, next_url=None):
        i = calls["n"]
        calls["n"] += 1
        return pages[min(i, len(pages) - 1)]

    with patch("connectors.rest_api._read_page", side_effect=fake_page), patch(
        "connectors.rest_api._resolve_config",
        return_value={
            "pagination_type": "cursor",
            "offset_param": "offset",
            "limit_param": "limit",
            "page_param": "page",
            "cursor_param": "cursor",
            "host": "https://api.example.com",
            "object_path": "items",
            "data_path": "",
            "next_path": "",
        },
    ):
        with pytest.raises(RuntimeError, match="repeated a cursor"):
            read_object(cfg={}, object="items", limit=50)


def test_rest_cursor_follows_absolute_next_url():
    from connectors.rest_api import read_object

    seen: list[tuple] = []

    def fake_page(cfg, pagination, next_url=None):
        seen.append((dict(pagination), next_url))
        if next_url is None and not pagination.get("cursor"):
            return [{"id": 1}], "https://api.example.com/items?page=2", True
        return [{"id": 2}], None, False

    with patch("connectors.rest_api._read_page", side_effect=fake_page), patch(
        "connectors.rest_api._resolve_config",
        return_value={
            "pagination_type": "cursor",
            "offset_param": "offset",
            "limit_param": "limit",
            "page_param": "page",
            "cursor_param": "cursor",
            "host": "https://api.example.com",
            "object_path": "items",
            "data_path": "",
            "next_path": "",
        },
    ):
        batch = read_object(cfg={}, object="items", limit=50)
    assert len(batch.rows) == 2
    # Second page followed the absolute URL, not ?cursor=https://...
    assert seen[1][1] == "https://api.example.com/items?page=2"
    assert "cursor" not in seen[1][0]
