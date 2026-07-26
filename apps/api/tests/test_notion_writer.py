"""Notion reverse-ETL writer tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mapped_data():
    return {
        "headers": ["name", "status", "price"],
        "data_rows": [["Widget", "Active", "9.99"]],
        "mappings": [
            {"source": "name", "target": "Name"},
            {"source": "status", "target": "Status"},
            {"source": "price", "target": "Price"},
        ],
    }


def _mock_response(json_body):
    resp = MagicMock()
    resp.json.return_value = json_body
    resp.raise_for_status.return_value = None
    return resp


def test_notion_writer_requires_database_id():
    from connectors.notion_writer import write_mapped_rows

    result = write_mapped_rows(
        api_key="secret_xxx",
        **{"data_rows": [], "headers": [], "mappings": []},
    )
    assert not result.ok
    assert "database id is required" in result.error


def test_notion_writer_requires_token():
    from connectors.notion_writer import write_mapped_rows

    result = write_mapped_rows(
        table_name="db123",
        **{"data_rows": [], "headers": [], "mappings": []},
    )
    assert not result.ok
    assert "integration token is required" in result.error


def test_notion_writer_creates_page(mapped_data):
    from connectors import notion_writer

    schema_resp = _mock_response({
        "properties": {
            "Name": {"type": "title"},
            "Status": {"type": "select"},
            "Price": {"type": "number"},
        }
    })
    page_resp = _mock_response({"id": "page-123", "url": "https://notion.so/page-123"})

    def _fake_request(*, method, url, **kwargs):
        if "databases/" in url:
            return schema_resp
        return page_resp

    with patch.object(notion_writer, "request", side_effect=_fake_request) as mock_req:
        result = notion_writer.write_mapped_rows(
            table_name="db123",
            api_key="secret_xxx",
            **mapped_data,
        )

    assert result.ok
    assert result.rows_written == 1
    assert result.driver == "notion"
    calls = [c for c in mock_req.call_args_list]
    post_call = next(c for c in calls if c.kwargs.get("method") == "POST")
    assert "v1/pages" in post_call.kwargs["url"]
    payload = post_call.kwargs["data"]
    assert payload["parent"]["database_id"] == "db123"
    assert payload["properties"]["Price"]["number"] == 9.99


def test_notion_writer_updates_page(mapped_data):
    from connectors import notion_writer

    schema_resp = _mock_response({
        "properties": {
            "Name": {"type": "title"},
            "Status": {"type": "select"},
            "Price": {"type": "number"},
        }
    })
    page_resp = _mock_response({"id": "page-123"})

    def _fake_request(*, method, url, **kwargs):
        if "databases/" in url:
            return schema_resp
        return page_resp

    data = {
        "headers": ["id", "name", "status", "price"],
        "data_rows": [["page-123", "Widget", "Active", "9.99"]],
        "mappings": [
            {"source": "id", "target": "id"},
            {"source": "name", "target": "Name"},
            {"source": "status", "target": "Status"},
            {"source": "price", "target": "Price"},
        ],
    }

    with patch.object(notion_writer, "request", side_effect=_fake_request) as mock_req:
        result = notion_writer.write_mapped_rows(
            table_name="db123",
            api_key="secret_xxx",
            write_mode="update",
            **data,
        )

    assert result.ok
    assert result.rows_written == 1
    patch_call = next(c for c in mock_req.call_args_list if c.kwargs.get("method") == "PATCH")
    assert "v1/pages/page-123" in patch_call.kwargs["url"]


def test_notion_writer_skips_read_only_property():
    from connectors import notion_writer

    schema_resp = _mock_response({
        "properties": {
            "Name": {"type": "title"},
            "Computed": {"type": "formula"},
        }
    })
    page_resp = _mock_response({"id": "page-123"})

    def _fake_request(*, method, url, **kwargs):
        if "databases/" in url:
            return schema_resp
        return page_resp

    data = {
        "headers": ["name", "computed"],
        "data_rows": [["Widget", "42"]],
        "mappings": [
            {"source": "name", "target": "Name"},
            {"source": "computed", "target": "Computed"},
        ],
    }

    with patch.object(notion_writer, "request", side_effect=_fake_request):
        result = notion_writer.write_mapped_rows(
            table_name="db123",
            api_key="secret_xxx",
            **data,
        )

    assert result.ok
    assert "read-only" in " ".join(result.warnings).lower()
