"""Zendesk reverse-ETL writer tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mapped_data():
    return {
        "headers": ["subject", "description", "priority"],
        "data_rows": [["Bug report", "It does not work", "high"]],
        "mappings": [
            {"source": "subject", "target": "subject"},
            {"source": "description", "target": "description"},
            {"source": "priority", "target": "priority"},
        ],
    }


def _mock_response(json_body):
    resp = MagicMock()
    resp.json.return_value = json_body
    resp.raise_for_status.return_value = None
    return resp


def test_zendesk_writer_requires_object():
    from connectors.zendesk_writer import write_mapped_rows

    result = write_mapped_rows(
        host="https://mycompany.zendesk.com",
        api_key="user@example.com:token",
        table_name="",
        **{"data_rows": [], "headers": [], "mappings": []},
    )
    assert not result.ok
    assert "object/table name is required" in result.error


def test_zendesk_writer_requires_credentials():
    from connectors.zendesk_writer import write_mapped_rows

    result = write_mapped_rows(
        host="https://mycompany.zendesk.com",
        table_name="tickets",
        **{"data_rows": [], "headers": [], "mappings": []},
    )
    assert not result.ok
    assert "credentials are required" in result.error


def test_zendesk_writer_requires_host():
    from connectors.zendesk_writer import write_mapped_rows

    result = write_mapped_rows(
        host="",
        username="user@example.com",
        password="token",
        table_name="tickets",
        **{"data_rows": [], "headers": [], "mappings": []},
    )
    assert not result.ok
    assert "subdomain host is required" in result.error


def test_zendesk_writer_creates_ticket(mapped_data):
    from connectors import zendesk_writer

    resp = _mock_response({"ticket": {"id": 12345, "subject": "Bug report"}})
    with patch(
        "connectors.zendesk.describe_fields",
        side_effect=RuntimeError("describe mocked down"),
    ), patch.object(zendesk_writer, "request", return_value=resp) as mock_req:
        result = zendesk_writer.write_mapped_rows(
            host="https://mycompany.zendesk.com",
            username="user@example.com",
            password="token",
            table_name="tickets",
            write_mode="insert",
            **mapped_data,
        )

    assert result.ok
    assert result.rows_written == 1
    assert result.driver == "zendesk"
    call = mock_req.call_args
    assert call.kwargs["method"] == "POST"
    assert "mycompany.zendesk.com/api/v2/tickets.json" in call.kwargs["url"]


def test_zendesk_writer_updates_ticket_with_id():
    from connectors import zendesk_writer

    data = {
        "headers": ["id", "subject"],
        "data_rows": [["98765", "Updated subject"]],
        "mappings": [
            {"source": "id", "target": "id"},
            {"source": "subject", "target": "subject"},
        ],
    }
    resp = _mock_response({"ticket": {"id": 98765, "subject": "Updated subject"}})
    with patch(
        "connectors.zendesk.describe_fields",
        side_effect=RuntimeError("describe mocked down"),
    ), patch.object(zendesk_writer, "request", return_value=resp) as mock_req:
        result = zendesk_writer.write_mapped_rows(
            host="https://mycompany.zendesk.com",
            api_key="user@example.com:token",
            table_name="tickets",
            write_mode="update",
            conflict_columns=["id"],
            **data,
        )

    assert result.ok
    assert result.rows_written == 1
    call = mock_req.call_args
    assert call.kwargs["method"] == "PUT"
    assert "tickets/98765.json" in call.kwargs["url"]


def test_zendesk_writer_quarantines_non_auth_error():
    from connectors import zendesk_writer

    import requests

    resp = MagicMock()
    resp.raise_for_status.side_effect = requests.exceptions.HTTPError("400 validation")
    with patch(
        "connectors.zendesk.describe_fields",
        side_effect=RuntimeError("describe mocked down"),
    ), patch.object(zendesk_writer, "request", return_value=resp):
        result = zendesk_writer.write_mapped_rows(
            host="https://mycompany.zendesk.com",
            username="user@example.com",
            password="token",
            table_name="tickets",
            write_mode="insert",
            headers=["subject"],
            data_rows=[["Bad"]],
            mappings=[{"source": "subject", "target": "subject"}],
        )

    assert result.ok
    assert result.rows_written == 0
    assert len(result.rejected_details) == 1
