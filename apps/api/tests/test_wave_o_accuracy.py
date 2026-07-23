"""Wave O accuracy: deny-create for object stores/Iceberg + SaaS ack completeness."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def test_s3_create_table_false_does_not_create_bucket():
    from botocore.exceptions import ClientError

    from connectors.s3_writer import write_mapped_rows

    client = MagicMock()
    client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
        "HeadBucket",
    )
    with patch("connectors.s3_writer.boto3_client", return_value=client):
        result = write_mapped_rows(
            host="s3.amazonaws.com",
            port=443,
            database="missing-bucket",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="export.json",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "INTEGER"},
            create_table=False,
        )
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    client.create_bucket.assert_not_called()
    client.put_object.assert_not_called()


def test_gcs_create_table_false_does_not_create_bucket():
    from connectors.gcs_writer import write_mapped_rows

    bucket = MagicMock()
    bucket.exists.return_value = False
    client = MagicMock()
    client.bucket.return_value = bucket
    with patch("connectors.gcs_writer.gcs_client", return_value=client):
        result = write_mapped_rows(
            host="",
            port=443,
            database="missing-gcs",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="export.json",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "INTEGER"},
            create_table=False,
        )
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    bucket.create.assert_not_called()


def test_adls_create_table_false_does_not_create_container():
    from connectors.adls_writer import write_mapped_rows

    container = MagicMock()
    container.exists.return_value = False
    client = MagicMock()
    client.get_container_client.return_value = container
    with patch("connectors.adls_writer.blob_service_client", return_value=client):
        result = write_mapped_rows(
            host="acct.blob.core.windows.net",
            port=443,
            database="missing-container",
            username="",
            password="key",
            schema="",
            connection_string="",
            ssl=True,
            table_name="export.json",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "INTEGER"},
            create_table=False,
        )
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    container.create_container.assert_not_called()
    client.get_blob_client.assert_not_called()


def test_iceberg_create_table_false_does_not_mkdir(tmp_path: Path):
    from connectors.iceberg_writer import write_mapped_rows

    warehouse = tmp_path / "wh"
    warehouse.mkdir()
    result = write_mapped_rows(
        host="",
        port=0,
        database=str(warehouse),
        username="",
        password="",
        schema="ns",
        connection_string="",
        ssl=False,
        table_name="events",
        headers=["id"],
        data_rows=[["1"]],
        mappings=[{"source": "id", "target": "id"}],
        column_types={"id": "INTEGER"},
        create_table=False,
    )
    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    assert not (warehouse / "ns" / "events" / "metadata").exists()


def test_salesforce_incomplete_ack_fails_closed():
    from connectors.salesforce_writer import write_mapped_rows

    mock_resp = MagicMock()
    mock_resp.content = b"[]"
    mock_resp.json.return_value = {"done": True}  # not a per-record list
    with patch("connectors.salesforce_writer.request", return_value=mock_resp):
        result = write_mapped_rows(
            host="example.my.salesforce.com",
            api_key="token",
            table_name="Account",
            headers=["External_Id__c", "Name"],
            data_rows=[["ext-1", "Acme"], ["ext-2", "Beta"]],
            mappings=[
                {"source": "External_Id__c", "target": "External_Id__c"},
                {"source": "Name", "target": "Name"},
            ],
            column_types={},
            write_mode="upsert",
            conflict_columns=["External_Id__c"],
            connection_string="",
            username="",
            password="",
            schema="",
            ssl=True,
            port=443,
            database="",
        )
    assert result.ok is False
    assert result.rows_written == 0
    assert "per-record" in (result.error or "").lower() or "result list" in (result.error or "").lower()


def test_salesforce_short_ack_fails_closed():
    from connectors.salesforce_writer import write_mapped_rows

    mock_resp = MagicMock()
    mock_resp.content = b"[]"
    mock_resp.json.return_value = [{"success": True, "id": "001"}]
    with patch("connectors.salesforce_writer.request", return_value=mock_resp):
        result = write_mapped_rows(
            host="example.my.salesforce.com",
            api_key="token",
            table_name="Account",
            headers=["External_Id__c", "Name"],
            data_rows=[["ext-1", "Acme"], ["ext-2", "Beta"]],
            mappings=[
                {"source": "External_Id__c", "target": "External_Id__c"},
                {"source": "Name", "target": "Name"},
            ],
            column_types={},
            write_mode="upsert",
            conflict_columns=["External_Id__c"],
            connection_string="",
            username="",
            password="",
            schema="",
            ssl=True,
            port=443,
            database="",
        )
    assert result.ok is False
    assert "acknowledged 1 of 2" in (result.error or "")


def test_hubspot_incomplete_ack_fails_closed():
    from connectors.hubspot_writer import write_mapped_rows

    mock_resp = MagicMock()
    mock_resp.content = b"{}"
    mock_resp.json.return_value = {"results": [{"id": "1"}], "errors": []}
    with patch("connectors.hubspot_writer.request", return_value=mock_resp):
        result = write_mapped_rows(
            host="",
            api_key="pat-xxx",
            table_name="contacts",
            headers=["email", "firstname"],
            data_rows=[["a@x.com", "A"], ["b@x.com", "B"]],
            mappings=[
                {"source": "email", "target": "email"},
                {"source": "firstname", "target": "firstname"},
            ],
            column_types={},
            write_mode="upsert",
            conflict_columns=["email"],
            connection_string="",
            username="",
            password="",
            schema="",
            ssl=True,
            port=443,
            database="",
        )
    assert result.ok is False
    assert "acknowledged only 1 of 2" in (result.error or "")
