"""Wave W accuracy: strict write policy, Weaviate batch ack, SF incomplete cursor."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import responses


def test_salesforce_writer_strict_policy_blocks_partial_activation():
    from connectors.salesforce_writer import write_mapped_rows

    mock_resp = MagicMock()
    mock_resp.content = b"[{}]"
    mock_resp.json.return_value = [
        {"success": True, "id": "001"},
        {"success": False, "errors": [{"message": "DUPLICATE_VALUE"}]},
    ]

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
            error_policy="fail",
        )

    assert result.ok is False
    assert "strict error policy" in (result.error or "").lower()
    assert result.rejected_details


def test_hubspot_writer_strict_policy_blocks_api_errors():
    from connectors.hubspot_writer import write_mapped_rows

    mock_resp = MagicMock()
    mock_resp.content = b"{}"
    mock_resp.json.return_value = {
        "results": [{"id": "1"}],
        "errors": [{"message": "PROPERTY_DOESNT_EXIST", "context": {}}],
    }

    with patch("connectors.hubspot_writer.request", return_value=mock_resp):
        result = write_mapped_rows(
            host="api.hubapi.com",
            api_key="token",
            table_name="contacts",
            headers=["email", "firstname"],
            data_rows=[["a@b.com", "A"], ["c@d.com", "C"]],
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
            error_policy="fail",
        )

    assert result.ok is False
    assert "strict error policy" in (result.error or "").lower()


def test_elasticsearch_transform_fail_skips_bulk():
    from connectors.elasticsearch_writer import write_mapped_rows

    client = MagicMock()
    client.indices.exists.return_value = True
    bulk = MagicMock()

    with patch("connectors.elasticsearch_writer._client", return_value=client):
        with patch("elasticsearch.helpers.bulk", bulk):
            result = write_mapped_rows(
                host="localhost",
                port=9200,
                database="",
                username="",
                password="",
                schema="",
                connection_string="",
                ssl=False,
                table_name="events",
                headers=["id", "amount"],
                data_rows=[["1", "not-a-number"]],
                mappings=[
                    {"source": "id", "target": "id"},
                    {"source": "amount", "target": "amount"},
                ],
                column_types={"id": "INTEGER", "amount": "DECIMAL"},
                create_table=False,
                error_policy="fail",
            )

    assert result.ok is False
    assert "Transform errors" in (result.error or "")
    bulk.assert_not_called()


def test_pinecone_strict_policy_blocks_partial_vectors():
    from connectors.pinecone_writer import write_mapped_rows

    with patch("connectors.pinecone_writer.vectorize_records") as vz:
        vz.return_value = [
            {"id": "a", "content": "ok", "embedding": [0.1, 0.2, 0.3]},
            {"id": "b", "content": "bad", "embedding": None},
        ]
        with patch("connectors.pinecone_writer._requests_session") as sess_factory:
            result = write_mapped_rows(
                host="https://example.pinecone.io",
                port=443,
                database="",
                username="",
                password="key",
                schema="",
                connection_string="",
                ssl=True,
                table_name="ns",
                headers=["id", "content"],
                data_rows=[["a", "ok"], ["b", "bad"]],
                mappings=[],
                column_types={},
                error_policy="fail",
                skip_chunking=True,
            )

    assert result.ok is False
    assert "strict error policy" in (result.error or "").lower()
    sess_factory.assert_not_called()


def test_weaviate_batch_object_errors_fail_strict():
    from connectors.weaviate_writer import write_mapped_rows

    session = MagicMock()
    schema_ok = MagicMock(status_code=200)
    batch_resp = MagicMock(status_code=200, content=b"[]")
    batch_resp.json.return_value = [
        {"id": "11111111-1111-1111-1111-111111111111", "result": {"status": "SUCCESS"}},
        {
            "id": "22222222-2222-2222-2222-222222222222",
            "result": {"errors": {"error": [{"message": "invalid vector"}]}},
        },
    ]
    session.get.return_value = schema_ok
    session.post.return_value = batch_resp

    with patch("connectors.weaviate_writer.vectorize_records") as vz:
        vz.return_value = [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "content": "a",
                "embedding": [0.1, 0.2],
            },
            {
                "id": "22222222-2222-2222-2222-222222222222",
                "content": "b",
                "embedding": [0.3, 0.4],
            },
        ]
        with patch("connectors.weaviate_writer._requests_session", return_value=session):
            result = write_mapped_rows(
                host="localhost",
                port=8080,
                database="",
                username="",
                password="",
                schema="",
                connection_string="",
                ssl=False,
                table_name="DataflowChunk",
                headers=["id", "content"],
                data_rows=[["1", "a"], ["2", "b"]],
                mappings=[],
                column_types={},
                error_policy="fail",
                skip_chunking=True,
                create_table=False,
            )

    assert result.ok is False
    assert "rejected" in (result.error or "").lower()
    assert result.rejected_details


@responses.activate
def test_salesforce_query_more_incomplete_cursor_fail_closed():
    import connectors.salesforce as salesforce

    responses.add(
        responses.GET,
        "https://login.salesforce.com/services/data/v58.0/sobjects/Account/describe",
        json={
            "fields": [
                {"name": "Id", "type": "id"},
                {"name": "Name", "type": "string"},
            ]
        },
        status=200,
    )
    responses.add(
        responses.GET,
        "https://login.salesforce.com/services/data/v58.0/query",
        json={
            "totalSize": 2,
            "done": False,
            "nextRecordsUrl": None,
            "records": [{"Id": "001", "Name": "Acme"}],
        },
        status=200,
    )
    with pytest.raises(RuntimeError, match="incomplete page"):
        salesforce.read_object(cfg={"api_key": "fake-token"}, limit=500)


def test_reject_on_strict_policy_helper():
    from connectors.writer_common import reject_on_strict_policy

    assert reject_on_strict_policy("quarantine", [{"row": 1}], "X") is None
    assert reject_on_strict_policy("fail", [], "X") is None
    msg = reject_on_strict_policy("fail", [{"row": 1}], "Pinecone")
    assert msg is not None
    assert "strict error policy" in msg
