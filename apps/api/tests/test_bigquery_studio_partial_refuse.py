"""BigQuery Studio merge — refuse partial Studio Map VARCHAR invent on create-new."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_bigquery_create_new_refuses_partial_studio():
    from connectors.bigquery_writer import write_mapped_rows

    client = MagicMock()
    client.get_table.side_effect = Exception("404 Not found")
    client.list_datasets.return_value = []

    with patch("connectors.bigquery_writer.stub_writes_allowed", return_value=False), patch(
        "connectors.bigquery_conn.get_client", return_value=client
    ), patch(
        "connectors.bigquery_conn._is_local_endpoint", return_value=(False, "")
    ):
        result = write_mapped_rows(
            host="proj",
            port=443,
            database="proj",
            username="",
            password="",
            schema="ds",
            connection_string="/tmp/sa.json",
            ssl=True,
            table_name="orders",
            headers=["id", "amount"],
            data_rows=[["1", "9.99"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "STRING"},
                {"source": "amount", "target": "amount", "target_type": "STRING"},
            ],
            column_types={"id": "STRING", "amount": "STRING"},
            destination_column_types={"id": "STRING"},  # amount missing
            create_table=True,
            service_account="/tmp/sa.json",
            warehouse="",
        )
    assert result.ok is False
    assert "amount" in (result.error or "").lower()
    client.create_table.assert_not_called()
