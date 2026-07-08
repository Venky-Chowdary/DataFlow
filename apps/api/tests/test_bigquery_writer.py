"""BigQuery writer tests (stub mode without live GCP project)."""

from connectors.bigquery_writer import write_mapped_rows


def test_bigquery_writer_stub(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ALLOW_STUB_WRITES", "1")
    result = write_mapped_rows(
        host="my-gcp-project",
        port=443,
        database="my-gcp-project",
        username="",
        password="",
        schema="dataflow",
        connection_string="",
        ssl=True,
        warehouse="",
        table_name="df_events_test",
        headers=["event_id", "amount"],
        data_rows=[["e1", "10.5"], ["e2", "20.0"]],
        mappings=[
            {"source": "event_id", "target": "event_id"},
            {"source": "amount", "target": "amount"},
        ],
        column_types={"event_id": "TEXT", "amount": "DECIMAL"},
    )
    assert result.ok
    assert result.rows_written == 2
    assert result.driver in {"stub", "google-cloud-bigquery"}
