"""BigQuery: existing table + create_table=True must still use live DDL before map."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch


def test_bq_existing_table_create_ok_uses_physical_before_map(monkeypatch):
    """create_table=True + exists_ok must not Map-VARCHAR coerce into BOOL."""
    monkeypatch.delenv("DATAFLOW_ALLOW_STUB_WRITES", raising=False)
    monkeypatch.setenv("DATAFLOW_ALLOW_STUB_WRITES", "0")

    from connectors import bigquery_writer as bw

    physical_fields = [
        SimpleNamespace(
            name="flag",
            field_type="BOOL",
            precision=None,
            scale=None,
            max_length=None,
        )
    ]
    table = SimpleNamespace(schema=physical_fields)

    client = MagicMock()
    client.get_table.return_value = table
    client.list_datasets.return_value = []
    # insert path — return empty so we stop after map/quarantine honesty check
    job = MagicMock()
    job.result.return_value = None
    job.output_rows = 0
    client.load_table_from_json.return_value = job
    client.insert_rows_json.return_value = []

    fake_bq = MagicMock()
    fake_bq.Dataset = MagicMock
    fake_bq.Table = MagicMock
    fake_bq.LoadJobConfig = MagicMock
    fake_bq.WriteDisposition = SimpleNamespace(
        WRITE_APPEND="WRITE_APPEND",
        WRITE_TRUNCATE="WRITE_TRUNCATE",
    )
    fake_bq.SchemaField = MagicMock(
        side_effect=lambda *a, **k: SimpleNamespace(name=a[0], field_type=a[1])
    )

    with patch.dict("sys.modules", {"google.cloud.bigquery": fake_bq}), patch(
        "connectors.bigquery_conn.get_client", return_value=client
    ), patch(
        "connectors.bigquery_conn._is_local_endpoint", return_value=(True, "http://localhost")
    ):
        result = bw.write_mapped_rows(
            host="local-project",
            port=443,
            database="local-project",
            username="",
            password="",
            schema="ds",
            connection_string="http://localhost:9050",
            ssl=False,
            warehouse="",
            table_name="flags",
            headers=["flag"],
            data_rows=[["true"], [""], ["yes"]],
            mappings=[
                {
                    "source": "flag",
                    "target": "flag",
                    "target_type": "VARCHAR",
                    "source_type": "VARCHAR",
                }
            ],
            column_types={"flag": "VARCHAR"},
            create_table=True,
            error_policy="quarantine",
            write_mode="insert",
        )

    # Empty / informal yes must quarantine under physical BOOL — not silent STRING.
    assert result.rejected_details
    cols = {(d.get("column") or "").lower() for d in result.rejected_details}
    assert "flag" in cols
    # Physical probe happened (existing table path).
    assert client.get_table.called


def test_bq_probe_permission_error_fail_closed(monkeypatch):
    """Non-404 probe errors must not invent 'table missing' Map VARCHAR path."""
    monkeypatch.delenv("DATAFLOW_ALLOW_STUB_WRITES", raising=False)
    monkeypatch.setenv("DATAFLOW_ALLOW_STUB_WRITES", "0")

    from connectors import bigquery_writer as bw

    client = MagicMock()
    client.get_table.side_effect = PermissionError("403 Permission denied on table")

    fake_bq = MagicMock()
    fake_bq.Dataset = MagicMock
    fake_bq.Table = MagicMock
    fake_bq.SchemaField = MagicMock(
        side_effect=lambda *a, **k: SimpleNamespace(name=a[0], field_type=a[1])
    )

    with patch.dict("sys.modules", {"google.cloud.bigquery": fake_bq}), patch(
        "connectors.bigquery_conn.get_client", return_value=client
    ), patch(
        "connectors.bigquery_conn._is_local_endpoint", return_value=(True, "http://localhost")
    ):
        result = bw.write_mapped_rows(
            host="local-project",
            port=443,
            database="local-project",
            username="",
            password="",
            schema="ds",
            connection_string="http://localhost:9050",
            ssl=False,
            warehouse="",
            table_name="flags",
            headers=["flag"],
            data_rows=[["true"]],
            mappings=[{"source": "flag", "target": "flag", "target_type": "VARCHAR"}],
            column_types={"flag": "VARCHAR"},
            create_table=True,
            error_policy="quarantine",
        )

    assert result.ok is False
    err = (result.error or "").lower()
    assert "probe failed" in err or "permission" in err
    assert "physical" in err or "invent" in err
