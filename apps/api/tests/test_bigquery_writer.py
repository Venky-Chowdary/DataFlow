"""BigQuery writer tests (stub mode without live GCP project)."""

from types import SimpleNamespace

from connectors.bigquery_writer import (
    resolve_bigquery_decimal_target_types,
    write_mapped_rows,
)
from connectors.writer_common import quarantine_unfit_decimals


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


def test_resolve_bigquery_decimal_prefers_physical_schema():
    schema = [
        SimpleNamespace(name="amount", field_type="NUMERIC", precision=10, scale=2),
        SimpleNamespace(name="label", field_type="STRING", precision=None, scale=None),
    ]
    types = resolve_bigquery_decimal_target_types(
        ["amount", "label"],
        ["DECIMAL(20,6)", "string"],
        schema,
    )
    assert types[0] == "NUMERIC(10,2)"
    assert types[1] == "STRING"


def test_resolve_bigquery_decimal_uses_ddl_without_table():
    types = resolve_bigquery_decimal_target_types(
        ["amount"],
        ["DECIMAL(20,6)"],
        None,
    )
    assert types == ["BIGNUMERIC(20,6)"]


def test_bigquery_numeric_quarantine_holds_out_overflow():
    rows = [("99999999999999999999",), ("1.50",)]
    details: list[dict] = []
    out = quarantine_unfit_decimals(
        rows,
        ["amount"],
        ["NUMERIC(10,2)"],
        details,
        policy="quarantine",
        dialect_label="BigQuery NUMERIC",
    )
    assert out == [("1.50",)]
    assert details and "BigQuery NUMERIC(10,2)" in details[0]["reason"]
