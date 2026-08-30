"""BigQuery writer tests (stub mode without live GCP project)."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from connectors.bigquery_writer import (
    bq_schema_field,
    bq_type,
    resolve_bigquery_decimal_target_types,
    write_mapped_rows,
)
from connectors.writer_common import quarantine_unfit_decimals


def test_bq_type_strips_string_bytes_width():
    assert bq_type("STRING(64)") == "STRING"
    assert bq_type("BINARY(16)") == "BYTES"
    assert bq_type("DECIMAL(20,6)") == "BIGNUMERIC"
    assert bq_type("NUMERIC(10,2)") == "NUMERIC"
    assert bq_type("BIGNUMERIC(20,6)") == "BIGNUMERIC"


def test_bq_type_over_bignumeric_cap_fails_closed_to_string():
    assert bq_type("BIGNUMERIC(80,40)") == "STRING"
    assert bq_type("DECIMAL(100,0)") == "STRING"


def test_bq_schema_field_sets_max_length():
    bq = MagicMock()
    bq.SchemaField = MagicMock(side_effect=lambda *a, **k: SimpleNamespace(args=a, kwargs=k))
    field = bq_schema_field(bq, "blob", "BINARY(16)")
    assert field.args == ("blob", "BYTES")
    assert field.kwargs.get("max_length") == 16
    field2 = bq_schema_field(bq, "name", "VARCHAR(32)")
    assert field2.args == ("name", "STRING")
    assert field2.kwargs.get("max_length") == 32


def test_bq_schema_field_honors_map_decimal_stamp():
    """Map≡CREATE: SchemaField must carry approved (p,s), never bare invent."""
    bq = MagicMock()
    bq.SchemaField = MagicMock(side_effect=lambda *a, **k: SimpleNamespace(args=a, kwargs=k))

    bn = bq_schema_field(bq, "amount", "DECIMAL(20,6)")
    assert bn.args == ("amount", "BIGNUMERIC")
    assert bn.kwargs == {"precision": 20, "scale": 6}

    num = bq_schema_field(bq, "price", "NUMERIC(10,2)")
    assert num.args == ("price", "NUMERIC")
    assert num.kwargs == {"precision": 10, "scale": 2}

    bare = bq_schema_field(bq, "loose", "DECIMAL")
    assert bare.args == ("loose", "BIGNUMERIC")
    assert "precision" not in bare.kwargs
    assert "scale" not in bare.kwargs

    emu = bq_schema_field(bq, "amount", "DECIMAL(20,6)", emulator=True)
    assert emu.args == ("amount", "BIGNUMERIC")
    assert emu.kwargs == {}


def test_bq_schema_field_numeric_over_cap_promotes_keeping_stamp():
    """NUMERIC(40,4) exceeds NUMERIC caps — BIGNUMERIC with same (p,s)."""
    bq = MagicMock()
    bq.SchemaField = MagicMock(side_effect=lambda *a, **k: SimpleNamespace(args=a, kwargs=k))
    field = bq_schema_field(bq, "wide", "NUMERIC(40,4)")
    assert field.args == ("wide", "BIGNUMERIC")
    assert field.kwargs == {"precision": 40, "scale": 4}


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
        SimpleNamespace(name="label", field_type="STRING", precision=None, scale=None, max_length=None),
        SimpleNamespace(name="code", field_type="STRING", precision=None, scale=None, max_length=32),
        SimpleNamespace(name="blob", field_type="BYTES", precision=None, scale=None, max_length=16),
    ]
    types = resolve_bigquery_decimal_target_types(
        ["amount", "label", "code", "blob"],
        ["DECIMAL(20,6)", "string", "STRING(64)", "BINARY(32)"],
        schema,
    )
    assert types[0] == "NUMERIC(10,2)"
    assert types[1] == "STRING"
    assert types[2] == "STRING(32)"
    assert types[3] == "BYTES(16)"


def test_resolve_bigquery_decimal_uses_ddl_without_table():
    types = resolve_bigquery_decimal_target_types(
        ["amount", "price"],
        ["DECIMAL(20,6)", "NUMERIC(10,2)"],
        None,
    )
    assert types == ["BIGNUMERIC(20,6)", "NUMERIC(10,2)"]


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
