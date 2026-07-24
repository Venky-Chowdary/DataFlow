"""Proofs for NULL vs empty, BigQuery INTERVAL, Kafka Debezium source path."""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.header_union import SCHEMALESS_SOURCE_TYPES
from connectors.kafka_debezium_bridge import debezium_to_row, parse_debezium_envelope
from services.transform_engine import apply_transform
from services.type_system import ddl_type, normalize_logical_type
from services.value_serializer import (
    SQL_NULL_SENTINEL,
    cell_to_string,
    format_bigquery_interval,
)


def test_sql_null_sentinel_distinct_from_empty_string():
    assert cell_to_string(None) == ""
    assert cell_to_string(None, preserve_sql_null=True) == SQL_NULL_SENTINEL
    assert cell_to_string("") == ""

    null_val, null_err = apply_transform(SQL_NULL_SENTINEL, "none")
    assert null_err is None and null_val is None

    empty_val, empty_err = apply_transform("", "none")
    assert empty_err is None and empty_val == ""

    # Typed transforms still coerce empty → None.
    typed_val, typed_err = apply_transform("", "integer")
    assert typed_err is None and typed_val is None


def test_safe_ddl_preserves_float_not_varchar():
    """IEEE FLOAT must not collapse to VARCHAR when samples are numeric strings."""
    from services.schema_inference import safe_ddl_logical_type

    assert safe_ddl_logical_type("FLOAT", ["1500.0", "0.025"], field_name="amt_float") == "FLOAT"
    assert safe_ddl_logical_type("float", ["1.5e3"], field_name="amt_float") == "FLOAT"


def test_g8_write_path_preserves_sql_null_vs_empty_fingerprint():
    """G8 must not collapse __DF_SQL_NULL__ into '' (false identity mismatch)."""
    from preflight.gates import _apply_write_path_transform
    from services.reconciliation import normalize_cell

    null_out, null_err = _apply_write_path_transform(SQL_NULL_SENTINEL, "none")
    empty_out, empty_err = _apply_write_path_transform("", "none")
    assert null_err is None and empty_err is None
    assert null_out == SQL_NULL_SENTINEL
    assert empty_out == ""
    assert normalize_cell(SQL_NULL_SENTINEL) == normalize_cell(null_out)
    assert normalize_cell(SQL_NULL_SENTINEL) != normalize_cell("")
    assert normalize_cell(None) == normalize_cell(SQL_NULL_SENTINEL)


def test_empty_string_survives_identity_round_trip_in_mapped_rows():
    from connectors.writer_common import build_mapped_rows

    headers = ["id", "note"]
    rows = [
        ["1", SQL_NULL_SENTINEL],  # SQL NULL
        ["2", ""],                 # empty string
        ["3", "hi"],
    ]
    mappings = [
        {"source": "id", "target": "id", "transform": "none"},
        {"source": "note", "target": "note", "transform": "none"},
    ]
    mapped, errors = build_mapped_rows(
        headers=headers,
        data_rows=rows,
        mappings=mappings,
        target_cols=["id", "note"],
        column_types={"id": "VARCHAR", "note": "VARCHAR"},
    )
    assert not errors
    assert mapped[0][1] is None
    assert mapped[1][1] == ""
    assert mapped[2][1] == "hi"


def test_bigquery_interval_native_ddl_and_wire():
    assert normalize_logical_type("INTERVAL") == "interval"
    assert ddl_type("bigquery", "INTERVAL") == "INTERVAL"
    assert "STRING" != ddl_type("bigquery", "INTERVAL")

    assert format_bigquery_interval(timedelta(seconds=1)) == "0-0 0 0:00:01"
    assert format_bigquery_interval(timedelta(days=1, hours=2, minutes=3, seconds=4)) == "0-0 1 2:03:04"
    assert format_bigquery_interval("P1DT15M") == "0-0 1 0:15:00"
    assert format_bigquery_interval("01:02:03") == "0-0 0 1:02:03"

    from connectors.warehouse_temporal import format_bigquery_bind

    assert format_bigquery_bind("P1D", "INTERVAL") == "0-0 1 0:00:00"


def test_kafka_debezium_envelope_to_row():
    envelope = {
        "payload": {
            "op": "c",
            "after": {"id": 1, "name": "alice"},
            "source": {"table": "users", "ts_ms": 100, "lsn": 42},
        }
    }
    change = parse_debezium_envelope(envelope)
    assert change is not None
    row = debezium_to_row(change)
    assert row["id"] == 1
    assert row["name"] == "alice"
    assert row["__op"] == "c"


def test_kafka_is_transfer_ready_source():
    from src.transfer.connector_capabilities import get_capabilities
    from src.transfer.connector_registry import CONNECTOR_MODULES

    assert "kafka" in SCHEMALESS_SOURCE_TYPES
    caps = get_capabilities("kafka")
    assert caps.get("read") is True
    assert caps.get("write") is True
    assert CONNECTOR_MODULES["kafka"].reader == "connectors.kafka_reader"
    assert CONNECTOR_MODULES["kafka"].reader_fn == "read_topic_batch"
