"""Shared ETL / schema / type fidelity — zero silent loss across connectors."""

from __future__ import annotations

from connectors.writer_common import build_mapped_rows_with_details
from services.ddl_compatibility import evaluate_ddl_compatibility
from services.schema_introspect import _mysql_to_logical, _pg_to_logical
from services.transform_engine import apply_transform, infer_transform_for_mapping
from services.type_system import (
    ddl_type,
    decimal_precision_would_truncate,
    decimal_scale_would_truncate,
)


def test_decimal_precision_clamp_is_gated():
    assert decimal_precision_would_truncate("DECIMAL(40,10)", "mysql") is True
    assert decimal_scale_would_truncate("DECIMAL(40,10)", "mysql") is False
    # ddl_type must not silently clamp — prefer lossless text.
    out = ddl_type("mysql", "DECIMAL(40,10)")
    assert "DECIMAL(38" not in out.upper() or "TEXT" in out.upper() or "VARCHAR" in out.upper()
    assert ddl_type("duckdb", "DECIMAL") == "DECIMAL(38,15)"


def test_ddl_compatibility_flags_precision_clamp():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "amt", "target": "amt", "source_type": "DECIMAL(40,10)", "target_type": "DECIMAL(38,10)"}],
        source_schema={"amt": "DECIMAL(40,10)"},
        target_schema={"amt": "DECIMAL(38,10)"},
        dest_db_type="mysql",
        table_exists=True,
    )
    assert ok is False
    assert any("precision clamps" in i for i in issues)


def test_pg_array_preserves_element_carrier():
    assert _pg_to_logical("integer[]") == "ARRAY<INTEGER>"
    assert _pg_to_logical("text[]") == "ARRAY<VARCHAR>"
    assert _pg_to_logical("numeric(12,4)[]") == "ARRAY<DECIMAL(12,4)>"
    assert _pg_to_logical("jsonb") == "JSON"


def test_mysql_unsigned_widths_preserved():
    assert _mysql_to_logical("int unsigned") == "INT UNSIGNED"
    assert _mysql_to_logical("int(11) unsigned") == "INT UNSIGNED"
    assert _mysql_to_logical("mediumint unsigned") == "MEDIUMINT UNSIGNED"
    assert _mysql_to_logical("smallint unsigned") == "SMALLINT UNSIGNED"
    assert _mysql_to_logical("bigint unsigned") == "BIGINT UNSIGNED"


def test_json_nan_rejected_not_nulled():
    val, err = apply_transform("NaN", "json")
    assert val is None
    assert err and "non-finite" in err.lower()
    val2, err2 = apply_transform('{"x": Infinity}', "json")
    assert val2 is None
    assert err2


def test_vector_transform_parses_and_rejects_bad():
    assert infer_transform_for_mapping("emb", "emb", "VECTOR(3)", "VECTOR(3)") == "vector"
    ok, err = apply_transform("[0.1, 0.2, 0.3]", "vector")
    assert err is None
    assert ok == [0.1, 0.2, 0.3]
    bad, err_bad = apply_transform("[0.1, NaN]", "vector")
    assert bad is None and err_bad


def test_quarantine_holds_out_json_nan_row():
    mapped, _errors, details = build_mapped_rows_with_details(
        headers=["id", "payload"],
        data_rows=[["1", '{"a":1}'], ["2", "NaN"], ["3", "[1,2]"]],
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {"source": "payload", "target": "payload", "transform": "json"},
        ],
        target_cols=["id", "payload"],
        error_policy="quarantine",
    )
    assert len(mapped) == 2
    assert details and details[0]["policy"] == "quarantine"
    ids = {r[0] for r in mapped}
    assert ids == {"1", "3"}


def test_tz_polarity_lakehouse_and_mysql():
    assert "UTC" in ddl_type("clickhouse", "TIMESTAMPTZ") or "DateTime" in ddl_type("clickhouse", "TIMESTAMPTZ")
    assert "NTZ" in ddl_type("databricks", "TIMESTAMP_NTZ").upper() or "TIMESTAMP_NTZ" in ddl_type(
        "databricks", "TIMESTAMP_NTZ"
    ).upper()
    assert ddl_type("mysql", "TIMESTAMPTZ").upper().startswith("DATETIME")
