"""Schema engine proof matrix — enums, flags, DDL safety, existing-dest honesty."""

from __future__ import annotations

import pytest

from connectors.sqlite_common import sqlite_file_path
from connectors.writer_common import resolve_target_columns, sample_values_by_source_from_batch
from services.coercion_probe import analyze_coercion
from services.schema_inference import (
    _is_boolean_field_name,
    infer_column,
    infer_schema_map,
    infer_type,
    samples_fit_logical_type,
    safe_ddl_logical_type,
)
from services.transform_engine import _parse_boolean


# Connector-agnostic cases (same engine for Mongo/PG/MySQL/files → SF/BQ/PG)
ENUM_CASES = [
    (["active", "invalidated"], "status", "VARCHAR", "string_enum"),
    (["pending", "approved", "rejected"], "state", "VARCHAR", "string_enum"),
    (["draft", "published"], "lifecycle", "VARCHAR", "string_enum"),
    (["enabled", "disabled"], "account_state", "VARCHAR", "string_enum"),
]

FLAG_CASES = [
    (["true", "false"], "deviceVerified", "BOOLEAN", "boolean_flag"),
    (["0", "1"], "is_active", "BOOLEAN", "boolean_flag"),
    (["yes", "no"], "email_verified", "BOOLEAN", "boolean_flag"),
]

TEMPORAL_CASES = [
    (["20240115"], "txn_yyyymmdd", "DATE", "temporal"),
    (["20240115"], "sku", "INTEGER", "numeric"),
    (["1705312200000"], "created_at", "TIMESTAMP", "temporal"),
]


@pytest.mark.parametrize(
    "token",
    [
        "active", "inactive", "enabled", "disabled", "pending", "invalidated",
        "approved", "rejected", "completed", "ok", "positive", "negative",
    ],
)
def test_status_words_are_not_boolean_literals(token: str) -> None:
    assert _parse_boolean(token) is None


@pytest.mark.parametrize("token,expected", [("true", True), ("false", False), ("yes", True), ("no", False)])
def test_classic_booleans(token: str, expected: bool) -> None:
    assert _parse_boolean(token) is expected


@pytest.mark.parametrize(
    "name",
    ["is_active", "has_subscription", "deviceVerified", "email_verified", "enabled", "user_flag"],
)
def test_flag_names(name: str) -> None:
    assert _is_boolean_field_name(name) is True


@pytest.mark.parametrize(
    "name",
    ["status", "state", "active", "completed", "approved", "session_status", "lifecycle"],
)
def test_enum_names_are_not_flags(name: str) -> None:
    assert _is_boolean_field_name(name) is False


@pytest.mark.parametrize("samples,field,logical,role", ENUM_CASES)
def test_enum_matrix(samples, field, logical, role):
    intel = infer_column(samples, field_name=field)
    assert intel["logical_type"] == logical
    assert intel["semantic_role"] == role


@pytest.mark.parametrize("samples,field,logical,role", FLAG_CASES)
def test_flag_matrix(samples, field, logical, role):
    intel = infer_column(samples, field_name=field)
    assert intel["logical_type"] == logical
    assert intel["semantic_role"] == role


@pytest.mark.parametrize("samples,field,logical,role", TEMPORAL_CASES)
def test_temporal_matrix(samples, field, logical, role):
    intel = infer_column(samples, field_name=field)
    assert intel["logical_type"] == logical
    assert intel["semantic_role"] == role


def test_bare_active_zero_one_stays_integer():
    assert infer_type(["0", "1"], field_name="active") == "INTEGER"


def test_phone_not_timestamp():
    assert infer_type(["5558675309"], field_name="phone") in {"INTEGER", "VARCHAR"}


def test_infer_schema_map_choke_point():
    schema, intel = infer_schema_map(
        {
            "status": ["active", "invalidated"],
            "deviceVerified": ["true", "false"],
            "amount": ["12.50", "3"],
        }
    )
    assert schema["status"] == "VARCHAR"
    assert intel["status"]["semantic_role"] == "string_enum"
    assert schema["deviceVerified"] == "BOOLEAN"
    assert schema["amount"] == "DECIMAL"


def test_status_samples_do_not_fit_boolean():
    assert samples_fit_logical_type(["active", "invalidated"], "BOOLEAN", field_name="status") is False
    assert samples_fit_logical_type(["true", "false"], "BOOLEAN", field_name="flag") is True


def test_safe_ddl_widens_enum_boolean_for_new_table():
    assert (
        safe_ddl_logical_type(
            "BOOLEAN",
            ["active", "invalidated"],
            field_name="status",
            source_type="VARCHAR",
        )
        == "VARCHAR"
    )


def test_safe_ddl_keeps_timestamptz_as_timestamp_logical():
    """Identity mappings project ddl_type(pg, TIMESTAMP)=TIMESTAMPTZ — must not widen to TEXT."""
    samples = ["2024-12-31T23:59:59+00:00", "1735689600"]
    assert (
        safe_ddl_logical_type(
            "TIMESTAMPTZ",
            samples,
            field_name="measured_at",
            source_type="TIMESTAMP",
        )
        == "TIMESTAMP"
    )


def test_resolve_target_columns_new_table_widens_status_boolean():
    headers = ["status", "id"]
    rows = [["active"], ["invalidated"], ["pending"]]
    mappings = [
        {"source": "status", "target": "status", "target_type": "BOOLEAN"},
        {"source": "id", "target": "id", "target_type": "VARCHAR"},
    ]
    samples = sample_values_by_source_from_batch(headers, rows, mappings)
    cols, types = resolve_target_columns(
        mappings,
        {"status": "VARCHAR", "id": "VARCHAR"},
        sample_values_by_source=samples,
        table_exists=False,
    )
    by = dict(zip(cols, types))
    assert by["status"].upper() == "VARCHAR"


def test_resolve_target_columns_existing_table_keeps_proposed_when_no_widen_flag():
    """When table_exists is not False, do not force widen (existing dest types win via dest_types)."""
    mappings = [{"source": "status", "target": "status", "target_type": "BOOLEAN"}]
    cols, types = resolve_target_columns(
        mappings,
        {"status": "VARCHAR"},
        sample_values_by_source={"status": ["active"]},
        table_exists=True,
    )
    assert dict(zip(cols, types))["status"] == "BOOLEAN"


def test_coercion_marks_existing_boolean_status_as_destination_exists():
    report = analyze_coercion(
        sample_rows=[{"status": "active"}, {"status": "invalidated"}],
        mappings=[{"source": "status", "target": "status", "target_type": "BOOLEAN"}],
        source_types={"status": "VARCHAR"},
        dest_types={"status": "BOOLEAN"},
        dest_db_type="snowflake",
        table_exists=True,
    )
    assert report["has_blocking_failures"] is True
    col = report["by_source"]["status"]
    assert col["destination_exists"] is True
    assert col["severity"] == "block"


def test_sqlite_path_allowlist_blocks_escape(monkeypatch, tmp_path):
    root = tmp_path / "allowed"
    root.mkdir()
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(root))
    ok = sqlite_file_path(str(root / "app.db"), "", "")
    assert ok.endswith("app.db")
    with pytest.raises(ValueError, match="DATAFLOW_SQLITE_ROOT"):
        sqlite_file_path(str(tmp_path / "outside.db"), "", "")
