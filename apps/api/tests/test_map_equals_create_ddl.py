"""Map≡CREATE fidelity — approved target_type must never be rewritten by samples."""

from __future__ import annotations

from connectors.writer_common import resolve_target_columns, sample_values_by_source_from_batch
from services.schema_inference import safe_ddl_logical_type


def test_honor_explicit_never_rewrites_boolean_stamp_to_varchar():
    """P0 Migration Assurance: Map BOOLEAN stays BOOLEAN even when samples are enums.

    Unfit values must quarantine on write — they must not mutate CREATE DDL.
    """
    assert (
        safe_ddl_logical_type(
            "BOOLEAN",
            ["active", "invalidated", "pending"],
            field_name="status",
            source_type="VARCHAR",
            honor_explicit=True,
        )
        == "BOOLEAN"
    )


def test_resolve_target_columns_map_equals_create_for_explicit_boolean():
    headers = ["status", "id"]
    rows = [["active"], ["invalidated"], ["pending"]]
    mappings = [
        {"source": "status", "target": "status", "target_type": "BOOLEAN"},
        {"source": "id", "target": "id", "target_type": "INTEGER"},
    ]
    samples = sample_values_by_source_from_batch(headers, rows, mappings)
    cols, types = resolve_target_columns(
        mappings,
        {"status": "VARCHAR", "id": "INTEGER"},
        sample_values_by_source=samples,
        table_exists=False,
    )
    by = dict(zip(cols, types))
    assert by["status"] == "BOOLEAN"
    assert by["id"] == "INTEGER"


def test_inferred_boolean_helper_may_widen_when_not_honoring_explicit():
    """Helper still widens when honor_explicit=False — resolve_target_columns never uses that path for CREATE."""
    assert (
        safe_ddl_logical_type(
            "BOOLEAN",
            ["active", "invalidated"],
            field_name="status",
            source_type="VARCHAR",
            honor_explicit=False,
        )
        == "VARCHAR"
    )


def test_create_new_without_map_stamp_keeps_source_carrier_not_sample_invent():
    cols, types = resolve_target_columns(
        [{"source": "status", "target": "status"}],
        {"status": "VARCHAR"},
        sample_values_by_source={"status": ["true", "false"]},
        table_exists=False,
    )
    assert dict(zip(cols, types))["status"].upper() != "BOOLEAN"
