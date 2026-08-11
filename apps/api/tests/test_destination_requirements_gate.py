"""G14 — a required destination column nothing fills blocks at Validate."""

from __future__ import annotations

from services.destination_requirements_gate import (
    build_destination_requirements_gate,
)

_MAPPED = [
    {"source": "id", "target": "id", "confidence": 0.99},
    {"source": "email", "target": "email", "confidence": 0.99},
]
_NULLS = {"id": False, "email": True, "tenant_id": False}


def _gate(**kwargs):
    args = {
        "destination_table_exists": True,
        "column_nullability": _NULLS,
        "column_defaults": {},
        "identity_columns": [],
        "generated_columns": [],
        "mappings": _MAPPED,
    }
    args.update(kwargs)
    return build_destination_requirements_gate(**args)


def test_unmapped_not_null_without_default_blocks_before_the_write():
    gate = _gate()

    assert gate["status"] == "block"
    assert gate["details"]["unfilled_required_columns"] == ["tenant_id"]
    assert "tenant_id" in gate["message"]


def test_default_identity_and_generated_columns_are_filled_by_the_engine():
    assert _gate(column_defaults={"tenant_id": "'acme'"})["status"] == "pass"
    assert _gate(identity_columns=["tenant_id"])["status"] == "pass"
    assert _gate(generated_columns=["tenant_id"])["status"] == "pass"


def test_a_mapping_fills_the_column_regardless_of_case():
    mapped = [*_MAPPED, {"source": "tid", "target": "TENANT_ID", "confidence": 0.9}]

    assert _gate(mappings=mapped)["status"] == "pass"


def test_an_intentional_omission_does_not_fill_a_required_column():
    """Declaring a source omitted says nothing about the destination's demand."""
    mapped = [
        *_MAPPED,
        {"source": "tenant_id", "target": "", "intentional_omit": True},
    ]

    assert _gate(mappings=mapped)["status"] == "block"


def test_unreadable_nullability_is_unmeasured_not_pass():
    gate = _gate(column_nullability={})

    assert gate["status"] == "skip"
    assert gate["details"]["reason"] == "nullability_metadata_unavailable"


def test_create_new_destination_has_no_existing_requirements_to_check():
    assert _gate(destination_table_exists=False) is None
    assert _gate(destination_table_exists=None) is None
