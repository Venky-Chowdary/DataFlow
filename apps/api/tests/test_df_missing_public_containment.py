"""Audit §2.4 — ``__DF_MISSING__`` must not escape public mapped-row APIs."""

from __future__ import annotations

from connectors.writer_common import build_mapped_rows_with_details, to_json_value
from services.value_serializer import (
    DF_MISSING_SENTINEL,
    Missing,
    is_missing_sentinel,
    public_mapped_cell,
)


def test_coerce_null_returns_python_none_not_wire_string():
    mapped, errors, details = build_mapped_rows_with_details(
        headers=["is_active"],
        data_rows=[["true"], ["maybe"]],
        mappings=[{"source": "is_active", "target": "is_active", "transform": "boolean"}],
        target_cols=["is_active"],
        column_types={"is_active": "BOOLEAN"},
        error_policy="coerce_null",
        allow_job_coerce_null=True,
    )
    assert mapped == [(True,), (None,)]
    assert all(cell != DF_MISSING_SENTINEL for row in mapped for cell in row)
    assert errors and details


def test_sparse_missing_input_returns_missing_singleton_not_wire_string():
    """Sparse CDC omit cell stays ``Missing`` — never the ``__DF_MISSING__`` str."""
    mapped, _errors, _details = build_mapped_rows_with_details(
        headers=["id", "note"],
        data_rows=[["1", DF_MISSING_SENTINEL]],
        mappings=[
            {"source": "id", "target": "id", "transform": "integer"},
            {"source": "note", "target": "note", "transform": "none"},
        ],
        target_cols=["id", "note"],
        column_types={"id": "INTEGER", "note": "TEXT"},
        error_policy="quarantine",
    )
    assert mapped == [(1, Missing)] or (
        mapped and mapped[0][0] == 1 and is_missing_sentinel(mapped[0][1])
    )
    assert not isinstance(mapped[0][1], str)
    assert mapped[0][1] is Missing


def test_to_json_value_never_emits_wire_sentinel():
    assert to_json_value(Missing, "c", {}) is None
    assert to_json_value(DF_MISSING_SENTINEL, "c", {}) is None


def test_public_mapped_cell_dense_null():
    assert public_mapped_cell(Missing, dense_null=True) is None
    assert public_mapped_cell(DF_MISSING_SENTINEL, dense_null=True) is None
    assert public_mapped_cell(Missing, dense_null=False) is Missing
