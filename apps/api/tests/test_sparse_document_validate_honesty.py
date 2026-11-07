"""Sparse documents at Validate: absent key ≠ empty string, native doc wire ≠ loss.

A Mongo/Dynamo/NDJSON record omits keys entirely. Projecting those onto the
column list with ``""`` made every typed transform report
``Empty value cannot coerce to decimal`` for a field the document never
carried, which blocked Validate on effectively any real document collection.
The destination's own document wire (Snowflake VARIANT, MySQL JSON, PG JSONB)
is likewise not a field-DDL collapse.
"""

from __future__ import annotations

import pytest

from services.value_serializer import DF_MISSING_SENTINEL, project_row_cells


def test_absent_key_projects_missing_sentinel_not_empty_string():
    cells = project_row_cells({"id": 1, "balance": None}, ["id", "balance", "score"])
    assert cells[0] == "1"
    # Present-but-null keeps the empty/NULL wire; only the absent key is missing.
    assert cells[1] == ""
    assert cells[2] == DF_MISSING_SENTINEL


def test_reader_marked_missing_survives_projection():
    cells = project_row_cells({"balance": DF_MISSING_SENTINEL}, ["balance"])
    assert cells == [DF_MISSING_SENTINEL]


def test_sparse_docs_do_not_produce_empty_value_dry_run_failures():
    from services.transform_engine import dry_run_sample

    headers = ["id", "balance", "active"]
    rows = [
        project_row_cells(r, headers)
        for r in ({"id": 1, "balance": "100.50", "active": "true"}, {"id": 2})
    ]
    ok, errors = dry_run_sample(
        headers=headers,
        sample_rows=rows,
        mappings=[
            {"source": "id", "target": "id", "target_type": "NUMBER(38,0)"},
            {"source": "balance", "target": "balance", "target_type": "NUMBER(38,10)"},
            {"source": "active", "target": "active", "target_type": "BOOLEAN"},
        ],
        column_types={"id": "INTEGER", "balance": "DECIMAL", "active": "BOOLEAN"},
    )
    assert ok, errors


@pytest.mark.parametrize(
    ("dest_db", "target"),
    [
        ("snowflake", "VARIANT"),
        ("mysql", "JSON"),
        ("postgresql", "JSONB"),
    ],
)
def test_nested_to_native_document_wire_is_not_a_collapse(dest_db, target):
    from services.type_system import is_nested_document_collapse

    assert is_nested_document_collapse("ARRAY", target, dest_db=dest_db) is False
    # Without the dialect the helper still fails closed — callers must pass it.
    assert is_nested_document_collapse("ARRAY", target) is True
