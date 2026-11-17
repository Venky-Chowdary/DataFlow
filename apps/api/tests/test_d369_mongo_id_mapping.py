"""D369: an unrelated column is never proposed as MongoDB ``_id``.

DATE, INT and TEXT columns beside an existing ``_id`` stay off that key.
An omitted column is not part of read-back. Migration rule 4.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from services.reconciliation import sample_compare_rows  # noqa: E402
from services.semantic_mapper import map_columns  # noqa: E402


def test_date_int_and_text_are_not_mapped_onto_id() -> None:
    mappings = map_columns(
        ["id", "d", "qty", "name", "note"],
        ["_id", "id", "loyalty_tier", "app_meta"],
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2"]},
            {"name": "d", "inferred_type": "DATE", "samples": ["2026-01-01", "2026-02-02"]},
            {"name": "qty", "inferred_type": "INTEGER", "samples": ["10", "20"]},
            {"name": "name", "inferred_type": "VARCHAR", "samples": ["ada", "beau"]},
            {"name": "note", "inferred_type": "TEXT", "samples": ["", "memo"]},
        ],
        target_schemas=[
            {"name": "_id", "inferred_type": "OBJECTID"},
            {"name": "id", "inferred_type": "INTEGER"},
            {"name": "loyalty_tier", "inferred_type": "VARCHAR"},
            {"name": "app_meta", "inferred_type": "JSON"},
        ],
        destination_db_type="mongodb",
        destination_table_exists=True,
        source_db_type="postgresql",
    )
    onto_id = [m for m in mappings if str(m.get("target")) == "_id"]
    assert onto_id == []
    by_source = {m["source"]: m["target"] for m in mappings}
    assert by_source.get("id") == "id"


def test_source_id_may_map_onto_destination_id() -> None:
    mappings = map_columns(
        ["_id", "name"],
        ["_id", "name"],
        source_schemas=[
            {"name": "_id", "inferred_type": "OBJECTID"},
            {"name": "name", "inferred_type": "VARCHAR"},
        ],
        target_schemas=[
            {"name": "_id", "inferred_type": "OBJECTID"},
            {"name": "name", "inferred_type": "VARCHAR"},
        ],
        destination_db_type="mongodb",
        destination_table_exists=True,
    )
    by_source = {m["source"]: m["target"] for m in mappings}
    assert by_source.get("_id") == "_id"


def test_omitted_column_is_not_compared_on_read_back() -> None:
    result = sample_compare_rows(
        [{"id": "1", "d": None, "name": "ada"}],
        [{"id": "1", "_id": "6afb9c4272091f51d66bf9e5", "name": "ada"}],
        [
            {"source": "id", "target": "id"},
            {"source": "d", "target": "_id", "transform": "omit"},
            {"source": "name", "target": "name"},
        ],
        sort_key="id",
    )
    assert result["passed"] is True
    assert result["mismatches"] == []
    assert result["compared"] == 2


@pytest.mark.parametrize("flag", ["omit", "omitted"])
def test_omit_and_omitted_keys_are_not_compared(flag: str) -> None:
    mapping = {"source": "d", "target": "_id", flag: True}
    result = sample_compare_rows(
        [{"id": "1", "d": None, "name": "ada"}],
        [{"id": "1", "_id": "6afb9c4272091f51d66bf9e5", "name": "ada"}],
        [
            {"source": "id", "target": "id"},
            mapping,
            {"source": "name", "target": "name"},
        ],
        sort_key="id",
    )
    assert result["passed"] is True
    assert result["mismatches"] == []


def test_llm_refine_cannot_map_a_non_id_source_onto_id(monkeypatch) -> None:
    from services.mapping_pipeline import run_mapping_pipeline

    seen = {"called": False}

    def _refine(mappings, *_args, **_kwargs):
        seen["called"] = True
        return (
            [
                {
                    "source": "created",
                    "target": "_id",
                    "confidence": 0.99,
                    "target_type": "OBJECTID",
                }
            ],
            {"used": True},
        )

    monkeypatch.setattr("services.llm_mapping.refine_mappings_with_llm", _refine)
    result = run_mapping_pipeline(
        source_columns=["created"],
        target_columns=["_id"],
        source_schemas=[{"name": "created", "inferred_type": "DATE"}],
        target_schemas=[{"name": "_id", "inferred_type": "OBJECTID", "primary_key": True}],
        destination_db_type="mongodb",
        source_db_type="postgresql",
        destination_table_exists=True,
        use_llm=True,
        confidence_threshold=0.5,
    )
    assert seen["called"] is True
    assert all(m.get("target") != "_id" for m in result["mappings"])


def test_constraint_pass_protects_primary_key_and_identity() -> None:
    from services.mapping_constraints import enforce_destination_constraints

    kept, dropped, _invented = enforce_destination_constraints(
        [
            {"source": "email", "target": "_id", "confidence": 0.99},
            {"source": "sku", "target": "id", "confidence": 0.99},
            {"source": "id", "target": "id", "confidence": 0.99},
        ],
        ["_id", "id"],
        target_schemas=[
            {"name": "_id", "primary_key": True},
            {"name": "id", "identity": True},
        ],
    )
    pairs = {(m["source"], m["target"]) for m in kept}
    assert ("email", "_id") not in pairs
    assert ("sku", "id") not in pairs
    assert ("id", "id") in pairs
    assert "email" in dropped and "sku" in dropped
