"""Tests for unified transform resolver."""

from services.transform_resolver import (
    ENGINE_TO_UI,
    attach_transforms_to_mappings,
    resolve_transform,
)


def test_ui_transform_maps_to_engine():
    assert resolve_transform({"source": "a", "target": "b", "transform": "cast_number"}) == "decimal"
    assert resolve_transform({"source": "a", "target": "b", "transform": "hash_pii"}) == "hash_pii"


def test_infer_when_no_transform():
    t = resolve_transform(
        {"source": "AMT", "target": "amount"},
        column_types={"AMT": "VARCHAR"},
        dest_types={"amount": "DECIMAL"},
    )
    assert t in {"decimal", "trim"}


def test_attach_transforms_to_all_mappings():
    out = attach_transforms_to_mappings(
        [{"source": "id", "target": "id", "confidence": 0.95}],
        column_types={"id": "INTEGER"},
        dest_types={"id": "INTEGER"},
    )
    assert out[0]["transform"]


def test_engine_to_ui_coverage():
    assert ENGINE_TO_UI["decimal"] == "cast_number"
    assert ENGINE_TO_UI["datetime"] == "date_iso"
