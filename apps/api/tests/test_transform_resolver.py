"""Tests for unified transform resolver."""

from services.transform_resolver import (
    ENGINE_TO_UI,
    UI_TO_ENGINE,
    attach_transforms_to_mappings,
    resolve_transform,
)


def test_ui_transform_maps_to_engine():
    assert resolve_transform({"source": "a", "target": "b", "transform": "cast_number"}) == "decimal"
    assert resolve_transform({"source": "a", "target": "b", "transform": "hash_pii"}) == "hash_pii"


def test_none_transform_does_not_resolve_to_trim():
    assert UI_TO_ENGINE["none"] == "none"
    assert resolve_transform({"source": "name", "target": "name", "transform": "none"}) == "none"


def test_infer_when_no_transform():
    t = resolve_transform(
        {"source": "AMT", "target": "amount"},
        column_types={"AMT": "VARCHAR"},
        dest_types={"amount": "DECIMAL"},
    )
    assert t in {"decimal", "none"}


def test_attach_transforms_to_all_mappings():
    out = attach_transforms_to_mappings(
        [{"source": "id", "target": "id", "confidence": 0.95}],
        column_types={"id": "INTEGER"},
        dest_types={"id": "INTEGER"},
    )
    assert out[0]["transform"]


def test_engine_to_ui_coverage():
    assert ENGINE_TO_UI["decimal"] == "cast_number"
    assert ENGINE_TO_UI["integer"] == "cast_integer"
    assert ENGINE_TO_UI["datetime"] == "date_iso"
    assert ENGINE_TO_UI["json"] == "parse_json"
    assert ENGINE_TO_UI["binary"] == "binary"
    assert ENGINE_TO_UI["phone"] == "phone"
    assert ENGINE_TO_UI["none"] == "none"
    assert ENGINE_TO_UI["identity"] == "none"
    assert UI_TO_ENGINE["cast_integer"] == "integer"
    assert UI_TO_ENGINE["parse_json"] == "json"
    assert UI_TO_ENGINE["identity_specialty"] == "none"
