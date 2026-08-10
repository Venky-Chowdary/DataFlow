"""An all-NULL catalog-declared TEXT column must not be capped at the floor."""

from __future__ import annotations

from services.mapping_pipeline import (
    _UNTYPED_VARCHAR_CONF_CAP,
    _demote_untyped_varchar_confidence,
)

SCHEMAS = [{"name": "country", "inferred_type": "TEXT", "samples": []}]


def _mapping(target_type: str) -> dict:
    return {
        "source": "country",
        "target": "country",
        "source_type": "TEXT",
        "target_type": target_type,
        "confidence": 0.99,
    }


def test_catalog_typed_text_to_text_keeps_confidence() -> None:
    out = _demote_untyped_varchar_confidence(
        [_mapping("TEXT")],
        source_schemas=SCHEMAS,
        source_types_authoritative=True,
    )
    assert out[0]["confidence"] == 0.99
    assert not out[0].get("requires_review")


def test_catalog_typed_text_to_clob_keeps_confidence() -> None:
    out = _demote_untyped_varchar_confidence(
        [_mapping("CLOB")],
        source_schemas=SCHEMAS,
        source_types_authoritative=True,
    )
    assert out[0]["confidence"] == 0.99


def test_catalog_typed_text_to_numeric_is_still_demoted() -> None:
    out = _demote_untyped_varchar_confidence(
        [_mapping("NUMBER(10,2)")],
        source_schemas=SCHEMAS,
        source_types_authoritative=True,
    )
    assert out[0]["confidence"] == _UNTYPED_VARCHAR_CONF_CAP
    assert out[0]["requires_review"] is True


def test_unproven_varchar_source_is_still_demoted() -> None:
    out = _demote_untyped_varchar_confidence(
        [_mapping("TEXT")],
        source_schemas=[{"name": "country", "inferred_type": "VARCHAR", "samples": []}],
        source_types_authoritative=False,
    )
    assert out[0]["confidence"] == _UNTYPED_VARCHAR_CONF_CAP
    assert "weak type evidence" in out[0]["reasoning"]


def test_sampled_column_is_never_demoted() -> None:
    out = _demote_untyped_varchar_confidence(
        [_mapping("TEXT")],
        source_schemas=[{"name": "country", "inferred_type": "TEXT", "samples": ["IN"]}],
        source_types_authoritative=False,
    )
    assert out[0]["confidence"] == 0.99
