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
    assert resolve_transform({"source": "a", "target": "b", "transform": "redact"}) == "redact"


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


def test_omitted_transform_does_not_invent_date_into_varchar():
    """Map strips transform=none; write-path must not re-invent Date→ISO."""
    t = resolve_transform(
        {"source": "event_date", "target": "event_date", "target_type": "VARCHAR"},
        column_types={"event_date": "VARCHAR"},
        dest_types={"event_date": "VARCHAR"},
    )
    assert t == "none"


def test_resolve_transform_live_dest_beats_map_boolean_stamp():
    """Map BOOLEAN over live VARCHAR must not invent cast_boolean."""
    t = resolve_transform(
        {
            "source": "status",
            "target": "status",
            "target_type": "BOOLEAN",
            "transform": "none",
        },
        column_types={"status": "VARCHAR"},
        dest_types={"status": "VARCHAR"},
    )
    assert t == "none"


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

def test_declared_numeric_source_still_validates_the_wire():
    """A declared-INTEGER source column can still carry an unparsable cell.

    Map calls INTEGER→INTEGER identity widening (no invented parse). The write
    path must not take that as permission to hand the driver whatever the row
    carried: one bad cell would fail the whole batch instead of quarantining
    the row that caused it.
    """
    from connectors.writer_common import apply_transform

    for src_type, dest_type in (
        ("INTEGER", "INTEGER"),
        ("DECIMAL(10,2)", "DECIMAL(10,2)"),
        ("DOUBLE PRECISION", "DOUBLE PRECISION"),
    ):
        t = resolve_transform(
            {"source": "amt", "target": "amt"},
            column_types={"amt": src_type},
            dest_types={"amt": dest_type},
        )
        assert t in {"integer", "decimal"}, (src_type, dest_type, t)
        _value, err = apply_transform("bad", t)
        assert err, f"{src_type}→{dest_type} accepted an unparsable cell"


def test_numeric_wire_guard_leaves_text_and_declared_transforms_alone():
    assert (
        resolve_transform(
            {"source": "note", "target": "note"},
            column_types={"note": "INTEGER"},
            dest_types={"note": "VARCHAR(50)"},
        )
        == "none"
    )
    assert (
        resolve_transform(
            {"source": "amt", "target": "amt", "transform": "currency"},
            column_types={"amt": "VARCHAR"},
            dest_types={"amt": "DECIMAL(10,2)"},
        )
        == "currency"
    )
