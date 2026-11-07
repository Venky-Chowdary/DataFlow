"""Shared rematerialize / Studio-or-Map helpers — no Map VARCHAR gap-fill."""

from __future__ import annotations


def test_rematerialize_live_dest_types_full_coverage():
    from connectors.writer_common import rematerialize_live_dest_types

    out = rematerialize_live_dest_types(
        {"qty": "INTEGER", "QTY": "INTEGER"},
        ["qty"],
        product="PostgreSQL",
    )
    assert out is not None
    assert "INT" in str(out.get("qty") or "").upper()


def test_rematerialize_live_dest_types_refuses_gap_fill():
    from connectors.writer_common import rematerialize_live_dest_types

    out = rematerialize_live_dest_types(
        {"id": "INTEGER"},
        ["id", "amount"],
        product="MySQL",
    )
    assert out is None


def test_rematerialize_live_dest_types_empty_physical():
    from connectors.writer_common import rematerialize_live_dest_types

    assert rematerialize_live_dest_types({}, ["id"], product="SQLite") is None
    assert rematerialize_live_dest_types(None, ["id"], product="SQLite") is None


def test_resolve_studio_or_map_full_studio():
    from connectors.writer_common import resolve_studio_or_map_dest_types

    dest, err = resolve_studio_or_map_dest_types(
        ["id", "qty"],
        [
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
        ],
        {"id": "VARCHAR", "qty": "VARCHAR"},
        studio_types={"id": "INTEGER", "qty": "DECIMAL(10,2)"},
        product="DynamoDB",
    )
    assert err is None
    assert "INT" in str(dest.get("id") or "").upper()
    assert "DECIMAL" in str(dest.get("qty") or "").upper()


def test_resolve_studio_or_map_partial_studio_refuses():
    from connectors.writer_common import resolve_studio_or_map_dest_types

    dest, err = resolve_studio_or_map_dest_types(
        ["id", "qty"],
        [
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
        ],
        {"id": "VARCHAR", "qty": "VARCHAR"},
        studio_types={"id": "INTEGER"},
        product="MongoDB",
    )
    assert err is not None
    assert "qty" in err.lower()
    assert "id" in dest


def test_resolve_studio_or_map_no_studio_uses_map():
    from connectors.writer_common import resolve_studio_or_map_dest_types

    dest, err = resolve_studio_or_map_dest_types(
        ["id"],
        [{"source": "id", "target": "id", "target_type": "INTEGER"}],
        {"id": "INTEGER"},
        studio_types=None,
        product="Redis",
    )
    assert err is None
    assert "INT" in str(dest.get("id") or "").upper()
