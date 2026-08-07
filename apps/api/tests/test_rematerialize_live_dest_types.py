"""Shared rematerialize_live_dest_types — no Map VARCHAR gap-fill."""

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
