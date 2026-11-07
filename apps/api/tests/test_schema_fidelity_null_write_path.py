"""Catalog DEFAULT literals use present_cell_text, not a second stringify.

Reader-wired SQL_NULL_SENTINEL used to look like a CREATE DEFAULT token.
SQL expressions stay as written. True and dest "true" share one literal.
Empty / whitespace stay absent (no default).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.schema_fidelity import (  # noqa: E402
    build_catalog_from_introspect,
    catalog_from_payload,
)
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_introspect_defaults_skip_reader_null():
    cat = build_catalog_from_introspect(
        dialect="postgresql",
        columns=["status", "flag", "note", "qty"],
        defaults={
            "status": "'active'",
            "flag": True,
            "note": SQL_NULL_SENTINEL,
            "qty": None,
            "blank": "   ",
        },
    )
    assert cat.defaults["status"] == "'active'"
    assert cat.defaults["flag"] == "true"
    assert "note" not in cat.defaults
    assert "qty" not in cat.defaults
    assert "blank" not in cat.defaults


def test_payload_defaults_skip_reader_null():
    cat = catalog_from_payload(
        {
            "dialect": "postgresql",
            "columns": ["email", "flag"],
            "defaults": {
                "email": "'x'",
                "flag": True,
                "gone": SQL_NULL_SENTINEL,
            },
        }
    )
    assert cat is not None
    assert cat.defaults["email"] == "'x'"
    assert cat.defaults["flag"] == "true"
    assert "gone" not in cat.defaults
