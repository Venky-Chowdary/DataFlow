"""SaaS writers omit reader-null instead of writing the wire token.

Notion / Airtable / HubSpot used to treat only Python None / Missing as
absent. After extract emits SQL_NULL_SENTINEL, that spelling became a
CRM/title cell, and True missed dest true on HubSpot / Notion text.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.airtable_writer import _present_fields  # noqa: E402
from connectors.hubspot_writer import hubspot_property_fields  # noqa: E402
from connectors.notion_writer import _as_property_value  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)

_NULL_WIRES = (None, SQL_NULL_SENTINEL, "__df_ddb_null__", Missing, DF_MISSING_SENTINEL)


def test_notion_reader_null_omits_not_sentinel_text():
    warnings: list[str] = []
    for wire in _NULL_WIRES:
        for kind in ("title", "rich_text", "url", "email", "select", "number"):
            assert _as_property_value(wire, kind, "Col", warnings, 1) is None, (kind, wire)


def test_notion_true_shares_dest_true_on_text():
    warnings: list[str] = []
    assert _as_property_value(True, "select", "Flag", warnings, 1) == {
        "select": {"name": "true"}
    }
    assert _as_property_value("true", "select", "Flag", warnings, 1) == {
        "select": {"name": "true"}
    }
    assert _as_property_value(0, "rich_text", "Note", warnings, 1) == {
        "rich_text": [{"type": "text", "text": {"content": "0"}}]
    }


def test_airtable_reader_null_omits_zero_stays():
    row = {
        "name": "kept",
        "gone": SQL_NULL_SENTINEL,
        "blank": "",
        "zero": 0,
        "flag": False,
    }
    present = _present_fields(row, ["name", "gone", "blank", "zero", "flag"])
    assert present == {"name": "kept", "zero": 0, "flag": False}


def test_hubspot_reader_null_omits_true_shares_dest():
    props = hubspot_property_fields(
        [
            ("email", "a@b.test"),
            ("note", SQL_NULL_SENTINEL),
            ("flag", True),
            ("zero", 0),
            ("empty", ""),
            ("missing", Missing),
        ]
    )
    assert props == {"email": "a@b.test", "flag": "true", "zero": "0"}
    assert "True" not in props.values()
    assert SQL_NULL_SENTINEL not in props.values()
