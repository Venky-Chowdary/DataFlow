"""Notion number properties use coerce_float_wire, not float(text).

float(text) invented Auto 1.234 as a JSON number and missed $1,234
the write path stores. Notion's wire is still IEEE float after a
successful bind — that is the API carrier, not a second parser.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.notion_writer import _as_property_value  # noqa: E402


def test_notion_number_locale_money_binds():
    warnings: list[str] = []
    assert _as_property_value("$1,234", "number", "Price", warnings, 1) == {
        "number": 1234.0
    }
    assert _as_property_value("€1.234", "number", "Price", warnings, 1) == {
        "number": 1234.0
    }
    assert _as_property_value("9.99", "number", "Price", warnings, 1) == {
        "number": 9.99
    }
    assert _as_property_value(Decimal("1.2345"), "number", "Price", warnings, 1) == {
        "number": 1.2345
    }


def test_notion_number_auto_grouping_refuses():
    warnings: list[str] = []
    for token in ("1.234", "1,234", "1.000", "1.005"):
        with pytest.raises(ValueError, match="refused"):
            _as_property_value(token, "number", "Price", warnings, 1)


def test_notion_number_empty_still_omits():
    warnings: list[str] = []
    assert _as_property_value("", "number", "Price", warnings, 1) is None
