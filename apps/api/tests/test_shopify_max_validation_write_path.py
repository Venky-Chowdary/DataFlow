"""Shopify metafield max uses write-path integers, not int(float()).

int(float('1.234')) invented VARCHAR(1). 2**53+1 collapsed.
Locale money binds. Auto 1,234 stays unset (default single-line width).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.saas_write_carriers import (  # noqa: E402
    shopify_live_types_for_columns,
    shopify_max_validation,
    shopify_metafield_type_to_carrier,
)


def test_plain_max_still_binds():
    assert shopify_max_validation("40") == 40
    assert shopify_max_validation(80) == 80
    assert (
        shopify_metafield_type_to_carrier(
            "single_line_text_field", max_validation=80
        )
        == "VARCHAR(80)"
    )


def test_locale_money_binds():
    assert shopify_max_validation("$1,234") == 1234


def test_auto_grouping_does_not_invent_width():
    assert shopify_max_validation("1,234") is None
    assert shopify_max_validation("1.234") is None
    live = shopify_live_types_for_columns(
        "customers",
        ["custom.note"],
        metafield_defs=[
            {
                "namespace": "custom",
                "key": "note",
                "type": "single_line_text_field",
                "validations": [{"name": "max", "value": "1,234"}],
            }
        ],
    )
    assert live["custom.note"].startswith("VARCHAR(")
    assert live["custom.note"] != "VARCHAR(1234)"
    assert live["custom.note"] != "VARCHAR(1)"


def test_ieee_lossy_mantissa_stays_exact_or_unset():
    assert shopify_max_validation("9007199254740993") == 9007199254740993
    assert shopify_max_validation(True) is None
    assert shopify_max_validation(False) is None
    assert shopify_max_validation("true") is None
