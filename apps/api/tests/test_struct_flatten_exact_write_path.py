"""STRUCT flatten parses JSON objects through json_loads_exact.

json.loads collapsed 1.234567890123456789 before flatten promoted the leaf.
Integers and IEEE-exact 1.5 stay native.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.json_intelligence import flatten_struct_field  # noqa: E402

LONG = "1.234567890123456789"


def test_flatten_long_fraction_stays_identity():
    flat = flatten_struct_field(
        f'{{"amt": {LONG}, "label": "x"}}',
        parent_key="payload",
    )
    assert flat["payload_amt"] == Decimal(LONG)
    assert flat["payload_label"] == "x"


def test_flatten_plain_number_still_binds():
    flat = flatten_struct_field('{"qty": 3, "amt": 1.5}', parent_key="payload")
    assert flat["payload_qty"] == 3
    assert flat["payload_amt"] == 1.5


def test_malformed_object_stays_empty():
    assert flatten_struct_field("{not-json", parent_key="payload") == {}
    assert flatten_struct_field("[1,2]", parent_key="payload") == {}
