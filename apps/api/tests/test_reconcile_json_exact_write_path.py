"""Gate-8 JSON checksum fold uses json_loads_exact.

stdlib json.loads collapsed 1.234567890123456789 so source and dest could
match after IEEE invent. Long digits stay in the fold. IEEE-exact 1.5
stays a JSON number. Key order still canonicalizes.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.reconciliation import normalize_cell  # noqa: E402

LONG = "1.234567890123456789"
COLLAPSED = str(json.loads(f"[{LONG}]")[0])


def test_normalize_cell_json_keeps_long_fraction_digits():
    src = f'{{"z": 1, "amt": {LONG}, "n": 1.5}}'
    dest_exact = f'{{"n": 1.5, "amt": {LONG}, "z": 1}}'
    dest_ieee = f'{{"n": 1.5, "amt": {COLLAPSED}, "z": 1}}'
    folded = normalize_cell(src)
    assert LONG in folded
    assert normalize_cell(dest_exact) == folded
    assert normalize_cell(dest_ieee) != folded
    assert '"n":1.5' in folded.replace(" ", "")


def test_normalize_cell_json_string_one_stays_string():
    assert normalize_cell('{"s": "1"}') != normalize_cell('{"s": 1}')
