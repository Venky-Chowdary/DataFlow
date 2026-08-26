"""Redis JSON docs use json_loads_exact, not stdlib float().

A stored {\"amt\": 1.234567890123456789} collapsed to IEEE on source flatten
and on sparse MERGE reread. IEEE-exact 1.5 stays float. Invalid docs stay None.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.redis_reader import load_redis_json_doc  # noqa: E402

LONG = "1.234567890123456789"


def test_load_redis_json_doc_keeps_long_fraction():
    raw = f'{{"amt": {LONG}, "n": 1.5, "id": 1}}'
    doc = load_redis_json_doc(raw)
    assert doc["amt"] == Decimal(LONG)
    assert doc["amt"] != json.loads(raw)["amt"]
    assert doc["n"] == 1.5
    assert isinstance(doc["n"], float)
    assert doc["id"] == 1
    assert load_redis_json_doc(raw.encode("utf-8")) == doc


def test_load_redis_json_doc_invalid_is_none():
    assert load_redis_json_doc("{not-json}") is None
    assert load_redis_json_doc(None) is None
    assert load_redis_json_doc(42) is None
    assert load_redis_json_doc("[1, 2]") == [1, 2]
