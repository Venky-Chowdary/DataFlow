"""Mongo JSON/OBJECT/ARRAY/VARIANT bind uses json_loads_exact.

stdlib json.loads collapsed 1.234567890123456789 before BSON insert.
IEEE-exact 1.5 stays float. Poison and non-finite still refuse.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.mongodb_writer import bind_mongo_json_document  # noqa: E402

LONG = "1.234567890123456789"


def test_mongo_json_keeps_long_fraction():
    doc = bind_mongo_json_document(f'{{"amt": {LONG}, "n": 1.5, "id": 1}}')
    assert doc["amt"] == Decimal(LONG)
    assert doc["amt"] != json.loads(f'{{"amt": {LONG}}}')["amt"]
    assert doc["n"] == 1.5
    assert isinstance(doc["n"], float)
    assert doc["id"] == 1


def test_mongo_json_passthrough_tree_and_refuse():
    tree = {"amt": Decimal(LONG)}
    assert bind_mongo_json_document(tree) is tree
    assert bind_mongo_json_document([1, 2]) == [1, 2]
    with pytest.raises(ValueError, match="refused"):
        bind_mongo_json_document("{not-json}")
    with pytest.raises(ValueError, match="refused"):
        bind_mongo_json_document("NaN")
    with pytest.raises(ValueError, match="refused"):
        bind_mongo_json_document(42)
