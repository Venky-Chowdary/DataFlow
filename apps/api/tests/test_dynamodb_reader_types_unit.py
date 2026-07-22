"""DynamoDB reader honesty — unit tests without moto (pyexpat-safe)."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.dynamodb_reader import (  # noqa: E402
    DDB_EXPLICIT_NULL,
    DDB_NULL_SENTINEL,
    SET_KIND_KEY,
    _cell,
    _item_to_record,
    infer_logical_from_native,
)
from connectors.header_union import union_attribute_keys  # noqa: E402
from services.transform_engine import apply_transform  # noqa: E402


def test_union_attribute_keys_stable():
    assert union_attribute_keys(["pk"], ["only_a"], ["only_b", "pk"]) == ["pk", "only_a", "only_b"]


def test_explicit_null_cell_and_transform():
    assert _cell(DDB_EXPLICIT_NULL) == DDB_NULL_SENTINEL
    assert _cell(None) == ""
    val, err = apply_transform(DDB_NULL_SENTINEL, "decimal")
    assert err is None and val is None
    val2, err2 = apply_transform(DDB_NULL_SENTINEL, "none")
    assert err2 is None and val2 is None


def test_item_to_record_null_map_list_binary_set():
    item = {
        "pk": {"S": "n"},
        "maybe": {"NULL": True},
        "addr": {"M": {"city": {"S": "Austin"}, "zip": {"N": "78701"}}},
        "tags": {"L": [{"S": "a"}, {"S": "b"}]},
        "bins": {"BS": [b"hello", b"world"]},
        "nums": {"NS": ["1", "2"]},
    }
    rec = _item_to_record(item)
    assert rec["maybe"] is DDB_EXPLICIT_NULL
    assert isinstance(rec["addr"], dict)
    assert rec["addr"]["city"] == "Austin"
    assert isinstance(rec["tags"], list)
    assert rec["tags"] == ["a", "b"]
    assert isinstance(rec["bins"], dict) and rec["bins"].get(SET_KIND_KEY) == "BS"
    assert all(isinstance(x, str) for x in rec["bins"]["v"])
    assert isinstance(rec["nums"], dict) and rec["nums"].get(SET_KIND_KEY) == "NS"


def test_infer_logical_from_native():
    assert infer_logical_from_native(DDB_EXPLICIT_NULL) == "VARCHAR"
    assert infer_logical_from_native(Decimal("1.5")) == "DECIMAL"
    assert infer_logical_from_native(Decimal("3")) == "INTEGER"
    assert infer_logical_from_native({"city": "x"}) == "JSON"
    assert infer_logical_from_native(["a"]) == "ARRAY"
    assert infer_logical_from_native({SET_KIND_KEY: "BS", "v": ["aGVsbG8="]}) == "BINARY"
