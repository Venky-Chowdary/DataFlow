"""Dynamo HASH/RANGE type S keys use present_cell_text, not str(value).

True became True so dest true missed item identity. NaN became nan.
Reader-null and blank still refuse — never empty-string invent.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.dynamodb_writer import _coerce_dynamo_cell  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def _s_key(value):
    return _coerce_dynamo_cell(
        value, col="pk", logical_type="TEXT", key_types={"pk": "S"}
    )


def test_dynamo_s_true_shares_dest_true():
    assert _s_key(True) == "true"
    assert _s_key("true") == "true"
    assert _s_key(True) != "True"
    assert _s_key(False) == "false"
    assert _s_key(0) == "0"


def test_dynamo_s_decimal_uses_dest_wire():
    assert _s_key(Decimal("1E+2")) == "100"
    assert _s_key(Decimal("100")) == "100"
    assert _s_key("100") == "100"


def test_dynamo_s_reader_null_still_refuses():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", "", "   "):
        with pytest.raises(ValueError, match="refused"):
            _s_key(wire)


def test_dynamo_s_nan_refuses_not_nan_token():
    with pytest.raises(ValueError, match="refused"):
        _s_key(float("nan"))
