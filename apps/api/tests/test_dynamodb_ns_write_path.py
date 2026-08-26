"""DynamoDB NS sets and key type N use the write-path decimal parser.

Decimal(str(x)) invented Auto 1.234 as a number and missed $1,234 / €1.234
that scalar DECIMAL already stores via coerce_decimal_wire. Reader Decimals
(dest-canonical 1.234) still bind — Auto refuse is for string tokens only.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.dynamodb_writer import (  # noqa: E402
    _coerce_dynamo_cell,
    _to_attr,
    _to_dynamo_value,
)


def test_ns_locale_money_binds_like_scalar_decimal():
    got = _to_dynamo_value(
        {"_df_ddb_set": "NS", "v": ["$1,234", "€1.234,56"]},
        "ARRAY",
    )
    assert got == {Decimal("1234"), Decimal("1234.56")}


def test_ns_auto_ambiguous_grouping_refuses():
    for token in ("1.234", "1,234", "1.000", "1.005"):
        with pytest.raises(ValueError, match="refuse"):
            _to_dynamo_value({"_df_ddb_set": "NS", "v": [token]}, "ARRAY")


def test_ns_plain_and_reader_decimals_still_bind():
    got = _to_dynamo_value({"_df_ddb_set": "NS", "items": ["1", "2.5"]}, "ARRAY")
    assert got == {Decimal("1"), Decimal("2.5")}
    # Reader envelopes carry Decimal, not locale strings.
    reader = _to_dynamo_value(
        {"_df_ddb_set": "NS", "v": [Decimal("1.234"), Decimal("2.5")]},
        "ARRAY",
    )
    assert reader == {Decimal("1.234"), Decimal("2.5")}


def test_ns_envelope_serializes_and_json_string_round_trips():
    ns = _to_attr({"_df_ddb_set": "NS", "items": ["1", "2.5"]}, "ARRAY")
    assert "NS" in ns
    assert {Decimal(x) for x in ns["NS"]} == {Decimal("1"), Decimal("2.5")}
    money = _to_attr({"_df_ddb_set": "NS", "v": ["$1,234.56"]}, "ARRAY")
    assert {Decimal(x) for x in money["NS"]} == {Decimal("1234.56")}
    from_json = _to_dynamo_value(
        '{"_df_ddb_set":"NS","v":["$1,234"]}',
        "ARRAY",
    )
    assert from_json == {Decimal("1234")}


def test_key_n_uses_write_path_not_decimal_text():
    assert _coerce_dynamo_cell(
        "$1,234", col="pk", logical_type="DECIMAL", key_types={"pk": "N"}
    ) == Decimal("1234")
    assert _coerce_dynamo_cell(
        "9", col="pk", logical_type="INTEGER", key_types={"pk": "N"}
    ) == Decimal("9")
    assert _coerce_dynamo_cell(
        Decimal("1.234"), col="pk", logical_type="DECIMAL", key_types={"pk": "N"}
    ) == Decimal("1.234")
    with pytest.raises(ValueError, match="key type N refused"):
        _coerce_dynamo_cell("1.234", col="pk", logical_type="DECIMAL", key_types={"pk": "N"})
    with pytest.raises(ValueError, match="key type N refused"):
        _coerce_dynamo_cell("x", col="pk", logical_type="INTEGER", key_types={"pk": "N"})
