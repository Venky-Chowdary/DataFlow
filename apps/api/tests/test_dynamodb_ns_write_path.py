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


def test_ns_money_text_is_refused_like_a_scalar_decimal():
    """An NS member is bound by the same write path as a scalar DECIMAL.

    Currency text there means no conversion was declared, so stripping the
    mark would invent the number — refuse and quarantine instead.
    """
    with pytest.raises(ValueError, match="currency marker"):
        _to_dynamo_value({"_df_ddb_set": "NS", "v": ["$1,234", "€1.234,56"]}, "ARRAY")


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
    typed = _to_attr({"_df_ddb_set": "NS", "v": [Decimal("1234.56")]}, "ARRAY")
    assert {Decimal(x) for x in typed["NS"]} == {Decimal("1234.56")}
    round_trip = _to_dynamo_value('{"_df_ddb_set":"NS","v":["1","2.5"]}', "ARRAY")
    assert round_trip == {Decimal("1"), Decimal("2.5")}


def test_json_string_envelope_refusal_is_not_swallowed_into_a_map():
    """A refused member must raise, not land the envelope as a document.

    The JSON-string form only falls through when the *parse* fails; letting a
    refusal fall through wrote ``{"_df_ddb_set": "NS", ...}`` as a Dynamo map,
    so a declared number set silently became a document on the destination.
    """
    with pytest.raises(ValueError, match="currency marker"):
        _to_dynamo_value('{"_df_ddb_set":"NS","v":["$1,234"]}', "ARRAY")
    with pytest.raises(ValueError, match="refuse"):
        _to_dynamo_value('{"_df_ddb_set":"NS","v":["1.234"]}', "ARRAY")


def test_key_n_uses_write_path_not_decimal_text():
    with pytest.raises(ValueError, match="key type N refused"):
        _coerce_dynamo_cell(
            "$1,234", col="pk", logical_type="DECIMAL", key_types={"pk": "N"}
        )
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
