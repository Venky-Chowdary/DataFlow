"""Iceberg leftover PK variants use integer_wire_value and present_cell_text.

float('1.000') is_integer invented 1. Locale money $1,234 / €1.234
the leftover long column stores must still expand. After extract emits
SQL_NULL_SENTINEL, the predicate probed the wire token. True added
``True`` and missed dest ``true``.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.iceberg_writer import (  # noqa: E402
    _pk_lookup_part,
    _pk_predicate_variants,
)
from connectors.writer_common import _is_nullish_conflict_key  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_plain_int_string_still_expands():
    assert 42 in _pk_predicate_variants("42")
    assert -7 in _pk_predicate_variants("-7")


def test_locale_money_expands_to_int():
    assert 1234 in _pk_predicate_variants("$1,234")
    assert 1234 in _pk_predicate_variants("€1.234")


def test_auto_three_digit_group_does_not_invent_int():
    for token in ("1.000", "1.234", "1,234"):
        got = _pk_predicate_variants(token)
        assert 1 not in got
        assert 1234 not in got


def test_reader_null_is_sql_null_not_sentinel_token():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__"):
        assert _pk_predicate_variants(wire) == [None]
        assert _pk_lookup_part(wire) == ""
        assert _is_nullish_conflict_key(wire) is True
    assert _pk_predicate_variants(Missing) == [Missing]
    assert _pk_predicate_variants(DF_MISSING_SENTINEL) == [DF_MISSING_SENTINEL]
    assert SQL_NULL_SENTINEL not in _pk_predicate_variants(SQL_NULL_SENTINEL)


def test_bool_pk_shares_dest_true_token():
    got = _pk_predicate_variants(True)
    assert True in got
    assert "true" in got
    assert "True" not in got
    assert _pk_lookup_part(True) == "true"
    assert _pk_lookup_part("true") == "true"
    assert _pk_lookup_part(True) == _pk_lookup_part("true")
    assert _pk_lookup_part(0) == "0"
