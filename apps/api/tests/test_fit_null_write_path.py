"""Typed fit/quarantine treat reader-null as holding, not as present text.

``fits_varchar`` / ``fits_decimal`` / ``fits_integer`` / ``fits_binary``
only skipped Python None. After extract emits SQL_NULL_SENTINEL, that
token is 14 ASCII characters — VARCHAR(3) quarantined a NULL, INTEGER
called it not-an-integer, and VARCHAR(20) could write the wire spelling.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.writer_common import (  # noqa: E402
    fit_skips_reader_null,
    fits_binary,
    fits_decimal,
    fits_integer,
    fits_varchar,
    integer_fit_failure,
    quarantine_unfit_decimals,
    quarantine_unfit_integers,
    quarantine_unfit_strings,
)
from services.type_system import boolean_value_fits  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_fit_skips_reader_null_not_empty_or_zero():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", Missing, DF_MISSING_SENTINEL):
        assert fit_skips_reader_null(wire) is True, wire
    assert fit_skips_reader_null("") is False
    assert fit_skips_reader_null("  ") is False
    assert fit_skips_reader_null(0) is False
    assert fit_skips_reader_null(False) is False


def test_fits_treat_reader_null_as_holding():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__"):
        assert fits_varchar(wire, 3, "VARCHAR(3)") is True, wire
        assert fits_decimal(wire, 10, 2) is True, wire
        assert fits_integer(wire, "INT") is True, wire
        assert fits_binary(wire, 1) is True, wire
        assert integer_fit_failure(wire, "INT") is None, wire
        assert boolean_value_fits(wire) is True, wire
    assert fits_varchar(0, 3, "VARCHAR(3)") is True
    assert fits_varchar("abcd", 3, "VARCHAR(3)") is False
    assert fits_integer(0, "INT") is True
    assert integer_fit_failure("", "INT") is not None
    assert boolean_value_fits("") is False
    assert boolean_value_fits(False) is True


def test_varchar_quarantine_keeps_sentinel_null():
    details: list[dict] = []
    out = quarantine_unfit_strings(
        [(SQL_NULL_SENTINEL,), ("abcd",)],
        ["name"],
        ["VARCHAR(3)"],
        details,
        policy="quarantine",
    )
    assert out == [(SQL_NULL_SENTINEL,)]
    assert details
    assert SQL_NULL_SENTINEL not in str(details[0].get("value") or "")


def test_decimal_and_integer_quarantine_keep_sentinel_null():
    dec_details: list[dict] = []
    dec_out = quarantine_unfit_decimals(
        [(SQL_NULL_SENTINEL,), ("999999999999",)],
        ["amt"],
        ["DECIMAL(5,2)"],
        dec_details,
        policy="quarantine",
    )
    assert dec_out == [(SQL_NULL_SENTINEL,)]
    assert dec_details

    int_details: list[dict] = []
    int_out = quarantine_unfit_integers(
        [(SQL_NULL_SENTINEL,), ("abc",)],
        ["n"],
        ["INT"],
        int_details,
        policy="quarantine",
    )
    assert int_out == [(SQL_NULL_SENTINEL,)]
    assert int_details
