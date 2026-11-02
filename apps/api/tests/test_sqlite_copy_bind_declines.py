"""COPY binds only cells that are already the declared carrier.

The SQLite fast path binds CSV text straight into the destination, so a cell
the row path would have parsed, rejected or quarantined must make COPY decline
instead of being forced (``int('false')`` crashed the job) or stored verbatim
(a grouped ``1,234`` landed as text in a DECIMAL column, so the number the
operator declared never reached the destination).
"""

from __future__ import annotations

import pytest

from services.copy_fast_path import FastPathUnavailable
from services.copy_sqlite_common import sqlite_bind_from_text


def test_integer_carrier_binds_an_integer_and_keeps_null_null():
    bind = sqlite_bind_from_text("INTEGER")
    assert bind("42") == 42
    assert bind(None) is None


@pytest.mark.parametrize("cell", ["false", "1,234", "", "n/a"])
def test_integer_carrier_declines_a_cell_the_row_path_owns(cell: str):
    bind = sqlite_bind_from_text("INTEGER")
    with pytest.raises(FastPathUnavailable):
        bind(cell)


def test_real_carrier_declines_a_cell_the_row_path_owns():
    bind = sqlite_bind_from_text("REAL")
    assert bind("1.5") == 1.5
    with pytest.raises(FastPathUnavailable):
        bind("1.234,50")


def test_declared_decimal_keeps_the_exact_digits_it_was_given():
    bind = sqlite_bind_from_text("TEXT", "decimal")
    assert bind("1234.50") == "1234.50"
    assert bind("-0.001") == "-0.001"
    assert bind(None) is None


@pytest.mark.parametrize("cell", ["1,234", "$1,000.00", "€2.000,50", "1 234,5"])
def test_declared_decimal_declines_grouped_or_marked_money(cell: str):
    bind = sqlite_bind_from_text("TEXT", "decimal")
    with pytest.raises(FastPathUnavailable):
        bind(cell)


def test_undeclared_text_column_still_carries_free_text():
    bind = sqlite_bind_from_text("TEXT")
    assert bind("1,234") == "1,234"
    assert bind("anything at all") == "anything at all"
