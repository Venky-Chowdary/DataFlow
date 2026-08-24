"""Signed/unsigned integer write-path fit + quarantine."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    fits_integer,
    quarantine_unfit_integers,
)
from services.type_system import integer_storage_bounds  # noqa: E402


def test_integer_storage_bounds_signed_and_unsigned():
    assert integer_storage_bounds("INTEGER") == (-(2**31), 2**31 - 1)
    assert integer_storage_bounds("SMALLINT") == (-(2**15), 2**15 - 1)
    assert integer_storage_bounds("BIGINT") == (-(2**63), 2**63 - 1)
    assert integer_storage_bounds("INT UNSIGNED") == (0, 2**32 - 1)
    assert integer_storage_bounds("SMALLINT UNSIGNED") == (0, 2**16 - 1)
    # BIGINT UNSIGNED is DECIMAL carrier — no integer bounds.
    assert integer_storage_bounds("BIGINT UNSIGNED") is None


def test_fits_integer_overflow_cases():
    assert fits_integer(2**31 - 1, "INTEGER") is True
    assert fits_integer(2**31, "INTEGER") is False
    assert fits_integer(-(2**31), "INTEGER") is True
    assert fits_integer(-(2**31) - 1, "INTEGER") is False
    assert fits_integer(2**32 - 1, "INT UNSIGNED") is True
    assert fits_integer(-1, "INT UNSIGNED") is False
    assert fits_integer("2147483648", "INTEGER") is False


def test_quarantine_holds_out_overflow_integer():
    rows = [(2**31, "ok"), (1, "fine")]
    details: list[dict] = []
    out = quarantine_unfit_integers(
        rows,
        ["qty", "label"],
        ["INTEGER", "VARCHAR"],
        details,
        policy="quarantine",
        dialect_label="INTEGER",
    )
    assert out == [(1, "fine")]
    assert details and "does not fit" in details[0]["reason"]


def test_coerce_null_nulls_overflow_cell():
    rows = [(2**31, "keep")]
    details: list[dict] = []
    out = quarantine_unfit_integers(
        rows,
        ["qty", "label"],
        ["INTEGER", "VARCHAR"],
        details,
        policy="coerce_null",
    )
    from services.value_serializer import DF_MISSING_SENTINEL

    assert out == [(DF_MISSING_SENTINEL, "keep")]
    assert details


def test_decimal_carrier_skips_integer_quarantine():
    # BIGINT UNSIGNED → DECIMAL — integer quarantine must not touch these cells.
    rows = [(2**64 - 1,)]
    details: list[dict] = []
    out = quarantine_unfit_integers(
        rows,
        ["big"],
        ["BIGINT UNSIGNED"],
        details,
        policy="quarantine",
    )
    assert out == rows
    assert details == []
