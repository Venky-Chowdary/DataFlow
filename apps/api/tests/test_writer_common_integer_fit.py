"""Signed/unsigned integer write-path fit + quarantine."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    fits_integer,
    integer_fit_failure,
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


def test_a_fractional_value_never_fits_an_integer_carrier():
    """The 1M-row MySQL abort: ``fits_integer`` said 22.433332 fits ``INT``.

    It truncated to 22, found it in range, and let Validate promise a write the
    writer refuses at row 1 with "Invalid integer". Fit and the write must agree.
    """
    for value in ("22.433332", "22.05", "21.833334", "-3.5", 22.5):
        assert fits_integer(value, "INT", dest_db="mysql") is False, value
        reason = integer_fit_failure(value, "INT", dest_db="mysql")
        assert reason and "fractional" in reason

    # Integral values — including scientific notation and integral decimals —
    # are what the writer's _parse_integer accepts, so they still fit.
    for value in ("22", "22.0", "1e3", "2.5e1", 22, True):
        assert fits_integer(value, "INT", dest_db="mysql") is True, value


def test_fit_and_writer_transform_agree_on_every_numeric_form():
    """Same values through both rules — no third answer is possible."""
    from services.transform_engine import apply_transform

    for value in (
        "22.433332",
        "22.05",
        "21.833334",
        "-3.5",
        "22",
        "22.0",
        "1e3",
        "2.5e1",
        "0",
        "-7",
    ):
        parsed, error = apply_transform(value, "integer")
        writer_accepts = error is None and parsed is not None
        assert fits_integer(value, "BIGINT") is writer_accepts, value


def test_quarantine_holds_out_a_fractional_cell_with_the_reason():
    rows = [("22.433332", "ok"), ("22", "fine")]
    details: list[dict] = []
    out = quarantine_unfit_integers(
        rows,
        ["arr_time", "label"],
        ["INT", "VARCHAR"],
        details,
        policy="quarantine",
        dialect_label="MySQL INTEGER",
        dest_db="mysql",
    )
    assert out == [("22", "fine")]
    assert details and "fractional" in details[0]["reason"]


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
