"""Exact decimals must survive inference and the Gate-8 checksum unchanged.

Three independent layers used to round wide decimals, each for a defensible
reason, and together they turned a clean CSV → PostgreSQL load into corrupted
money:

1. sample inference read scientific notation as proof of IEEE float,
2. the observation clamp truncated observed scale to fit a 38-digit cap,
3. the checksum canonicaliser pushed long mantissas through ``float``.

Excel/IEEE residue must still collapse, so every case below is paired: an exact
decimal that has to survive, and a residue value that still has to be folded.
"""

from __future__ import annotations

from decimal import Decimal

from services.decimal_observe import observe_numeric_samples
from services.reconciliation import _canonicalize_number
from services.type_system import ddl_type

# Values a finance export produces: wide, small, and scientific — all exact.
EXACT_DECIMALS = [
    "12345678901234567890.1234567890",
    "0.00000000000000000001",
    "-999999999999999999.999999",
    "1.23E-10",
    "9.87E+20",
]

# Values a spreadsheet produces when a binary double is written out as text.
IEEE_RESIDUE = [
    "111.89999999999999",
    "42.100000000000001",
    "7.199999999999999",
]


def test_scientific_notation_is_a_spelling_not_a_storage_class():
    """``9.87E+20`` is an exact decimal; only the mantissa may argue otherwise."""
    obs = observe_numeric_samples(["1.23E-10", "9.87E+20"])
    assert obs["kind"] == "fixed_decimal"
    assert obs["carrier"].startswith("DECIMAL(")


def test_long_scale_with_few_significant_digits_is_exact():
    """``0.00000000000000000001`` has scale 20 and one significant digit."""
    obs = observe_numeric_samples(["0.00000000000000000001"])
    assert obs["kind"] == "fixed_decimal"


def test_ieee_residue_still_infers_float():
    """The mantissa a double actually produces must keep choosing FLOAT."""
    assert observe_numeric_samples(IEEE_RESIDUE)["kind"] == "ieee_float"
    # One residue cell is enough to make the whole column approximate.
    assert observe_numeric_samples(["1.50", "2.25", IEEE_RESIDUE[2]])["kind"] == "ieee_float"


def test_observed_scale_is_never_truncated_to_fit_the_cap():
    """The cap may reclaim added head-room, never a digit the samples used."""
    obs = observe_numeric_samples(EXACT_DECIMALS)
    assert obs["kind"] == "fixed_decimal"
    # 20 integer digits and 20 fractional digits are both present in the samples.
    assert obs["max_int_digits"] >= 20
    assert obs["scale"] >= 20


def test_wide_decimal_reaches_an_exact_or_lossless_destination_type():
    """Engines that can hold the value do; the rest degrade losslessly, not to FLOAT."""
    carrier = observe_numeric_samples(EXACT_DECIMALS)["carrier"]
    assert "NUMERIC" in ddl_type("postgresql", carrier).upper()
    assert "BIGNUMERIC" in ddl_type("bigquery", carrier).upper()
    for engine in ("mysql", "sqlite", "snowflake", "oracle", "sqlserver"):
        emitted = ddl_type(engine, carrier).upper()
        assert not any(f in emitted for f in ("FLOAT", "DOUBLE", "REAL")), (engine, emitted)


def test_checksum_agrees_across_equivalent_spellings():
    """Gate-8 must read ``1.23E-10`` and ``1.2300000000E-10`` as one value."""
    pairs = [
        ("1.23E-10", Decimal("1.2300000000E-10")),
        ("9.87E+20", Decimal("987000000000000000000.00000000000000000000")),
        ("0.00000000000000000001", Decimal("1E-20")),
        (
            "12345678901234567890.1234567890",
            Decimal("12345678901234567890.12345678900000000000"),
        ),
        ("-999999999999999999.999999", Decimal("-999999999999999999.99999900000000000000")),
    ]
    for source, target in pairs:
        assert _canonicalize_number(source) == _canonicalize_number(target), source


def test_checksum_still_folds_excel_residue():
    """Collapsing binary residue is what stops false Gate-8 failures."""
    assert _canonicalize_number("106.60000000000001") == _canonicalize_number(Decimal("106.6"))
    assert _canonicalize_number("9.5") == _canonicalize_number(Decimal("9.5000000000"))


def test_checksum_does_not_collide_beyond_the_context_precision():
    """Different values may never share a digest.

    ``Decimal.normalize`` honours the ambient 28-digit context, so these two
    30-significant-digit values used to canonicalise identically — Gate-8 would
    have called corrupted data verified.
    """
    a = _canonicalize_number("12345678901234567890.1234567890")
    b = _canonicalize_number("12345678901234567890.1234567899")
    assert a != b


def test_checksum_preserves_every_digit_of_an_exact_decimal():
    """No rounding at all for a value no double could hold."""
    assert _canonicalize_number("-999999999999999999.999999") == "-999999999999999999.999999"
    assert (
        _canonicalize_number("12345678901234567890.1234567890")
        == "12345678901234567890.123456789"
    )
