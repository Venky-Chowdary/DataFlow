"""X-134 follow-on: float32 cells fingerprint at binary32 granularity.

A single-precision carrier keeps 24 significand bits. The same stored value
reads back as ``0.33333334`` from one driver (PostgreSQL ``real`` text) and
``0.3333333432674408`` (the float64 widening of the same binary32) from
another (MySQL ``FLOAT``). Gate-8 must compare the float32 spelling on both
sides: the double fold (.15g) reads a faithful float4 round-trip as
corruption, and folding a double carrier would collapse two genuinely
different doubles onto one digest.
"""

from __future__ import annotations

from decimal import Decimal

from services.reconciliation import (
    fingerprint_for_reconcile,
    normalize_cell,
)

WIDENED = 0.3333333432674408  # float64 widening of float32(0.33333334)
SPELLING = "0.33333334"  # canonical binary32 spelling


class TestFloat32Fold:
    def test_mysql_float_driver_widening_matches_source_spelling(self):
        src = normalize_cell(SPELLING, ddl_type="float", engine="mysql")
        dst = normalize_cell(WIDENED, ddl_type="float", engine="mysql")
        assert src == dst == SPELLING

    def test_pg_real_driver_widening_matches_source_spelling(self):
        src = normalize_cell(SPELLING, ddl_type="real", engine="postgresql")
        dst = normalize_cell(WIDENED, ddl_type="real", engine="postgresql")
        assert src == dst == SPELLING

    def test_float4_named_carrier(self):
        assert (
            normalize_cell(WIDENED, ddl_type="float4", engine="postgresql")
            == SPELLING
        )

    def test_decimal_and_int_fold_through_binary32(self):
        assert (
            normalize_cell(Decimal("5"), ddl_type="float4", engine="postgresql")
            == normalize_cell(5, ddl_type="float4", engine="postgresql")
            == normalize_cell(5.0, ddl_type="float4", engine="postgresql")
            == "5"
        )

    def test_fingerprint_for_reconcile_symmetric(self):
        assert fingerprint_for_reconcile(
            SPELLING, ddl_type="float", engine="mysql"
        ) == fingerprint_for_reconcile(WIDENED, ddl_type="float", engine="mysql")


class TestDoubleCarrierIsNotFolded:
    """The fold must never equate two different doubles on a float8 carrier."""

    def test_float8_keeps_distinct_values_distinct(self):
        src = normalize_cell(SPELLING, ddl_type="float8", engine="postgresql")
        dst = normalize_cell(WIDENED, ddl_type="float8", engine="postgresql")
        assert src != dst

    def test_pg_bare_float_is_double_not_folded(self):
        src = normalize_cell(SPELLING, ddl_type="float", engine="postgresql")
        dst = normalize_cell(WIDENED, ddl_type="float", engine="postgresql")
        assert src != dst

    def test_sqlite_real_is_double_not_folded(self):
        src = normalize_cell(SPELLING, ddl_type="real", engine="sqlite")
        dst = normalize_cell(WIDENED, ddl_type="real", engine="sqlite")
        assert src != dst


class TestEdgeValues:
    def test_negative_zero_folds_to_zero(self):
        assert normalize_cell(-0.0, ddl_type="float4", engine="postgresql") == "0"
        assert normalize_cell(0.0, ddl_type="float4", engine="postgresql") == "0"

    def test_non_numeric_text_stays_text(self):
        assert (
            normalize_cell("not-a-number", ddl_type="float", engine="mysql")
            == "not-a-number"
        )

    def test_infinity(self):
        assert (
            normalize_cell(float("inf"), ddl_type="float4", engine="postgresql")
            == "Infinity"
        )
        assert (
            normalize_cell(float("-inf"), ddl_type="float4", engine="postgresql")
            == "-Infinity"
        )

    def test_int_beyond_f32_mantissa_folds_to_carrier_value(self):
        # 2**24 + 1 is not representable in binary32 — the carrier stores
        # 16777216, so both cells compare at the carrier value.
        assert (
            normalize_cell(16777217, ddl_type="float4", engine="postgresql")
            == normalize_cell(16777216.0, ddl_type="float4", engine="postgresql")
        )
