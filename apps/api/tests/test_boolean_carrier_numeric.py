"""Boolean sources landing on an engine's numeric boolean carrier.

Oracle (pre-23ai), DB2 and most warehouses have no BOOLEAN, so a SQL Server
``BIT`` / PostgreSQL ``BOOLEAN`` column lands on ``NUMBER(1)``. The live matrix
quarantined every such row with "decimal does not fit Oracle DECIMAL(1,0)" —
total, lossless conversions must not be held out, while genuine numeric columns
must keep refusing boolean wire.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from connectors.writer_common import fits_decimal
from services.transform_engine import boolean_carrier_numeric_value


class TestBooleanCarrierDetection:
    @pytest.mark.parametrize(
        ("value", "expected"), [("true", 1), ("TRUE", 1), ("t", 1), ("false", 0), ("f", 0)]
    )
    def test_canonical_wire_maps_to_numeric(self, value: str, expected: int) -> None:
        assert boolean_carrier_numeric_value(value, 1, 0) == expected

    def test_python_bool_maps_to_numeric(self) -> None:
        assert boolean_carrier_numeric_value(True, 1, 0) == 1
        assert boolean_carrier_numeric_value(False, 1, 0) == 0

    @pytest.mark.parametrize("value", ["yes", "on", "y", "enabled", "-1"])
    def test_informal_truth_is_never_invented(self, value: str) -> None:
        assert boolean_carrier_numeric_value(value, 1, 0) is None

    @pytest.mark.parametrize(
        ("precision", "scale"), [(12, 2), (2, 0), (38, 0), (1, 1), (None, None)]
    )
    def test_wider_numeric_columns_refuse_boolean_wire(
        self, precision: int | None, scale: int | None
    ) -> None:
        # A real money/quantity column is not a boolean carrier: refusing here
        # keeps "true" quarantined instead of silently becoming 1.
        assert boolean_carrier_numeric_value("true", precision, scale) is None


class TestDecimalFitGate:
    def test_boolean_wire_fits_the_boolean_carrier(self) -> None:
        assert fits_decimal("true", 1, 0, dest_db="oracle") is True
        assert fits_decimal("false", 1, 0, dest_db="oracle") is True

    def test_boolean_wire_still_fails_a_real_decimal_column(self) -> None:
        assert fits_decimal("true", 12, 2, dest_db="oracle") is False

    def test_numeric_overflow_is_unchanged(self) -> None:
        assert fits_decimal(Decimal("42"), 1, 0, dest_db="oracle") is False
        assert fits_decimal(Decimal("1"), 1, 0, dest_db="oracle") is True

    def test_informal_boolean_still_quarantines(self) -> None:
        assert fits_decimal("yes", 1, 0, dest_db="oracle") is False


class TestBindPath:
    def test_bind_converts_boolean_wire_on_the_carrier(self) -> None:
        from connectors.sql_bind import coerce_decimal_wire

        assert coerce_decimal_wire("true", ddl_type="NUMBER(1)", engine="oracle") == Decimal(1)
        assert coerce_decimal_wire("false", ddl_type="NUMBER(1)", engine="oracle") == Decimal(0)
        assert coerce_decimal_wire(True, ddl_type="NUMBER(1,0)", engine="oracle") == Decimal(1)

    def test_bind_still_refuses_bool_into_a_money_column(self) -> None:
        from connectors.sql_bind import coerce_decimal_wire

        with pytest.raises(ValueError, match="refuse"):
            coerce_decimal_wire(True, ddl_type="NUMBER(12,2)", engine="oracle")
