"""P1 (train review): fits_decimal and the decimal bind agree on NaN for
every PostgreSQL alias. fits_decimal said NaN was unfit on pg / timescaledb /
alloydb / supabase while the bind wrote it, so those rows were quarantined."""

from __future__ import annotations

from decimal import Decimal

import pytest

from connectors.sql_bind import _PG_NAN_ENGINES, coerce_decimal_wire
from connectors.writer_common import fits_decimal


@pytest.mark.parametrize("engine", sorted(_PG_NAN_ENGINES))
@pytest.mark.parametrize("value", ["NaN", "nan", float("nan"), Decimal("NaN")])
def test_pg_aliases_fit_and_bind_nan_alike(engine, value):
    assert fits_decimal(value, 10, 2, dest_db=engine) is True
    assert coerce_decimal_wire(value, ddl_type="NUMERIC(10,2)", engine=engine).is_nan()


@pytest.mark.parametrize("engine", ["mysql", "sqlserver", "oracle", "snowflake"])
def test_engines_without_numeric_nan_still_refuse(engine):
    assert fits_decimal("NaN", 10, 2, dest_db=engine) is False
    with pytest.raises(ValueError):
        coerce_decimal_wire("NaN", ddl_type="NUMERIC(10,2)", engine=engine)
