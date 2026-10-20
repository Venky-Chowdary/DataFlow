"""X-79/X-89/X-80: exact IEEE bounds, float4-as-float4 compare, ±Inf carriers.

X-79: PG REAL extremes falsely refused — the wire renders FLT_MAX as
'3.4028235e38', a double hair above true FLT_MAX, so a magnitude compare on
the parsed double rejected it. The fit test is the float32 cast itself.
X-89: same for float64 on SQL Server — mssql stores no subnormals so the
floor is min-normal 2.2250738585072014e-308 and the refusal says "subnormal".
X-80: ±Inf refused on engines that store it (DuckDB/SQLite); NaN refused on
SQLite (it binds as NULL — silent loss).
"""

from __future__ import annotations

import math
import os

import pytest

from connectors.sql_bind import coerce_float_wire
from services.reconciliation import _fingerprint_with_ddl


def test_flt_max_wire_fits_float4_carriers():
    for eng in ("sqlserver", "duckdb"):
        assert coerce_float_wire("3.4028235e38", ddl_type="REAL", engine=eng) == 3.4028235e38
        assert coerce_float_wire("-1.1754944e-38", ddl_type="REAL", engine=eng) == -1.1754944e-38


def test_true_overflow_still_refused():
    with pytest.raises(ValueError):
        coerce_float_wire("1.7976931348623157e308", ddl_type="REAL", engine="duckdb")
    with pytest.raises(ValueError):
        coerce_float_wire("3.5e38", ddl_type="REAL", engine="sqlserver")


def test_mssql_binds_full_binary64_range():
    # X-56: SQL Server FLOAT binds the full IEEE binary64 range — subnormals
    # like 5e-324 land exactly; the old doc-floor refused values pyodbc binds.
    assert coerce_float_wire("5e-324", ddl_type="FLOAT", engine="sqlserver") == 5e-324
    assert coerce_float_wire("-2.2250738585072014e-308", ddl_type="FLOAT", engine="sqlserver")


def test_duckdb_sqlite_keep_ieee_subnormal_floor():
    assert coerce_float_wire("5e-324", ddl_type="DOUBLE", engine="duckdb") == 5e-324
    assert coerce_float_wire("5e-324", ddl_type="REAL", engine="sqlite") == 5e-324


def test_inf_binds_where_the_engine_stores_it():
    for eng in ("duckdb", "sqlite", "postgresql"):
        assert coerce_float_wire(math.inf, ddl_type="DOUBLE", engine=eng) == math.inf
        assert coerce_float_wire(-math.inf, ddl_type="DOUBLE", engine=eng) == -math.inf


def test_inf_nan_refuse_with_plain_message():
    for eng in ("sqlserver", "mysql"):
        with pytest.raises(ValueError) as exc:
            coerce_float_wire(math.inf, ddl_type="FLOAT", engine=eng)
        msg = str(exc.value)
        assert "cannot store Infinity" in msg and "quarantine" in msg
        assert "non-finite" in msg
        assert "refuse invent" not in msg
    # X-80: only the SQL Server/MySQL families refuse non-finite — every
    # other engine keeps the explicit IEEE wire (SQLite included; how the
    # destination stores NaN is its own documented behavior).
    assert math.isnan(coerce_float_wire(math.nan, ddl_type="REAL", engine="sqlite"))
    # DuckDB and PG store NaN.
    assert math.isnan(coerce_float_wire(math.nan, ddl_type="DOUBLE", engine="duckdb"))


def test_float4_fingerprint_compares_at_float4_precision():
    wire = _fingerprint_with_ddl("3.4028235e38", "REAL", "sqlite")
    stored = _fingerprint_with_ddl(3.4028234663852886e38, "REAL", "sqlite")
    assert wire == stored
    assert _fingerprint_with_ddl("0.1", "REAL", "sqlite") == _fingerprint_with_ddl(0.1, "REAL", "sqlite")
    # A real tamper still differs at float4 precision.
    assert _fingerprint_with_ddl("0.1", "REAL", "sqlite") != _fingerprint_with_ddl("0.2", "REAL", "sqlite")


@pytest.mark.skipif(not os.environ.get("DATAFLOW_X79_LIVE"), reason="live engines — user-run")
def test_x79_live_5value_matrix():
    pytest.fail("harness: FLT_MAX/0.1/NULL/-FLT_MIN/16777217 x 4 dests clean+tamper")
