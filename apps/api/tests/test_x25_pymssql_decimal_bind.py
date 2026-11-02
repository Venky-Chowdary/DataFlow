"""X-25: pymssql cannot RPC-pack a Decimal at wide precision — a
DECIMAL(38,10) value aborted the whole MySQL->SQL Server copy with TDS
error 8023. Under pymssql the bind sends canonical decimal text (SQL
Server converts varchar->decimal implicitly); pyodbc binds natively."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_pg_sqlserver import bind_row_for_sqlserver  # noqa: E402


class _PymssqlCursor:
    __module__ = "pymssql"


class _PyodbcCursor:
    __module__ = "pyodbc"


WIDE = Decimal("9999999999999999999999999999.9999999999")


def test_pymssql_binds_decimal_as_canonical_text():
    row = (1, "alpha", WIDE, Decimal("-0.0000000001"), None)
    out = bind_row_for_sqlserver(_PymssqlCursor(), row)
    assert out[2] == "9999999999999999999999999999.9999999999"
    assert out[3] == "-0.0000000001"
    assert out[0] == 1 and out[1] == "alpha" and out[4] is None


def test_pymssql_never_emits_scientific_notation():
    out = bind_row_for_sqlserver(_PymssqlCursor(), (Decimal("1.50E-8"),))
    assert out == ("0.0000000150",)


def test_pyodbc_wide_decimal_binds_as_text():
    # pyodbc fast_executemany packs Decimal into a SQL_NUMERIC_STRUCT that
    # cannot hold >28 significant digits — same TDS 8023 RPC error (X-25).
    row = (1, WIDE)
    out = bind_row_for_sqlserver(_PyodbcCursor(), row)
    assert out[1] == "9999999999999999999999999999.9999999999"


def test_pyodbc_narrow_decimal_binds_as_text():
    # X-48b: pyodbc now binds every Decimal as canonical text, not only >28
    # digits — HY090 ("invalid precision") fired on narrow-but-odd scales
    # too, so the uniform rule removes a second native-bind path that could
    # still abort the copy. SQL Server converts varchar->decimal implicitly.
    narrow = Decimal("123456789012345.12345")
    out = bind_row_for_sqlserver(_PyodbcCursor(), (1, narrow))
    assert out[1] == "123456789012345.12345"


def test_pyodbc_inner_wrapped_cursor_detected():
    class _Wrap:
        def __init__(self, cur):
            self._cur = cur

    out = bind_row_for_sqlserver(_Wrap(_PyodbcCursor()), (WIDE,))
    assert out == ("9999999999999999999999999999.9999999999",)


def test_int_float_str_pass_through_on_pymssql():
    row = (7, 1.5, "x", b"\x00")
    assert bind_row_for_sqlserver(_PymssqlCursor(), row) == row
