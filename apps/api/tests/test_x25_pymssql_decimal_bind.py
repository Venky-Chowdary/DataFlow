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


def test_pyodbc_row_untouched():
    row = (1, WIDE)
    out = bind_row_for_sqlserver(_PyodbcCursor(), row)
    assert out[1] is WIDE  # native Decimal bind — no conversion


def test_int_float_str_pass_through_on_pymssql():
    row = (7, 1.5, "x", b"\x00")
    assert bind_row_for_sqlserver(_PymssqlCursor(), row) == row
