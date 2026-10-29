"""D183 — a tz-aware value bound for SQL Server DATETIMEOFFSET must keep every digit.

The production image has unixodbc but no Microsoft ODBC driver, so the engine
runs SQL Server on pymssql (FreeTDS). pymssql renders a Python ``datetime``
parameter as ``'YYYY-MM-DD hh:mm:ss.mmm'``: three fractional digits and no
offset. PostgreSQL ``TIMESTAMPTZ(6)`` -> ``DATETIMEOFFSET`` therefore landed
milliseconds, and a non-UTC offset landed as UTC. The DATETIME2 text carrier
already existed (``sqlserver_datetime2``); DATETIMEOFFSET was bound raw.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.dialects import mssql
from sqlalchemy.dialects.mssql import pymssql as sa_pymssql, pyodbc as sa_pyodbc

from connectors.generic_sql import _to_sa_value
from connectors.sqlserver_datetime2 import install_sqlserver_datetime2_bind

_IST = timezone(timedelta(hours=5, minutes=30))
_VALUE = datetime(2025, 1, 2, 3, 4, 5, 123456, tzinfo=_IST)


def _processor(dialect, sa_type):
    install_sqlserver_datetime2_bind()
    return sa_type.dialect_impl(dialect).bind_processor(dialect)


def test_pymssql_bind_is_text_with_the_full_fraction_and_offset():
    process = _processor(sa_pymssql.dialect(), mssql.DATETIMEOFFSET(7))
    assert process(_VALUE) == "2025-01-02 03:04:05.1234560 +05:30"
    assert process(None) is None


def test_pymssql_bind_survives_pymssql_parameter_quoting():
    quote = pytest.importorskip("pymssql._mssql").quote_simple_value
    bound = _processor(sa_pymssql.dialect(), mssql.DATETIMEOFFSET(7))(_VALUE)
    assert "03:04:05.1234560 +05:30" in quote(bound).decode()


def test_pymssql_bind_default_precision_is_seven_digits():
    process = _processor(sa_pymssql.dialect(), mssql.DATETIMEOFFSET())
    assert process(_VALUE) == "2025-01-02 03:04:05.1234560 +05:30"


def test_pymssql_bind_negative_offset_and_declared_precision():
    value = datetime(2025, 1, 2, 3, 4, 5, 123000, tzinfo=timezone(timedelta(hours=-3, minutes=-30)))
    assert _processor(sa_pymssql.dialect(), mssql.DATETIMEOFFSET(3))(value) == (
        "2025-01-02 03:04:05.123 -03:30"
    )


def test_pyodbc_keeps_the_processor_sqlalchemy_gives_it():
    # SQLAlchemy's ODBC processor already renders the fraction and the offset.
    process = _processor(sa_pyodbc.dialect(), mssql.DATETIMEOFFSET(7))
    assert process(_VALUE) == "2025-01-02 03:04:05.123456+05:30"


def test_writer_refuses_a_fraction_a_datetimeoffset_below_six_digits_would_round():
    def bind(value, digits):
        return _to_sa_value(value, "timestamptz", mssql.DATETIMEOFFSET(digits), "mssql", "mssql")

    with pytest.raises(ValueError, match=r"DATETIMEOFFSET\(3\) cannot store microsecond 123456"):
        bind(_VALUE, 3)
    assert bind(datetime(2025, 1, 2, 3, 4, 5, 123000, tzinfo=_IST), 3).utcoffset() == _VALUE.utcoffset()
    assert bind(_VALUE, 7).microsecond == 123456


def test_other_engines_are_not_refused():
    out = _to_sa_value(_VALUE, "timestamptz", None, "postgresql", "postgresql")
    assert out.utcoffset() is not None
