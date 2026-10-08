"""SQL Server DATETIME2 bind keeps the declared fraction.

pyodbc sends a Python ``datetime`` as ODBC ``SQL_TIMESTAMP``, which keeps
three fractional digits. ``DATETIME2(6)`` then stores milliseconds and the
job completes with nothing rejected. ``DATETIME2`` whose precision is omitted
is SQL Server's default of 7.

Precision 3 and below stay a ``datetime``: the millisecond struct is exact,
and the write path already quarantines a fraction those digits cannot store.
Precision above 6 can only add trailing zeros — Python has six digits, so
this is not a claim of 100-nanosecond fidelity.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

_INSTALLED = False

_SQLSERVER_ENGINES = frozenset({
    "mssql",
    "sqlserver",
    "sql_server",
    "microsoft_sql_server",
    "azure_sql",
    "azure_sql_database",
    "synapse",
    "azure_synapse",
    "synapse_analytics",
    "amazon_rds_sql_server",
    "google_cloud_sql_sql_server",
    "azure_synapse_dedicated",
    "azure_synapse_serverless",
})


def is_sqlserver_engine(db_type: str) -> bool:
    engine = (db_type or "").strip().lower()
    return engine in _SQLSERVER_ENGINES or "mssql" in engine


def datetime2_declared_digits(sa_type: Any, logical: str) -> int | None:
    """Fractional digits of a DATETIME2 carrier, else None.

    Classic ``DATETIME`` and ``DATETIMEOFFSET`` are not this carrier.
    Omitted precision is SQL Server's default of 7.
    """
    type_name = ""
    if sa_type is not None:
        type_name = str(getattr(getattr(sa_type, "__class__", None), "__name__", "") or "")
    logical_u = (logical or "").upper()
    physical = type_name.upper()
    if physical == "DATETIMEOFFSET" or (
        "DATETIMEOFFSET" in logical_u and physical != "DATETIME"
    ):
        return None
    if physical != "DATETIME2" and not (
        "DATETIME2" in logical_u and physical != "DATETIME"
    ):
        return None
    prec = getattr(sa_type, "precision", None) if physical == "DATETIME2" else None
    if prec is None and "DATETIME2" in logical_u:
        match = re.search(r"DATETIME2\s*\(\s*(\d+)\s*\)", logical_u)
        prec = int(match.group(1)) if match else 7
    try:
        return 7 if prec is None else int(prec)
    except (TypeError, ValueError):
        return 7


def sqlserver_datetime2_bind_text(value: Any, *, precision: int | None) -> Any:
    """Bind text for DATETIME2 when the ODBC timestamp struct would truncate.

    ``None`` and ``str`` pass through. Precision of 3 or less returns the
    datetime object. Precision 4 and 5 keep that many fractional digits.
    Precision above 6 appends zeros for the digits Python cannot supply.
    """
    if value is None or isinstance(value, str):
        return value
    digits = 7 if precision is None else int(precision)
    if isinstance(value, datetime):
        clock = value
    elif isinstance(value, date):
        clock = datetime(value.year, value.month, value.day)
    else:
        return value
    if digits <= 3:
        return clock if not isinstance(value, datetime) else value
    if clock.tzinfo is not None:
        clock = clock.replace(tzinfo=None)
    text = clock.strftime("%Y-%m-%d %H:%M:%S.%f")
    if digits < 6:
        text = text[: -(6 - digits)]
    elif digits > 6:
        text = text + ("0" * (digits - 6))
    return text


def bind_sqlserver_datetime2(
    value: Any,
    *,
    logical: str,
    sa_type: Any,
    db_type: str,
) -> Any:
    """Return a fractional string when this SQL Server bind is DATETIME2(>3)."""
    if not is_sqlserver_engine(db_type):
        return value
    digits = datetime2_declared_digits(sa_type, logical)
    if digits is None or digits <= 3:
        return value
    return sqlserver_datetime2_bind_text(value, precision=digits)


def install_sqlserver_datetime2_bind() -> None:
    """Patch ``DATETIME2.bind_processor`` so pyodbc does not cut the fraction.

    Idempotent. The processor runs at execute time, so an engine created
    before this install still picks up the class method.
    """
    global _INSTALLED
    if _INSTALLED:
        return
    from sqlalchemy.dialects.mssql.base import DATETIME2

    def bind_processor(self, dialect):  # noqa: ANN001
        precision = getattr(self, "precision", None)

        def process(value: Any) -> Any:
            return sqlserver_datetime2_bind_text(value, precision=precision)

        return process

    DATETIME2.bind_processor = bind_processor  # type: ignore[method-assign]
    _INSTALLED = True
