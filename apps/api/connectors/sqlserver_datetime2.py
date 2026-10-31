"""SQL Server DATETIME2 / DATETIMEOFFSET bind keeps the declared fraction.

pyodbc sends a Python ``datetime`` as ODBC ``SQL_TIMESTAMP``, which keeps
three fractional digits. ``DATETIME2(6)`` then stores milliseconds and the
job completes with nothing rejected. ``DATETIME2`` whose precision is omitted
is SQL Server's default of 7.

Precision 3 and below stay a ``datetime``: the millisecond struct is exact,
and the write path already quarantines a fraction those digits cannot store.
Precision 7 sends the seventh digit a ``connectors.sqlserver_tick.Datetime2Tick``
carries (a DATETIME2(7) source read at full precision), else a zero.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
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
    Precision 7 takes the 100 ns digit a ``Datetime2Tick`` carries (else 0).
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
    from connectors.sqlserver_tick import fraction_text

    # DATETIME2(7) takes the seventh digit a Datetime2Tick carries (zero else).
    return clock.strftime("%Y-%m-%d %H:%M:%S.") + fraction_text(value, digits)


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
    # Execute reads the ODBC type after this conversion. Patch it here so a
    # writer that did not pass through engine setup still avoids SQL_TIMESTAMP.
    install_sqlserver_datetime2_bind()
    return sqlserver_datetime2_bind_text(value, precision=digits)


def datetimeoffset_declared_digits(sa_type: Any, logical: str) -> int | None:
    """Fractional digits of a DATETIMEOFFSET carrier, else None.

    Omitted precision is SQL Server's default of 7.
    """
    physical = str(getattr(getattr(sa_type, "__class__", None), "__name__", "") or "").upper()
    logical_u = (logical or "").upper()
    if physical != "DATETIMEOFFSET" and "DATETIMEOFFSET" not in logical_u:
        return None
    prec = getattr(sa_type, "precision", None) if physical == "DATETIMEOFFSET" else None
    if prec is None:
        match = re.search(r"DATETIMEOFFSET\s*\(\s*(\d+)\s*\)", logical_u)
        prec = int(match.group(1)) if match else 7
    try:
        return int(prec)
    except (TypeError, ValueError):
        return 7


def sqlserver_datetimeoffset_bind_text(value: Any, *, precision: int | None) -> Any:
    """``YYYY-MM-DD hh:mm:ss.fffffff +hh:mm`` for an aware ``datetime``, else unchanged.

    SQL Server reads that text as ``datetimeoffset(p)`` with the fraction and
    the offset it was sent with.
    """
    if not isinstance(value, datetime) or value.tzinfo is None:
        return value
    digits = 7 if precision is None else int(precision)
    micro = f"{value.microsecond:06d}"
    fraction = micro[:digits] if digits <= 6 else micro + "0" * (digits - 6)
    minutes = int((value.utcoffset() or timedelta(0)).total_seconds() // 60)
    hours, mins = divmod(abs(minutes), 60)
    return (
        value.strftime("%Y-%m-%d %H:%M:%S")
        + (f".{fraction}" if digits else "")
        + f" {'-' if minutes < 0 else '+'}{hours:02d}:{mins:02d}"
    )


def refuse_datetimeoffset_rounding(
    value: Any, *, logical: str, sa_type: Any, db_type: str
) -> None:
    """Quarantine a fraction a DATETIMEOFFSET(p<6) column would round.

    Raised before the write: a rounded row that only the read-back notices has
    already been committed.
    """
    if not isinstance(value, datetime) or not is_sqlserver_engine(db_type):
        return
    digits = datetimeoffset_declared_digits(sa_type, logical)
    if digits is None or digits >= 6:
        return
    if value.microsecond % (10 ** (6 - digits)):
        raise ValueError(
            f"SQL Server DATETIMEOFFSET({digits}) cannot store microsecond "
            f"{value.microsecond:06d} ({value.isoformat(sep='T')}) — column keeps "
            f"{digits} fractional digits. Quarantine; refuse round-after-commit."
        )


def sqlserver_datetime2_dbapi_type(precision: int | None, dbapi: Any) -> Any:
    """ODBC type for ``setinputsizes``.

    ``dbapi.DATETIME`` is ``SQL_TYPE_TIMESTAMP``. pyodbc then parses a
    fractional string back into a timestamp struct and keeps three digits,
    which undoes the text bind. Precision above 3 is sent as national
    character data so SQL Server converts the string into ``DATETIME2(p)``.
    """
    digits = 7 if precision is None else int(precision)
    if digits <= 3:
        return dbapi.DATETIME
    wchar = getattr(dbapi, "SQL_WVARCHAR", None)
    if wchar is None:
        return dbapi.DATETIME
    # ``YYYY-MM-DD HH:MM:SS.`` is 20 characters plus the declared fraction.
    return (wchar, 20 + digits, 0)


def install_sqlserver_datetime2_bind() -> None:
    """Patch ``DATETIME2`` so pyodbc does not cut the fraction.

    Idempotent. The processor and the ODBC type are read at execute time, so
    an engine created before this install still picks up the class methods.
    """
    global _INSTALLED
    if _INSTALLED:
        return
    from sqlalchemy.dialects.mssql.base import DATETIME2, DATETIMEOFFSET

    def bind_processor(self, dialect):  # noqa: ANN001
        precision = getattr(self, "precision", None)

        def process(value: Any) -> Any:
            return sqlserver_datetime2_bind_text(value, precision=precision)

        return process

    def get_dbapi_type(self, dbapi):  # noqa: ANN001
        return sqlserver_datetime2_dbapi_type(getattr(self, "precision", None), dbapi)

    DATETIME2.bind_processor = bind_processor  # type: ignore[method-assign]
    DATETIME2.get_dbapi_type = get_dbapi_type  # type: ignore[method-assign]

    inherited = DATETIMEOFFSET.bind_processor

    def offset_bind_processor(self, dialect):  # noqa: ANN001
        # pymssql renders a datetime as ``'… hh:mm:ss.mmm'``: three digits and
        # no offset (D183). pyodbc sends the offset struct whole, so it keeps
        # the processor it already had.
        if getattr(dialect, "driver", "") != "pymssql":
            return inherited(self, dialect)
        precision = getattr(self, "precision", None)

        def process(value: Any) -> Any:
            return sqlserver_datetimeoffset_bind_text(value, precision=precision)

        return process

    DATETIMEOFFSET.bind_processor = offset_bind_processor  # type: ignore[method-assign]
    _INSTALLED = True
