"""SQL Server ``DATETIME2(7)``'s seventh fractional digit (one 100 ns tick).

pyodbc builds a Python ``datetime`` from the ODBC timestamp struct, whose
fraction is in nanoseconds, and keeps six digits: ``03:04:05.1234567`` reads as
``03:04:05.123456``. The source digest read the column the same way, so a copy
that dropped the digit verified.

``install_tick_converter`` registers a pyodbc output converter that decodes the
struct itself. A value whose seventh digit is not zero comes back as
:class:`Datetime2Tick`: a ``datetime`` (every consumer still works, at six
digits) that also carries the digit in ``tick``. Only consumers that can hold
it ask for it, through :func:`temporal_iso_text` / :func:`tick_of`:

* text carriers (the reader's wire, SQLite TEXT, files) spell seven digits;
* a SQL Server ``DATETIME2(7)`` bind sends seven digits;
* the Gate-8 fingerprint compares seven digits.

A destination whose carrier keeps six digits or fewer receives the plain
``datetime``, exactly as before, and the reconciliation report names the column
as a declared narrowing (``services.carrier_instant``).
"""

from __future__ import annotations

import re
import struct
from datetime import datetime, timedelta
from typing import Any

#: SQL Server ``datetime``/``smalldatetime`` day zero.
_SQL_EPOCH = datetime(1900, 1, 1)  # noqa: DTZ001 - SQL Server wall-clock epoch
#: ODBC ``SQL_TYPE_TIMESTAMP``.
SQL_TYPE_TIMESTAMP = 93

#: The fraction of an ISO-ish timestamp text, and what follows it.
_FRACTION_RE = re.compile(r"(\d{2}:\d{2}:\d{2})\.(\d+)")


class Datetime2Tick(datetime):
    """A ``datetime`` that also carries the 100 ns digit (``tick``, 1-9)."""

    tick: int = 0

    def __reduce__(self):  # noqa: ANN204 - pickle / copy keep the digit
        return (_rebuild, (datetime(
            self.year, self.month, self.day, self.hour, self.minute,
            self.second, self.microsecond, self.tzinfo, fold=self.fold,
        ), self.tick))


def _rebuild(base: datetime, tick: int) -> datetime:
    return with_tick(base, tick)


def with_tick(value: datetime, tick: int) -> datetime:
    """``value`` carrying the seventh digit ``tick``; plain when ``tick`` is 0."""
    tick = int(tick or 0)
    if not tick or not isinstance(value, datetime):
        return value
    out = Datetime2Tick(
        value.year, value.month, value.day, value.hour, value.minute,
        value.second, value.microsecond, value.tzinfo, fold=value.fold,
    )
    out.tick = tick
    return out


def tick_of(value: Any) -> int:
    """The seventh fractional digit a value carries (0 when none)."""
    if isinstance(value, Datetime2Tick):
        return int(value.tick or 0)
    return 0


def drop_tick(value: Any) -> Any:
    """The six-digit ``datetime`` (what a microsecond carrier stores)."""
    if isinstance(value, Datetime2Tick):
        return datetime(
            value.year, value.month, value.day, value.hour, value.minute,
            value.second, value.microsecond, value.tzinfo, fold=value.fold,
        )
    return value


def temporal_iso_text(value: datetime, sep: str = "T") -> str:
    """``value.isoformat(sep)`` with the seventh digit when it carries one."""
    tick = tick_of(value)
    if not tick:
        return value.isoformat(sep=sep)
    naive = value.replace(tzinfo=None)
    text = datetime.isoformat(naive, sep=sep, timespec="microseconds") + str(tick)
    offset = value.utcoffset()
    if offset is not None:
        text += datetime(2000, 1, 1, tzinfo=value.tzinfo).isoformat()[19:]
    return text


def tick_from_text(text: Any) -> int:
    """The seventh fractional digit spelled in a timestamp text (0 when none)."""
    if not isinstance(text, str):
        return 0
    match = _FRACTION_RE.search(text)
    if match is None or len(match.group(2)) < 7:
        return 0
    return int(match.group(2)[6])


def decode_sql_timestamp(raw: bytes | None) -> datetime | None:
    """pyodbc output converter for ``SQL_TYPE_TIMESTAMP``.

    The Microsoft ODBC driver hands back three layouts:

    * 16 bytes, ``DATETIME2``: the ODBC ``TIMESTAMP_STRUCT`` (year, month, day,
      hour, minute, second as shorts, fraction as nanoseconds);
    * 8 bytes, ``DATETIME``: days since 1900-01-01 and 1/300-second ticks; the
      driver rounds those to milliseconds, as done here;
    * 4 bytes, ``SMALLDATETIME``: days since 1900-01-01 and minutes.
    """
    if raw is None:
        return None
    size = len(raw)
    if size == 16:
        year, month, day, hour, minute, second, fraction = struct.unpack("<6hI", raw[:16])
        micro, rest = divmod(int(fraction), 1000)
        value = datetime(year, month, day, hour, minute, second, micro)  # noqa: DTZ001
        return with_tick(value, rest // 100)
    if size == 8:
        days, ticks = struct.unpack("<iI", raw)
        millis = (ticks * 10 + 1) // 3
        return _SQL_EPOCH + timedelta(days=days, milliseconds=millis)
    if size == 4:
        days, minutes = struct.unpack("<HH", raw)
        return _SQL_EPOCH + timedelta(days=days, minutes=minutes)
    raise ValueError(f"unexpected SQL Server timestamp layout ({size} bytes)")


def install_tick_converter(dbapi_conn: Any) -> None:
    """Read ``DATETIME2`` at full precision on this pyodbc connection."""
    add = getattr(dbapi_conn, "add_output_converter", None)
    if not callable(add):
        return
    add(SQL_TYPE_TIMESTAMP, decode_sql_timestamp)


def fraction_text(value: Any, digits: int) -> str:
    """The fractional second of ``value`` at ``digits`` digits (up to 7)."""
    micro = getattr(value, "microsecond", 0) or 0
    text = f"{micro:06d}" + str(tick_of(value))
    return text[:digits] if digits <= 7 else text + "0" * (digits - 7)


__all__ = [
    "Datetime2Tick",
    "decode_sql_timestamp",
    "drop_tick",
    "fraction_text",
    "install_tick_converter",
    "temporal_iso_text",
    "tick_from_text",
    "tick_of",
    "with_tick",
]


def _bracket(name: str) -> str:
    return "[" + str(name).replace("]", "]]") + "]"


def full_precision_select_list(cur: Any, table_ref: str, columns: list[str] | None) -> str:
    """Select list reading DATETIME2(7) as SQL Server's own 27-character text.

    For DBAPI drivers without an output converter (pymssql reads DATETIME2 as
    a six-digit ``datetime``). ``columns`` ``None`` means every column, in
    table order. Other columns are selected unchanged; when the catalog cannot
    be read, the plain list is returned.
    """
    plain = "*" if not columns else ", ".join(_bracket(c) for c in columns)
    literal = table_ref.replace("'", "''")
    try:
        cur.execute(
            "SELECT c.name, t.name, c.scale FROM sys.columns c "  # nosec B608
            "JOIN sys.types t ON t.user_type_id = c.user_type_id "
            f"WHERE c.object_id = OBJECT_ID(N'{literal}') ORDER BY c.column_id"
        )
        catalog = [(str(n), str(t).lower(), int(s or 0)) for n, t, s in cur.fetchall()]
    except Exception:  # noqa: BLE001 - the plain list still reads every row
        return plain
    seven = {n.lower() for n, t, s in catalog if t == "datetime2" and s > 6}
    if not seven:
        return plain
    names = list(columns) if columns else [n for n, _t, _s in catalog]
    return ", ".join(
        f"CONVERT(varchar(27), {_bracket(n)}, 121) AS {_bracket(n)}"
        if str(n).lower() in seven
        else _bracket(n)
        for n in names
    )
