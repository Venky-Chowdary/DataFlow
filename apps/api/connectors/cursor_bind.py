"""Typed watermark binds for SQLAlchemy cursor predicates (D207).

A saved watermark is text (``'2026-10-01T00:30:00'``). Every incremental
predicate used to send it as a string inside ``CAST(:p AS <column type>)``, so
the *engine* parsed it — and Oracle parses a string into DATE / TIMESTAMP /
TIMESTAMP WITH TIME ZONE with the session's ``NLS_*_FORMAT`` (``DD-MON-RR``):
ORA-01843 "not a valid month" on every run after the first. SQL Server also
parses a space-separated string into legacy DATETIME by the login's DATEFORMAT.
(The PG / MySQL / Snowflake readers bind raw DB-API parameters that the server
types against the column; they do not go through this module.)

This module is the one owner of that bind: the text is parsed *here* into the
Python value matching the column's declared type (``datetime`` / ``date`` /
``time`` / ``int`` / ``Decimal``) and bound with that type, so no engine ever
parses a date string. Zone-bearing columns compare as instants: an aware
watermark is converted to UTC (a naive one is UTC); a zone-less column compares
its wall clock (an offset on the watermark is dropped, the same thing a PG
``CAST`` does). Text a temporal/numeric column cannot hold is refused with the
column named — never bound as a string for the engine to guess at.
"""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

_INT_RE = re.compile(r"^[+-]?\d+$")
_FRACTION_RE = re.compile(r"(\.\d{6})\d+")
_TZ_SUFFIX_RE = re.compile(r"\s*([+-]\d{2})(:?\d{2})?$")


class CursorBindError(ValueError):
    """The stored watermark cannot be a value of the cursor column's type."""


def _norm_datetime_text(text: str) -> str:
    raw = text.strip().replace("Z", "+00:00").replace("z", "+00:00")
    if " " in raw and "T" not in raw:
        head, _, rest = raw.partition(" ")
        raw = f"{head}T{rest.strip()}"
    # ``2026-10-01T00:30:00 +05:30`` (space before the offset).
    raw = re.sub(r"\s+([+-]\d{2}(:?\d{2})?)$", r"\1", raw)
    # Sub-microsecond digits: Python carries microseconds.
    return _FRACTION_RE.sub(r"\1", raw)


def parse_cursor_datetime(text: str) -> datetime | None:
    try:
        return datetime.fromisoformat(_norm_datetime_text(text))
    except ValueError:
        return None


def _parse_date(text: str) -> date | datetime | None:
    raw = text.strip()
    try:
        return date.fromisoformat(raw)
    except ValueError:
        pass
    value = parse_cursor_datetime(raw)
    if value is None:
        return None
    if value.tzinfo is None and value.time() == time(0):
        return value.date()
    return value


def _parse_time(text: str) -> time | None:
    try:
        return time.fromisoformat(_FRACTION_RE.sub(r"\1", text.strip()))
    except ValueError:
        return None


def _parse_decimal(text: str) -> Decimal | None:
    try:
        value = Decimal(text.strip())
    except (InvalidOperation, ValueError):
        return None
    return value if value.is_finite() else None


def _to_utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def cursor_kind(col_type: Any) -> str:
    """The family a cursor column's declared type binds as.

    ``datetime_tz`` / ``datetime`` / ``date`` / ``time`` / ``integer`` /
    ``decimal`` / ``float`` / ``text``, or ``""`` for a type this module does
    not own (the caller keeps its legacy cast).
    """
    import sqlalchemy as sa

    if col_type is None:
        return ""
    if isinstance(col_type, sa.DateTime):
        name = type(col_type).__name__.upper()
        if (
            bool(getattr(col_type, "timezone", False))
            or bool(getattr(col_type, "local_timezone", False))
            or "OFFSET" in name  # SQL Server DATETIMEOFFSET
            or name in {"TIMESTAMP_TZ", "TIMESTAMP_LTZ", "TIMESTAMPTZ"}
        ):
            return "datetime_tz"
        from connectors.generic_sql import _oracle_tz_kind_sa

        return "datetime_tz" if _oracle_tz_kind_sa(col_type) else "datetime"
    if isinstance(col_type, sa.Date):
        return "date"
    if isinstance(col_type, sa.Time):
        return "time"
    if isinstance(col_type, sa.Float):
        return "float"
    if isinstance(col_type, sa.Numeric):
        # Oracle NUMBER subclasses Numeric *and* Integer; Numeric owns it.
        return "decimal"
    if isinstance(col_type, sa.Integer):
        return "integer"
    if isinstance(col_type, (sa.String, sa.Enum)):
        return "text"
    return ""


def cursor_python_value(text: str, kind: str, *, column: str = "") -> Any:
    """Parse watermark ``text`` into the Python value a ``kind`` column binds."""
    label = f"cursor column {column!r}" if column else "cursor column"
    if kind == "text":
        return text
    if kind in ("datetime", "datetime_tz"):
        value = parse_cursor_datetime(text)
        if value is None:
            raise CursorBindError(
                f"Saved watermark {text!r} is not a date/time, but {label} is a "
                "date/time column — clear the cursor (clear_cursor_resnapshot) to "
                "re-read from the start."
            )
        if kind == "datetime":
            return value.replace(tzinfo=None)  # wall clock, as stored
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if kind == "date":
        value = _parse_date(text)
        if value is None:
            raise CursorBindError(f"Saved watermark {text!r} is not a date for {label}.")
        return value.replace(tzinfo=None) if isinstance(value, datetime) else value
    if kind == "time":
        value = _parse_time(text)
        if value is None:
            raise CursorBindError(f"Saved watermark {text!r} is not a time for {label}.")
        return value
    if kind == "integer":
        if _INT_RE.match(text.strip()):
            return int(text.strip())
        value = _parse_decimal(text)
        if value is None:
            raise CursorBindError(f"Saved watermark {text!r} is not a number for {label}.")
        return value
    if kind == "decimal":
        value = _parse_decimal(text)
        if value is None:
            raise CursorBindError(f"Saved watermark {text!r} is not a number for {label}.")
        return value
    if kind == "float":
        value = _parse_decimal(text)
        if value is None:
            raise CursorBindError(f"Saved watermark {text!r} is not a number for {label}.")
        return float(value)
    return text


def sa_cursor_bind(col: Any, text: Any) -> Any:
    """A typed SQLAlchemy bind of watermark ``text`` against column ``col``.

    Use it on the right-hand side of every cursor comparison
    (``col > sa_cursor_bind(col, wm)``).
    """
    import sqlalchemy as sa

    raw = "" if text is None else str(text)
    col_type = getattr(col, "type", None)
    kind = cursor_kind(col_type)
    if not kind:
        # A type no parser owns (UUID, binary, vendor types): the engine's own
        # cast, as before.
        return sa.cast(sa.literal(raw), col_type)
    value = cursor_python_value(raw, kind, column=str(getattr(col, "name", "") or ""))
    if kind == "datetime_tz":
        return _instant_bind(col, value)
    if kind == "date" and isinstance(value, datetime):
        return sa.bindparam(None, value, type_=sa.DateTime(), unique=True)
    if kind == "integer" and isinstance(value, Decimal):
        return sa.bindparam(None, value, type_=sa.Numeric(), unique=True)
    return sa.bindparam(None, value, type_=col_type, unique=True)


def _instant_bind(col: Any, value: datetime) -> Any:
    """Bind an instant against a zone-bearing column, per dialect.

    python-oracledb binds a ``datetime`` without its tzinfo, and pyodbc's
    DATETIMEOFFSET handling is driver-specific; the UTC wall clock plus an
    explicit zone is exact on every engine.
    """
    import sqlalchemy as sa

    from connectors.generic_sql import _oracle_tz_kind_sa

    utc = _to_utc_naive(value)
    if _oracle_tz_kind_sa(getattr(col, "type", None)):
        from sqlalchemy.dialects import oracle

        stamp = sa.bindparam(None, utc, type_=oracle.TIMESTAMP(), unique=True)
        return sa.func.from_tz(sa.cast(stamp, oracle.TIMESTAMP()), sa.literal_column("'UTC'"))
    return sa.bindparam(
        None, value.astimezone(timezone.utc), type_=getattr(col, "type", None), unique=True
    )


def count_cursor_window(conn: Any, table_obj: Any, col: Any, text: Any) -> int | None:
    """``COUNT(*)`` of rows at or past the watermark, on the typed bind.

    ``None`` when the count cannot be taken — never a guess: a short total
    stops a stream after its first page.
    """
    import sqlalchemy as sa

    try:
        stmt = sa.select(sa.func.count()).select_from(table_obj).where(
            col >= sa_cursor_bind(col, text)
        )
        return conn.execute(stmt).scalar()
    except CursorBindError:
        raise
    except Exception:  # noqa: BLE001 - an uncountable window is unknown, not zero
        return None



# --------------------------------------------------------------------------- #
# Zone-bearing cursor values compared as instants (client side).
#
# A zone-bearing column (SQL Server DATETIMEOFFSET, Oracle TIMESTAMP WITH TIME
# ZONE, PG timestamptz) hands each cell over with its own offset. The server
# orders and compares those by instant; text order does not:
# ``2026-01-01T10:00:00+05:30`` (04:30Z) sorts after ``2026-01-01T06:00:00+00:00``.
# Every client-side page maximum, bookmark comparison and stored watermark goes
# through these two helpers so it agrees with the server.
# --------------------------------------------------------------------------- #

_AWARE_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?\s*"
    r"(Z|z|[+-]\d{2}(?::?\d{2})?)$"
)


def _aware_parts(text: Any) -> tuple[datetime, str] | None:
    """``(UTC instant to the second, fraction digits)`` of offset-bearing text.

    The wall clock is read as UTC and the offset subtracted: the result is the
    instant in UTC, whatever offset the text was written with.
    """
    if not isinstance(text, str):
        return None
    m = _AWARE_RE.match(text.strip())
    if m is None:
        return None
    year, month, day, hh, mi, ss, frac, off = m.groups()
    try:
        if off in ("Z", "z"):
            sign, oh, om = 1, 0, 0
        else:
            sign = -1 if off[0] == "-" else 1
            digits = off[1:].replace(":", "")
            oh, om = int(digits[:2]), int(digits[2:4] or 0)
        local = datetime(
            int(year), int(month), int(day), int(hh), int(mi), int(ss), tzinfo=timezone.utc
        )
        utc = local - sign * timedelta(hours=oh, minutes=om)
    except (ValueError, OverflowError):
        return None
    return utc, (frac or "").rstrip("0")


def cursor_instant_key(text: Any) -> tuple[datetime, Decimal] | None:
    """A sort key ordering offset-bearing timestamp text by instant, else None.

    Only text that names its own offset (``+05:30``, ``-08:00``, ``Z``) is an
    instant; a naive timestamp is a wall clock and keeps its own ordering.
    Fraction digits are kept exactly (DATETIMEOFFSET(7) carries 100 ns), so two
    instants a tick apart never compare equal.
    """
    parts = _aware_parts(text)
    if parts is None:
        return None
    utc, frac = parts
    return utc, Decimal("0." + frac) if frac else Decimal(0)


def canonical_cursor_text(text: Any) -> Any:
    """Offset-bearing timestamp text rewritten as its UTC instant; other values unchanged.

    ``2026-01-01T02:45:00-08:00`` -> ``2026-01-01T10:45:00+00:00``. One spelling
    per instant, so a stored watermark never depends on which offset the row
    that set it happened to be written with. Fraction digits are kept as read
    (trailing zeros dropped).
    """
    parts = _aware_parts(text)
    if parts is None:
        return text
    utc, frac = parts
    stamp = utc.strftime("%Y-%m-%dT%H:%M:%S")
    return f"{stamp}.{frac}+00:00" if frac else f"{stamp}+00:00"


def declared_type_is_zone_bearing(declared: Any) -> bool:
    """True for a declared column type whose values carry their own instant.

    PG ``timestamp with time zone`` / ``timestamptz``, SQL Server
    ``datetimeoffset``, Oracle ``TIMESTAMP WITH [LOCAL] TIME ZONE``, Snowflake
    ``TIMESTAMP_TZ`` / ``TIMESTAMP_LTZ``. Text, naive timestamps and unknown
    types are not.
    """
    text = " ".join(str(declared or "").lower().split())
    if not text:
        return False
    if "without time zone" in text:
        return False
    return (
        "with time zone" in text
        or "with local time zone" in text
        or text.startswith(("timestamptz", "datetimeoffset", "timestamp_tz", "timestamp_ltz"))
    )
