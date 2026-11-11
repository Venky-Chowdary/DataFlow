"""Exact per-type tokens for the rehearsal digest, where Gate-8's typed normalisation folds values.

Gate-8's typed kernel (:mod:`services.reconciliation`) compares a value with its
re-rendering in another type, so it folds on purpose: a double to 15
significant digits, a timestamp to its instant, a ``numeric`` to its value, a
``timetz`` to a wall clock. A rehearsal must prove *every value* survived, so
for those types each side is reduced to a token that keeps what the type
stores (review 2, N5 / N8):

* ``float`` - the IEEE double itself (``repr``): ``0.30000000000000004`` is not
  ``0.3``, ``-0.0`` is not ``0.0``;
* ``numeric_text`` - an unconstrained ``numeric`` / ``NUMBER`` keeps the scale it
  was written with, so its exact decimal text is compared (``1.50`` is not
  ``1.5``); a column with a declared scale compares by value through Gate-8;
* ``offset`` - a type that stores each value's own offset (``DATETIMEOFFSET``,
  Oracle / Snowflake ``TIMESTAMP WITH TIME ZONE``): the instant *and* the offset;
* ``aware`` - a PostgreSQL ``timestamptz``: the instant, and an extract that lost
  the zone (naive text) differs;
* ``naive_datetime`` - an Oracle ``DATE`` keeps the time of day;
* ``offset_time`` - a ``timetz``: the wall clock and its offset.

A value a token cannot parse is kept as its exact text, so it differs from any
parsed rendering (fail closed).
"""

from __future__ import annotations

import re
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

FLOAT, NUMERIC_TEXT, OFFSET, AWARE, NAIVE_DATETIME, OFFSET_TIME = (
    "float", "numeric_text", "offset", "aware", "naive_datetime", "offset_time")
#: Engines whose bare NUMERIC / DECIMAL / NUMBER keeps any scale it is given.
_UNCONSTRAINED_NUMERIC = frozenset({"postgresql", "oracle"})


def _base(ddl: str) -> str:
    return re.sub(r"\s*\(.*$", "", str(ddl or "").strip()).upper()


def token_kind(ddl: str, engine: str) -> str | None:
    """The exact token a declared source type needs, or ``None`` (Gate-8's typed normalisation is exact)."""
    from services.reversibility_types import keeps_offset
    from services.type_system import datetime_timezone_polarity, normalize_logical_type

    text = str(ddl or "").strip()
    if not text:
        return None
    family, base = normalize_logical_type(text), _base(text)
    if family == "float":
        return FLOAT
    if family == "decimal" and "(" not in text and engine in _UNCONSTRAINED_NUMERIC \
            and base in {"NUMERIC", "DECIMAL", "NUMBER"}:
        return NUMERIC_TEXT
    if family == "time" and keeps_offset(text, engine):
        return OFFSET_TIME
    if engine == "oracle" and base == "DATE":
        return NAIVE_DATETIME
    if family == "datetime":
        if keeps_offset(text, engine):
            return OFFSET
        if engine == "postgresql" and datetime_timezone_polarity(text, dest_db=engine) in {"tz", "ltz"}:
            return AWARE
    return None


def _text(value: Any) -> str:
    literal = getattr(value, "literal", None)
    if isinstance(literal, str) and literal:
        return literal
    if isinstance(value, float):
        return float.__repr__(float(value))
    return str(value)


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time())
    try:
        return datetime.fromisoformat(str(value).strip())
    except ValueError:
        return None


def _offset_seconds(value: datetime | time) -> int | None:
    offset = value.utcoffset()
    return None if offset is None else int(offset.total_seconds())


def token(kind: str, value: Any) -> str | None:
    """``value``'s exact token for ``kind`` (``None`` stays SQL NULL)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return f"b:{value}"
    if kind == FLOAT:
        try:
            return "f:" + float.__repr__(float(value if isinstance(value, (int, float)) else _text(value).strip()))
        except (TypeError, ValueError):
            return "t:" + _text(value)
    if kind == NUMERIC_TEXT:
        try:
            return "n:" + str(Decimal(_text(value).strip()))
        except (InvalidOperation, ValueError):
            return "t:" + _text(value)
    if kind == OFFSET_TIME:
        try:
            parsed = value if isinstance(value, time) else time.fromisoformat(_text(value).strip())
        except ValueError:
            return "t:" + _text(value)
        return f"tt:{parsed.replace(tzinfo=None).isoformat()}|{_offset_seconds(parsed)}"
    parsed = _datetime(value)
    if parsed is None:
        return "t:" + _text(value)
    offset = _offset_seconds(parsed)
    if offset is None:
        return "naive:" + parsed.isoformat()
    instant = parsed.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
    return f"o:{instant}|{offset}" if kind == OFFSET else f"utc:{instant}"
