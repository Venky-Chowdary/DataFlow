"""How a destination renders a value cast from text, for the reversibility cast rule (review 4, M-2).

A cast from text is reversible only when the destination renders the stored
value as the exact text it was given: ``007`` lands in an INTEGER as ``7``,
``2024-1-5`` in a DATE as ``2024-01-05``, ``0.1000`` in a REAL as ``0.1`` and
``1e3`` is refused by an INTEGER. The gate's sample check and the writer's
per-value check both ask :func:`cast_changes`, so they agree.

Rendered here: integers, floating point (single precision where the engine
stores it; MySQL shows a ``FLOAT`` to six significant digits and a
``FLOAT(M,D)`` to ``D`` decimals), dates, zoneless timestamps and times without
an offset (a fraction finer than the declared precision is rounded away), in
the destination engine's own output format, the engine named by its family.
An offset-keeping timestamp is rendered where its text is the engine's own
(SQLite: RFC 3339 in UTC) and refused elsewhere (the session time zone decides
its text). A type not rendered here (decimal, boolean, a time with an offset,
...) answers ``None`` and keeps the gate's transform round trip.

On a text column the writer stores the transform's own text, so the gate's
transform round trip is already what lands (review 5, M-2).
"""

from __future__ import annotations

import math
import re
import struct
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

_PG = frozenset({"postgresql"})
#: (engine family, declared base) pairs stored as a single-precision float.
_FLOAT4 = {("postgresql", "REAL"), ("postgresql", "FLOAT4"), ("mysql", "FLOAT"), ("mysql", "FLOAT32"),
           ("sqlserver", "REAL")}
#: Engines that print an integral float without ".0".
_BARE_INTEGRAL = frozenset({"postgresql", "mysql", "sqlserver"})
_REFUSED = object()
#: A timestamp type that keeps an offset (TIMESTAMPTZ, DATETIMEOFFSET, TIMESTAMP WITH TIME ZONE): the stored text is
#: the engine's own (SQLite: RFC 3339 in UTC; elsewhere it follows the session time zone, so it is refused).
OFFSET_DATETIME = "offset_datetime"


def _base(ddl: str) -> str:
    return re.sub(r"\s*\(.*$", "", str(ddl or "").strip()).upper()


def _float4_text(value: float) -> str:
    """The shortest text that reads back as the same single-precision float."""
    single = struct.unpack("f", struct.pack("f", value))[0]
    for digits in range(1, 10):
        text = f"{single:.{digits}g}"
        if struct.unpack("f", struct.pack("f", float(text)))[0] == single:
            return text
    return repr(single)


def _float_text(value: float, ddl: str, engine: str) -> Any:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    single = (engine, _base(ddl)) in _FLOAT4
    stored = struct.unpack("f", struct.pack("f", value))[0] if single else value
    scale = re.search(r"\(\s*\d+\s*,\s*(\d+)\s*\)", str(ddl or "")) if engine == "mysql" else None
    if scale:
        return f"{stored:.{int(scale.group(1))}f}"  # MySQL FLOAT(M,D) / DOUBLE(M,D) shows D decimals
    if single and engine == "mysql":
        # MySQL sends a FLOAT to six significant digits (16777216 reads as 16777200); an exponent form is
        # refused rather than guessed (review 5, M-3).
        text = f"{stored:.6g}"
        return _REFUSED if "e" in text else text
    text = _float4_text(value) if single else float.__repr__(value)
    if engine in _BARE_INTEGRAL and text.endswith(".0"):
        text = text[:-2]  # these engines print an integral float without ".0"
    return text


def _keeps_fraction(value: datetime | time, ddl: str, engine: str) -> bool:
    """Whether the declared type keeps ``value``'s fraction (``TIMESTAMP(0)`` rounds ``.5`` away; review 5, M-3)."""
    if not value.microsecond or not engine:
        return True
    if engine == "sqlserver" and _base(ddl) == "DATETIME":
        # A legacy DATETIME keeps 1/300 s: only .xx0 / .xx3 / .xx7 milliseconds read back as given.
        return value.microsecond % 1000 == 0 and value.microsecond // 1000 % 10 in {0, 3, 7}
    from services.type_system import destination_temporal_fractional_digits

    digits = destination_temporal_fractional_digits(ddl, dest_db=engine)
    return digits is None or digits >= 6 or value.microsecond % 10 ** (6 - digits) == 0


def _clock(value: datetime | time, engine: str) -> str:
    base = f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
    if not value.microsecond:
        return base
    fraction = f".{value.microsecond:06d}"
    return base + (fraction.rstrip("0") if engine in _PG else fraction)


def render_value(value: Any, family: str, ddl: str, engine: str) -> Any:
    """The destination's text of a value already cast to ``family`` (``_REFUSED``: not that type)."""
    if isinstance(value, str):
        return render_text(value, family, ddl, engine)
    if family == OFFSET_DATETIME:
        return _offset_text(value, engine)
    if isinstance(value, bool):
        return _REFUSED
    try:
        if family == "integer":
            if isinstance(value, float) and not value.is_integer():
                return _REFUSED
            return str(int(value))
        if family == "float":
            return _float_text(float(value), ddl, engine)
    except (TypeError, ValueError, OverflowError, InvalidOperation):
        return _REFUSED
    if family == "date" and isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if family in {"datetime", "time"} and isinstance(value, (datetime, time)) \
            and not _keeps_fraction(value, ddl, engine):
        return _REFUSED
    if family == "datetime" and isinstance(value, datetime) and value.tzinfo is None:
        return f"{value.year:04d}-{value.month:02d}-{value.day:02d} {_clock(value, engine)}"
    if family == "time" and isinstance(value, time) and value.tzinfo is None:
        return _clock(value, engine)
    return _REFUSED


def _offset_text(value: Any, engine: str) -> Any:
    """How an offset-keeping timestamp column stores ``value`` (review 5, M-2).

    SQLite's writer stores an aware value as RFC 3339 in UTC (``Z`` and
    ``+02:00`` are respelled); a value without an offset is refused. Any other
    engine renders it in the session's time zone, which the gate cannot know,
    so a cast from text into it is refused rather than guessed.
    """
    if engine != "sqlite":
        return _REFUSED
    try:
        parsed = datetime.fromisoformat(value.strip()) if isinstance(value, str) else value
    except ValueError:
        return _REFUSED
    if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
        return _REFUSED
    return parsed.astimezone(timezone.utc).isoformat()


def render_text(text: str, family: str, ddl: str, engine: str) -> Any:
    """The destination's text of ``text`` cast to ``family`` (``_REFUSED`` when the cast refuses it)."""
    if family == OFFSET_DATETIME:
        return _offset_text(text, engine)
    try:
        if family == "integer":
            if not re.fullmatch(r"\s*[+-]?\d+\s*", text):
                return _REFUSED
            return str(int(text))
        if family == "float":
            return _float_text(float(Decimal(text.strip())), ddl, engine)
        if family == "date":
            return date.fromisoformat(text).isoformat()
        if family == "datetime":
            return render_value(datetime.fromisoformat(text), family, ddl, engine)
        if family == "time":
            return render_value(time.fromisoformat(text), family, ddl, engine)
    except (ValueError, ArithmeticError):
        return _REFUSED
    return _REFUSED


def rendered_family(ddl: str) -> str | None:
    """The family :func:`cast_changes` renders for a destination type, else ``None``."""
    from services.reversibility_types import keeps_offset, logical

    family = logical(ddl)
    if family in {"datetime", "time"}:
        from services.type_system import datetime_timezone_polarity

        if family == "datetime" and (keeps_offset(ddl, "") or datetime_timezone_polarity(ddl) in {"tz", "ltz"}):
            return OFFSET_DATETIME  # rendered by the engine's own rule, or refused (review 5, M-2)
        if keeps_offset(ddl, ""):
            return None  # a time with an offset: not rendered here
        if family == "time" and "ZONE" in str(ddl).upper().replace("WITHOUT TIME ZONE", ""):
            return None
    return family if family in {"integer", "float", "date", "datetime", "time", OFFSET_DATETIME} else None


#: Engine transforms that only cast text into a type (their family): checked by the destination's rendering.
_TYPE_CAST_TRANSFORMS = {"integer": "integer", "int": "integer", "float": "float", "double": "float",
                         "date": "date", "datetime": "datetime", "timestamp": "datetime", "time": "time"}


def renders_rule(transform: str, ddl: str) -> bool:
    """Whether a rule (a cast, or a transform that only casts into ``ddl``'s family) is checked here."""
    family = rendered_family(ddl)
    if family is None:
        return False
    name = str(transform or "").strip().lower()
    return not name or _TYPE_CAST_TRANSFORMS.get(name) == ("datetime" if family == OFFSET_DATETIME else family)


def cast_changes(text: str | None, converted: Any, ddl: str, engine: str, *, has_converted: bool = True) -> bool | None:
    """Whether the destination would hold ``text`` as other text (``None``: no rendering rule for ``ddl``).

    ``converted`` is the writer's conversion of the value (checked too: what
    lands is what the writer converted); the gate passes none.
    """
    family = rendered_family(ddl)
    if family is None:
        return None
    if text is None:
        return False
    from src.transfer.engine_family import family_or_kind

    eng = family_or_kind(engine)  # a hosted tile renders as its engine (review 5, C-1 / M-3)
    if render_text(text, family, ddl, eng) != text:
        return True
    if has_converted and converted is not None and render_value(converted, family, ddl, eng) != text:
        return True
    return False
