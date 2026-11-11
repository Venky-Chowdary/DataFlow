"""Reversibility of one declared type mapping (a column's source type -> destination type).

Split out of :mod:`services.reversibility` (the per-column table). The narrowing
rules are the type system's (``services.type_system`` ``*_would_narrow``, the
time-zone polarity, national-charset and enum-domain helpers): one rule set with
preflight. Two things differ, on purpose, from how Map uses those helpers:

* **Bare spellings resolve per engine.** For create-new invent the type system
  reads a bare ``integer`` / ``float`` / ``timestamp`` as width-unknown, so Map
  never narrows by surface spelling. Here both types are the declared types of
  real columns, and "unknown" read as "fits" called ``BIGINT -> integer`` a
  widening. A bare token is therefore resolved to the carrier it names on its
  own engine (``integer`` is int4 on PostgreSQL, ``FLOAT`` is single precision
  on MySQL, a bare ``timestamp`` keeps 6 fractional digits on PostgreSQL), and a
  spelling no engine resolves stays unknown, never "fits". The type system's
  own answers are unchanged for its other callers.
* **Losses that are not widths are named.** A time zone dropped (aware -> naive),
  ``json -> jsonb`` (duplicate keys, key order and spacing), a cast into a sized
  text column whose rendered width does not fit, a national charset narrowed, an
  enum domain, and CHAR blank padding.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from typing import Any

INVERTIBLE, LOSSY, CONDITIONAL = "invertible", "lossy", "conditional"

TEXT = frozenset({"string", "text"})
_SAFE_WIDEN = frozenset({("integer", "decimal"), ("date", "datetime"), ("boolean", "integer")})
_TRUNCATE = frozenset({("datetime", "date"), ("datetime", "time")})
_ROUND = frozenset({("decimal", "integer"), ("float", "integer"), ("decimal", "float"), ("float", "decimal")})
#: Engines whose bare ``FLOAT`` is IEEE single / double precision.
_SINGLE_FLOAT = frozenset({"mysql", "mariadb"})
_DOUBLE_FLOAT = frozenset({"postgresql", "sqlserver", "sqlite", "snowflake", "redshift", "duckdb", "bigquery"})
#: Engines whose bare temporal spellings carry a declared fractional precision
#: (SQLite stores text: its declaration says nothing about the digits kept).
_NO_DECLARED_FSP = frozenset({"sqlite", ""})
_INT_CARRIER = {8: "TINYINT", 16: "SMALLINT", 32: "INT4", 64: "BIGINT"}
_PARAMS = re.compile(r"\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)")
#: Longest text a value of the family renders as (sign, digits, separators, offset).
_FLOAT_TEXT = 24  # '-1.7976931348623157e+308'
_TZ_TEXT = 6  # '+00:00'


def _norm(value: Any) -> str:
    return str(value or "").strip()


def logical(type_str: str) -> str:
    """The logical family, or "" when no type is declared (the type system reads "" as string)."""
    if not _norm(type_str):
        return ""
    from services.type_system import normalize_logical_type

    return normalize_logical_type(type_str)


def engine(db: str) -> str:
    if not _norm(db):
        return ""
    from services.type_system import _normalize_dest_db

    return _normalize_dest_db(db)


def resolve(type_str: str, db: str) -> str:
    """The carrier a bare spelling names on ``db`` (``integer`` -> ``INT4`` on PostgreSQL)."""
    from services import type_system as ts

    text, eng = _norm(type_str), engine(db)
    if not text or not eng:
        return text
    family = logical(text)
    if family == "integer" and ts.integer_bit_width(text) is None:
        bounds = ts.integer_storage_bounds(text.upper(), dest_db=eng)
        if bounds and bounds[0] < 0:
            return _INT_CARRIER.get(bounds[1].bit_length() + 1, text)
    if family == "float" and text.lower() == "float":
        if eng in _SINGLE_FLOAT:
            return "FLOAT4"
        if eng in _DOUBLE_FLOAT:
            return "FLOAT8"
    return text


def _fsp(type_str: str, db: str) -> int | None:
    from services.type_system import destination_temporal_fractional_digits

    try:
        return destination_temporal_fractional_digits(type_str, dest_db=db)
    except Exception:  # noqa: BLE001 - an unreadable spelling decides nothing
        return None


def _fsp_narrows(stype: str, ttype: str, *, dest_db: str, source_db: str) -> bool:
    """Fractional seconds the destination drops, the source's bare spelling read on its own engine."""
    if logical(stype) not in {"datetime", "time"} or logical(ttype) not in {"datetime", "time"}:
        return False
    if engine(source_db) in _NO_DECLARED_FSP:
        return False
    src, tgt = _fsp(stype, engine(source_db)), _fsp(ttype, engine(dest_db))
    return src is not None and tgt is not None and tgt < src


def narrows(stype: str, ttype: str, *, dest_db: str, source_db: str) -> bool:
    """Any narrowing the type system names, on the carriers the spellings resolve to."""
    from services import type_system as ts

    s, t = resolve(stype, source_db), resolve(ttype, dest_db)
    checks = (
        lambda: ts.string_width_would_narrow(s, t, dest_db=dest_db),
        lambda: ts.decimal_params_would_narrow(s, t, dest_db=dest_db),
        lambda: ts.integer_width_would_narrow(s, t, dest_db=dest_db),
        lambda: ts.float_mantissa_would_narrow(s, t, dest_db=dest_db, source_db=source_db),
        lambda: ts.temporal_precision_would_narrow(s, t, dest_db=dest_db),
        lambda: _fsp_narrows(stype, ttype, dest_db=dest_db, source_db=source_db),
        lambda: ts.binary_width_would_narrow(s, t),
        lambda: ts.interval_precision_would_narrow(s, t),
        lambda: ts.bitstring_width_would_narrow(s, t),
    )
    for check in checks:
        try:
            if check():
                return True
        except Exception:  # noqa: BLE001 - a helper that cannot read a spelling decides nothing
            continue
    return False


def timezone_dropped(stype: str, ttype: str, *, dest_db: str) -> bool:
    """An offset-aware source landing on a naive (wall-clock) destination."""
    from services import type_system as ts

    sl, tl = logical(stype), logical(ttype)
    if sl == "time" and tl == "time":
        return ts.time_timezone_polarity(stype) == "tz" and ts.time_timezone_polarity(ttype) == "ntz"
    if sl == "datetime" and tl == "datetime":
        return (ts.datetime_timezone_polarity(stype) in {"tz", "ltz"}
                and ts.datetime_timezone_polarity(ttype, dest_db=engine(dest_db)) == "ntz")
    return False


def _base(type_str: str) -> str:
    return re.sub(r"\s*\(.*$", "", _norm(type_str)).upper()


def rendered_width(stype: str, *, source_db: str) -> int | None:
    """The longest text a ``stype`` value renders as, or ``None`` when the type does not bound it."""
    from services import type_system as ts

    family = logical(stype)
    if family == "integer":
        bits = ts.integer_bit_width(resolve(stype, source_db))
        return len(str(-(1 << (bits - 1)))) if bits else None
    if family == "decimal":
        match = _PARAMS.search(stype)
        if not match:
            return None
        precision, scale = int(match.group(1)), int(match.group(2) or 0)
        return 1 + max(precision - scale, 1) + (scale + 1 if scale else 0)
    if family == "float":
        return _FLOAT_TEXT
    if family == "boolean":
        return len("false")
    if family == "uuid":
        return 36
    if family == "date":
        return len("YYYY-MM-DD")
    if family in {"datetime", "time"}:
        digits = _fsp(stype, engine(source_db)) if engine(source_db) not in _NO_DECLARED_FSP else None
        digits = 9 if digits is None else digits
        aware = (ts.datetime_timezone_polarity(stype) in {"tz", "ltz"} if family == "datetime"
                 else ts.time_timezone_polarity(stype) == "tz")
        clock = len("YYYY-MM-DDTHH:MM:SS") if family == "datetime" else len("HH:MM:SS")
        return clock + (digits + 1 if digits else 0) + (_TZ_TEXT if aware else 0)
    return None


def _text_width(ttype: str) -> int | None:
    from services import type_system as ts

    if ts.is_unlimited_string_carrier(ttype):
        return None
    return ts.parse_string_carrier_width(ttype)


def type_pair(stype: str, ttype: str, *, dest_db: str, source_db: str) -> tuple[str, str]:
    """(invertibility, reason code) of a type mapping alone."""
    from services import type_system as ts

    slog, tlog = logical(stype), logical(ttype)
    if not slog or not tlog:
        return CONDITIONAL, "type_unknown"
    if narrows(stype, ttype, dest_db=dest_db, source_db=source_db):
        return LOSSY, "narrowing"
    if timezone_dropped(stype, ttype, dest_db=dest_db):
        return LOSSY, "timezone"
    if _base(stype) == "JSON" and _base(ttype) == "JSONB":
        return LOSSY, "json_normalize"
    if (slog, tlog) in _TRUNCATE:
        return LOSSY, "truncation"
    if (slog, tlog) in _ROUND:
        return LOSSY, "rounding"
    if slog == tlog or {slog, tlog} <= TEXT:
        if tlog in TEXT and ts.national_charset_would_collapse(stype, ttype, dest_db=engine(dest_db)):
            return CONDITIONAL, "charset"
        if ts.enum_set_domain_would_reject(stype, ttype):
            return CONDITIONAL, "enum"
        if tlog in TEXT and ts.is_fixed_char_carrier(ttype) and not ts.is_fixed_char_carrier(stype):
            return CONDITIONAL, "blank_pad"
        return INVERTIBLE, ("identity" if _norm(stype).upper() == _norm(ttype).upper() else "widen")
    if (slog, tlog) in _SAFE_WIDEN:
        return INVERTIBLE, "lossless_cast"
    if tlog in TEXT and slog != "binary":
        width = _text_width(ttype)
        if width is None:
            return INVERTIBLE, "lossless_cast"
        need = rendered_width(stype, source_db=source_db)
        if need is None:
            return CONDITIONAL, "text_width"
        return (LOSSY, "narrowing") if need > width else (INVERTIBLE, "lossless_cast")
    return CONDITIONAL, "cast"


#: Conditional type codes checked value by value (``text`` is the value's exact text).
def _fits(code: str, text: str, ttype: str) -> bool:
    from services import type_system as ts

    if code == "charset":
        return text.isascii()  # every code page holds ASCII; anything else may be replaced
    if code == "enum":
        members = ts.parse_enum_or_set_members(ttype)
        return members is not None and text in members[1]
    if code == "blank_pad":
        return not text.endswith(" ")
    if code == "text_width":
        width = _text_width(ttype)
        return width is None or len(text) <= width
    return False


TYPE_CHECKED = frozenset({"charset", "enum", "blank_pad", "text_width"})


def check_type_values(code: str, values: Iterable[Any], ttype: str,
                      text_of: Callable[[Any], str | None]) -> dict[str, Any]:
    """{status, rows_checked, rows_not_fitting} of a conditional type mapping over observed values."""
    checked = failing = 0
    for value in values:
        text = text_of(value)
        if text is None:
            continue
        checked += 1
        if not _fits(code, text, ttype):
            failing += 1
    status = "not_checked" if not checked else ("does_not_fit" if failing else "fits")
    return {"status": status, "rows_checked": checked, "rows_not_fitting": failing}
