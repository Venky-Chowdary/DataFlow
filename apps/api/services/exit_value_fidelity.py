"""Which source values a driver reads one-to-one, and the ones it folds (review 4, C-1 / H-2).

A rehearsal compares the source with its extract. Both sides used to come
from the same Python value: the source re-read through the driver, and the
extract written from the driver's value. Where the driver's adapter is not
one-to-one, two different stored values read as one, and so does the extract,
so the comparison proved nothing (psycopg2 reads ``'[2:3]={1,2}'`` as
``[1, 2]``, ``TIME '24:00:00'`` as ``00:00:00``, an interval as a plain
duration and a jsonb number without its scale).

* **PostgreSQL**: for the types whose adapter is not one-to-one (arrays,
  ``time`` / ``timetz``, ``interval``, ``jsonb``) the source is also read as
  the server's own text (``col::text``). A value whose driver reading does not
  render back to that text was folded by the driver: its column is named as
  not provable, with the reason (:func:`driver_fold`). The extract can only
  hold what the driver handed it.
* **Other engines**: a declared type whose driver can read two stored values
  as one is named as not provable (:func:`adapter_fold`), unless its adapter is
  one-to-one. SQL Server keeps 100 ns in ``time`` / ``datetime2`` /
  ``datetimeoffset`` (the driver reads microseconds); Oracle keeps nanoseconds
  in ``TIMESTAMP(7..9)``, an offset or region in ``TIMESTAMP WITH TIME ZONE``
  (the driver hands back a naive datetime) and months in intervals; MySQL's
  single-precision ``FLOAT`` is sent rounded to its display digits. SQLite's
  driver hands back each stored value in its own storage class, one-to-one.

No reason repeats a value: each names the column and the kind of fold.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

ARRAY, TIME, TIMETZ, INTERVAL, JSONB = "array", "time", "timetz", "interval", "jsonb"
_PG = frozenset({"postgresql", "postgres"})

ARRAY_BOUNDS = ("holds an array whose lower bound is not 1; the driver reads it as a plain list, so the extract "
                "cannot hold its bounds and the column is not provable")
ARRAY_ELEMENT = ("holds an array element the driver reads as another value (24:00:00, an interval, a JSON number's "
                 "scale), so the extract cannot hold it exactly and the column is not provable")
TIME_24 = ("holds 24:00:00, which the driver reads as 00:00:00 of the same day, so the extract cannot tell the two "
           "apart and the column is not provable")
TIME_OTHER = "holds a time the driver reads as another value, so the column is not provable"
INTERVAL_FOLD = ("holds an interval the driver reads as a plain duration (months as 30 days, 24:00:00 as 1 day), so "
                 "the extract cannot hold its exact value and the column is not provable")
JSONB_SCALE = ("holds a JSON number whose scale the driver drops (trailing zeros are read away), so the extract "
               "cannot hold its exact spelling and the column is not provable")
JSONB_OTHER = "holds a JSON document the driver reads as another value, so the column is not provable"


def _upper(ddl: str) -> str:
    return re.sub(r"\s+", " ", str(ddl or "").strip()).upper()


def element_ddl(ddl: str) -> str | None:
    """The element type of an array declaration (``ARRAY<DECIMAL>``, ``INTEGER[]``, ``_int4``), else ``None``."""
    text = str(ddl or "").strip()
    m = re.fullmatch(r"(?i)ARRAY\s*<\s*(.*?)\s*>", text)
    if m:
        return m.group(1) or "VARCHAR"
    if text.endswith("]") and "[" in text:
        return re.sub(r"(\s*\[\s*\d*\s*\])+$", "", text)
    if text.upper() == "ARRAY":
        return "VARCHAR"
    return None


def _scalar_kind(ddl: str) -> str | None:
    up = re.sub(r"\(\s*\d+\s*\)", "", _upper(ddl)).strip()
    if up in {"TIME", "TIME WITHOUT TIME ZONE"}:
        return TIME
    if up in {"TIMETZ", "TIME WITH TIME ZONE"}:
        return TIMETZ
    if up.startswith("INTERVAL"):
        return INTERVAL
    if up == "JSONB":
        return JSONB
    return None


def server_text_kind(ddl: str, engine: str) -> str | None:
    """Whether a PostgreSQL column must also be read as the server's own text (its adapter is not one-to-one)."""
    if str(engine or "").lower() not in _PG:
        return None
    if element_ddl(ddl) is not None:
        return ARRAY
    return _scalar_kind(ddl)


# ------------------------------------------------------------ fold checks


def _time_fold(text: str, value: Any, *, tz: bool) -> str | None:
    try:
        parsed = time.fromisoformat(text.strip())
    except ValueError:
        return TIME_24 if text.strip().startswith("24:") else TIME_OTHER
    if not isinstance(value, time) or parsed.replace(tzinfo=None) != value.replace(tzinfo=None):
        return TIME_OTHER
    if tz and parsed.utcoffset() != value.utcoffset():
        return TIME_OTHER
    return None


def _interval_text(value: timedelta) -> str:
    """How PostgreSQL (IntervalStyle ``postgres``) spells the interval a timedelta holds as days and a time."""
    days, seconds, micros = value.days, value.seconds, value.microseconds
    parts = [f"{days} day" + ("" if days == 1 else "s")] if days else []
    if seconds or micros or not days:
        clock = f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"
        if micros:
            clock += f".{micros:06d}".rstrip("0")
        parts.append(("+" if days < 0 else "") + clock)
    return " ".join(parts)


def _number(value: Any) -> str:
    """A number's exact token: its value and its scale (``1.50`` is not ``1.5``; ``-0.0`` is not ``0.0``)."""
    literal = getattr(value, "literal", None)
    try:
        if isinstance(literal, str) and literal:
            return str(Decimal(literal))
        if isinstance(value, float):
            return str(Decimal(float.__repr__(value)))
        return str(Decimal(value) if not isinstance(value, Decimal) else value)
    except (InvalidOperation, ValueError, TypeError):
        return "?" + str(value)


def _json_exact(value: Any) -> Any:
    """A parsed JSON document with each number as its exact token (keys sorted on dump)."""
    if isinstance(value, dict):
        return {str(k): _json_exact(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_exact(v) for v in value]
    if value is None or isinstance(value, (bool, str)):
        return value
    return {"\u0000n": _number(value)}


def _jsonb_fold(text: str, value: Any) -> str | None:
    try:
        server = json.loads(text, parse_float=Decimal)
    except ValueError:
        return JSONB_OTHER
    if isinstance(value, str) and not isinstance(server, str):
        try:
            value = json.loads(value)
        except ValueError:
            return JSONB_OTHER

    def dump(doc: Any) -> str:
        return json.dumps(_json_exact(doc), sort_keys=True, ensure_ascii=False)

    if dump(server) == dump(value):
        return None
    # The same document by value: what the driver dropped is a number's spelling (its scale).
    by_value = json.dumps(server, sort_keys=True, default=float) == json.dumps(value, sort_keys=True, default=float)
    return JSONB_SCALE if by_value else JSONB_OTHER


_ARRAY_DIMS = re.compile(r"^((?:\[-?\d+:-?\d+\])+)=")


def parse_pg_array(text: str) -> tuple[bool, Any]:
    """(explicit bounds, nested list of element texts / ``None``) of PostgreSQL's array output.

    Raises ``ValueError`` on text that is not an array literal.
    """
    m = _ARRAY_DIMS.match(text)
    bounded = bool(m)
    if m:
        text = text[m.end():]
    pos = 0

    def parse() -> list[Any]:
        nonlocal pos
        if text[pos] != "{":
            raise ValueError("array literal expected")
        pos += 1
        items: list[Any] = []
        if text[pos] == "}":
            pos += 1
            return items
        while True:
            if text[pos] == "{":
                items.append(parse())
            elif text[pos] == '"':
                pos += 1
                buf = []
                while text[pos] != '"':
                    if text[pos] == "\\":
                        pos += 1
                    buf.append(text[pos])
                    pos += 1
                pos += 1
                items.append("".join(buf))
            else:
                end = pos
                while text[end] not in ",}":
                    end += 1
                raw = text[pos:end].strip()
                items.append(None if raw.upper() == "NULL" else raw)
                pos = end
            if text[pos] == ",":
                pos += 1
                continue
            if text[pos] == "}":
                pos += 1
                return items
            raise ValueError("array literal expected")

    try:
        parsed = parse()
    except IndexError as exc:
        raise ValueError("array literal ended early") from exc
    if pos != len(text):
        raise ValueError("array literal has trailing text")
    return bounded, parsed


def _family(ddl: str) -> str:
    from services.type_system import normalize_logical_type

    return str(normalize_logical_type(str(ddl or "")) or "")


def _element_fold(elem: str, text: str, value: Any) -> bool:
    """Whether one array element's driver value is not what its server text says."""
    kind = _scalar_kind(elem)
    if kind == TIME:
        return _time_fold(text, value, tz=False) is not None
    if kind == TIMETZ:
        return _time_fold(text, value, tz=True) is not None
    if kind == INTERVAL:
        return not isinstance(value, timedelta) or _interval_text(value) != text
    if kind == JSONB or _upper(elem) == "JSON":
        return _jsonb_fold(text, value) is not None
    if isinstance(value, bool):
        return value is not (text.lower() in {"t", "true"})
    if isinstance(value, (int, float, Decimal)):
        try:
            return _number(value) != _number(float(text) if isinstance(value, float) else Decimal(text))
        except (InvalidOperation, ValueError):
            return True
    if isinstance(value, datetime):
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return True
        return parsed != value or parsed.utcoffset() != value.utcoffset()
    if isinstance(value, date):
        return value.isoformat() != text
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(value).hex() != text.lower()
    return str(value) != text


def _array_fold(ddl: str, text: str, value: Any) -> str | None:
    if isinstance(value, str):
        return None if value == text else ARRAY_ELEMENT  # no element adapter: the driver handed the text itself
    try:
        bounded, parsed = parse_pg_array(text)
    except ValueError:
        return ARRAY_ELEMENT
    if bounded:
        return ARRAY_BOUNDS
    elem = element_ddl(ddl) or "VARCHAR"

    def walk(server: Any, driver: Any) -> bool:
        if isinstance(server, list):
            return not isinstance(driver, (list, tuple)) or len(server) != len(driver) or any(
                walk(s, d) for s, d in zip(server, driver))
        if server is None or driver is None:
            return (server is None) != (driver is None)
        return _element_fold(elem, server, driver)

    return ARRAY_ELEMENT if walk(parsed, value) else None


def driver_fold(kind: str, ddl: str, text: Any, value: Any) -> str | None:
    """Why the driver's ``value`` is not the value whose server text is ``text`` (``None``: one-to-one)."""
    if text is None or value is None:
        return None  # SQL NULL (a jsonb ``null`` document is counted by the catalog and named there)
    text = str(text)
    if kind == ARRAY:
        return _array_fold(ddl, text, value)
    if kind in {TIME, TIMETZ}:
        return _time_fold(text, value, tz=kind == TIMETZ)
    if kind == INTERVAL:
        return None if isinstance(value, timedelta) and _interval_text(value) == text else INTERVAL_FOLD
    if kind == JSONB:
        return _jsonb_fold(text, value)
    return None


# ------------------------------------------------------- the other engines


def _precision(up: str) -> int | None:
    m = re.search(r"\(\s*(\d+)\s*\)", up)
    return int(m.group(1)) if m else None


def adapter_fold(ddl: str, engine: str) -> str | None:
    """Why a declared type cannot be proven on this engine (its driver can read two values as one), else ``None``."""
    eng = str(engine or "").lower()
    up = _upper(ddl)
    base = re.sub(r"\(.*$", "", up).strip()
    if eng in {"sqlserver", "mssql"}:
        if base in {"TIME", "DATETIME2", "DATETIMEOFFSET"} and (_precision(up) is None or _precision(up) > 6):
            return (f"is {base}({_precision(up) or 7}), which keeps 100-nanosecond ticks the driver reads to the "
                    "microsecond, so two values that differ in the seventh digit read alike and it is not provable")
        if base == "SQL_VARIANT":
            return ("is SQL_VARIANT: the driver reads each value as whatever Python type its base type maps to, so "
                    "its exact value and type are not provable")
    if eng == "oracle":
        if "TIME ZONE" in up and base.startswith("TIMESTAMP"):
            return ("is a TIMESTAMP WITH TIME ZONE: the driver hands back the wall clock without the offset or "
                    "region it was stored with, so it is not provable")
        if base.startswith("TIMESTAMP") and (_precision(up) or 6) > 6:
            return ("keeps fractional seconds past the microsecond the driver reads, so two values that differ "
                    "there read alike and it is not provable")
        if base.startswith("INTERVAL"):
            return ("is an INTERVAL: the driver reads it as a plain duration (months and fractional seconds past "
                    "the microsecond are not kept), so it is not provable")
    if eng in {"mysql", "mariadb"} and base == "FLOAT" and (_precision(up) is None or _precision(up) <= 24):
        return ("is a single-precision FLOAT: the server sends it rounded to its display digits, so two stored "
                "values can read alike and it is not provable")
    return None
