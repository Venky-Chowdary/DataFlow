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
* ``naive_datetime`` - a zoneless timestamp (and an Oracle ``DATE``, which keeps the
  time of day): the wall clock, and an extract that gained an offset differs
  (review 3: ``10:00`` is not ``10:00+05:00``);
* ``offset_time`` - a ``timetz``: the wall clock and its offset;
* ``json_document`` - a ``jsonb`` (or another parsed JSON type): the document in one
  canonical spelling that keeps each value's JSON type, so a top-level string
  ``"1"`` is not the number ``1`` and ``" a "`` is not ``"a"`` (review 3).

A value a token cannot parse is kept as its exact text, so it differs from any
parsed rendering (fail closed).
"""

from __future__ import annotations

import re
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

FLOAT, NUMERIC_TEXT, OFFSET, AWARE, NAIVE_DATETIME, OFFSET_TIME, JSON_DOCUMENT = (
    "float", "numeric_text", "offset", "aware", "naive_datetime", "offset_time", "json_document")
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
        polarity = datetime_timezone_polarity(text, dest_db=engine)
        if engine == "postgresql" and polarity in {"tz", "ltz"}:
            return AWARE
        if polarity == "ntz":
            return NAIVE_DATETIME
    if base == "JSONB" and engine in {"postgresql", "postgres"}:
        # A parsed document (the driver hands back the document itself). A PostgreSQL ``json`` keeps its
        # text and is not compared; other engines' JSON types stay on Gate-8's document comparison.
        return JSON_DOCUMENT
    return None


#: Integers a double holds exactly; beyond, a driver that parses JSON numbers as doubles has rounded.
_EXACT_INT = 2 ** 53


def _json_number(value: Any) -> str:
    text = _text(value).strip()
    try:
        if isinstance(value, int) or re.fullmatch(r"-?\d+", text):
            return str(int(text))
        return float.__repr__(float(text))
    except (TypeError, ValueError):
        return "?" + text


def _canonical(value: Any) -> Any:
    """``value`` (a parsed JSON document) with every number in one spelling and its own JSON type kept."""
    if isinstance(value, dict):
        return {str(k): _canonical(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    if value is None or isinstance(value, (bool, str)):
        return value
    return {"\u0000n": _json_number(value)}  # a number, never mistaken for the string that spells it


def parsed_document(value: Any) -> Any:
    """A JSON document handed over as text (a CSV cell) parsed; anything else as it is.

    The delimited writer spells a top-level JSON string as its bare text (``str``,
    not ``"str"``), so a cell that is not JSON text is that string.
    """
    if isinstance(value, str):
        import json

        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def ambiguous_in_text_cell(value: Any) -> bool:
    """Whether a top-level JSON string would read back as another JSON value from a delimited cell.

    The delimited writer spells the string ``"123"`` as ``123``, which reads back
    as the number: the file cannot say which it was, so such a column is named,
    never compared as if it could (review 3).
    """
    if not isinstance(value, str):
        return False
    import json

    try:
        json.loads(value)
    except ValueError:
        return False
    return True


def json_document_token(value: Any, *, parse_text: bool = False) -> str:
    import json

    doc = parsed_document(value) if parse_text else value
    return "j:" + json.dumps(_canonical(doc), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def inexact_json_number(value: Any) -> bool:
    """Whether a JSON document holds a number a double cannot hold exactly.

    A driver that parses ``jsonb`` numbers as doubles has already rounded such
    a number before anything is compared, on the source side too: the column
    cannot be proven from parsed values (it is named, never called equal).
    """
    if isinstance(value, dict):
        return any(inexact_json_number(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(inexact_json_number(v) for v in value)
    if value is None or isinstance(value, (bool, str)):
        return False
    text = _text(value).strip()
    if isinstance(value, int) or re.fullmatch(r"-?\d+", text):
        try:
            return abs(int(text)) > _EXACT_INT
        except ValueError:
            return True
    try:
        number = float(text)
    except (TypeError, ValueError):
        return True
    digits = re.sub(r"[eE].*$", "", float.__repr__(number)).replace("-", "").replace(".", "").lstrip("0")
    return abs(number) > _EXACT_INT or len(digits) >= 16


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


def token(kind: str, value: Any, *, from_text: bool = False) -> str | None:
    """``value``'s exact token for ``kind`` (``None`` stays SQL NULL).

    ``from_text`` says the value comes from a file that holds a document as
    text (a CSV cell): a JSON document is parsed before it is spelled.
    """
    if value is None:
        return None
    if kind == JSON_DOCUMENT:
        if from_text and parsed_document(value) is None:
            # A delimited cell spells a JSON null document ``null``; the source's driver hands it back as
            # None, like SQL NULL. Folded here, and the column is named by the catalog's count of them.
            return None
        return json_document_token(value, parse_text=from_text)
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
