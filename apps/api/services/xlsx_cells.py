"""Typed XLSX cells for file exports — a formula is never created from data.

G7 (CWE-1236): ``ws.append(row)`` lets openpyxl guess each cell's type from
the Python value, and any text starting with ``=`` becomes a live formula
(``data_type='f'``) — ``=HYPERLINK("http://evil…")`` opened as a link, and a
harmless ``=== VIP ===`` became a broken formula. ``#N/A`` became an error cell.

Rule: every text value is an explicit string cell. Numbers, decimals, dates
and booleans are typed cells, decided by the column's export type (the
mapping/destination type the export already knows) and, without one, by the
Python type of the value. A value Excel cannot hold exactly stays exact text:
more than 15 significant digits, a non-canonical number (``0012``), a date
before 1900, or a timestamp with a time-zone offset (Excel has no zones).
"""

from __future__ import annotations

import datetime as _dt
import math
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from services.value_serializer import cell_to_string

# Excel stores numbers as IEEE-754 doubles: 15 significant digits round-trip.
_EXCEL_EXACT_DIGITS = 15
_EXCEL_MIN_YEAR = 1900

_INT_TEXT = re.compile(r"^[+-]?(?:0|[1-9]\d*)$")
_DECIMAL_TEXT = re.compile(r"^[+-]?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$")

_INTEGER_BASES = frozenset({
    "int", "integer", "smallint", "bigint", "tinyint", "mediumint", "int2", "int4",
    "int8", "int16", "int32", "int64", "long", "short", "serial", "smallserial",
    "bigserial", "uint8", "uint16", "uint32", "uint64",
})
_NUMBER_BASES = frozenset({
    "numeric", "decimal", "dec", "number", "float", "float4", "float8", "float32",
    "float64", "double", "real", "money", "smallmoney", "bignumeric",
})
_BOOLEAN_BASES = frozenset({"bool", "boolean"})
_DATE_BASES = frozenset({"date"})
_DATETIME_BASES = frozenset({
    "timestamp", "timestamptz", "datetime", "datetime2", "smalldatetime",
    "datetimeoffset", "timestamp_ntz", "timestamp_tz", "timestamp_ltz",
})
_TRUE_TEXT = frozenset({"true", "t", "yes", "y", "1"})
_FALSE_TEXT = frozenset({"false", "f", "no", "n", "0"})


def export_type_family(declared: Any) -> str:
    """``integer`` | ``number`` | ``boolean`` | ``date`` | ``datetime`` | ``""`` (text/unknown)."""
    text = str(declared or "").strip().lower()
    base = re.split(r"[\s(\[]", text, maxsplit=1)[0] if text else ""
    if base in _INTEGER_BASES:
        return "integer"
    if base in _NUMBER_BASES:
        return "number"
    if base in _BOOLEAN_BASES:
        return "boolean"
    if base in _DATE_BASES:
        return "date"
    if base in _DATETIME_BASES:
        return "datetime"
    return ""


def _significant_digits(text: str) -> int:
    mantissa = re.split(r"[eE]", text, maxsplit=1)[0]
    return len(re.sub(r"\D", "", mantissa).lstrip("0"))


def _excel_number(value: Any) -> Any:
    """The number when Excel holds it exactly, else ``None``."""
    if isinstance(value, bool):
        return None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (int, Decimal)):
        text = str(value)
        if isinstance(value, Decimal) and not value.is_finite():
            return None
        if _significant_digits(text.lstrip("+-")) > _EXCEL_EXACT_DIGITS:
            return None
        if not math.isfinite(float(value)):
            return None
        return value
    return None


def _excel_date(value: Any) -> Any:
    if isinstance(value, _dt.datetime):
        if value.tzinfo is not None or value.year < _EXCEL_MIN_YEAR:
            return None
        return value
    if isinstance(value, _dt.date):
        return value if value.year >= _EXCEL_MIN_YEAR else None
    return None


def _typed_from_python(value: Any) -> Any:
    """Python-type fallback: the typed value, or ``None`` to write exact text."""
    if isinstance(value, bool):
        return value
    number = _excel_number(value)
    if number is not None:
        return number
    return _excel_date(value)


def _typed_from_text(text: str, family: str) -> Any:
    """The typed value for ``text`` in a column of ``family``, or ``None``."""
    stripped = text.strip()
    if stripped != text or not stripped:
        return None
    if family == "integer" and _INT_TEXT.match(text):
        return _excel_number(int(text))
    if family in {"integer", "number"} and _DECIMAL_TEXT.match(text):
        try:
            return _excel_number(Decimal(text))
        except InvalidOperation:
            return None
    if family == "boolean":
        low = text.lower()
        if low in _TRUE_TEXT:
            return True
        if low in _FALSE_TEXT:
            return False
        return None
    if family in {"date", "datetime"}:
        try:
            if family == "date" and len(text) == 10:
                return _excel_date(_dt.date.fromisoformat(text))
            return _excel_date(_dt.datetime.fromisoformat(text))
        except ValueError:
            return None
    return None


def xlsx_cell_value(value: Any, declared_type: Any = None) -> tuple[Any, bool]:
    """``(value, as_text)`` for one export cell; ``as_text`` forces a string cell."""
    if value is None:
        return None, False
    if isinstance(value, str):
        family = export_type_family(declared_type)
        typed = _typed_from_text(value, family) if family else None
        return (value, True) if typed is None else (typed, False)
    typed = _typed_from_python(value)
    if typed is not None:
        return typed, False
    return cell_to_string(value), True


def fill_sheet(
    ws: Any,
    headers: list[str],
    rows: list[list[Any]],
    column_types: dict[str, str] | None = None,
) -> None:
    """Write the header and rows into ``ws`` with explicit cell types."""
    types = column_types or {}
    declared = [types.get(str(h)) for h in headers]
    for col, header in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col)
        cell.value = "" if header is None else str(header)
        cell.data_type = "s"
    for row_idx, row in enumerate(rows, start=2):
        for col, raw in enumerate(row, start=1):
            value, as_text = xlsx_cell_value(raw, declared[col - 1] if col <= len(declared) else None)
            if value is None:
                continue
            cell = ws.cell(row=row_idx, column=col)
            cell.value = value
            if as_text:
                # openpyxl binds ``=…`` as a formula and ``#N/A`` as an error:
                # data is never either.
                cell.data_type = "s"
