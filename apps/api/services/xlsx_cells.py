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
before 1900, a timestamp with a time-zone offset (Excel has no zones), or a
timestamp with digits finer than milliseconds.
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

# Excel date-times carry milliseconds; anything finer is kept as exact text.
_EXCEL_FRACTION_DIGITS = 3
_FRACTION_TEXT = re.compile(r"\d{2}:\d{2}:\d{2}[.,](\d+)")

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
        # Excel holds a date-time to the millisecond: finer digits stay text.
        if value.microsecond % 1000:
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
        fraction = _FRACTION_TEXT.search(text)
        if fraction and len(fraction.group(1).rstrip("0")) > _EXCEL_FRACTION_DIGITS:
            # ``.123456`` / ``.1235`` / a DATETIME2 seventh digit: Excel would
            # round it, and fromisoformat drops a seventh digit silently.
            return None
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


_SCALED_NUMERIC = re.compile(
    r"^\s*(?:numeric|decimal|dec|number|bignumeric)\s*\(\s*\d+\s*,\s*(\d+)\s*\)", re.IGNORECASE
)


def number_format_for(declared: Any) -> str | None:
    """``0.00`` for ``numeric(12,2)``: the declared scale is shown, the value stays exact."""
    match = _SCALED_NUMERIC.match(str(declared or ""))
    if not match:
        return None
    scale = int(match.group(1))
    return "0" if scale == 0 else "0." + "0" * min(scale, 30)


def _set_cell(cell: Any, raw: Any, declared: Any, *, text: bool = False) -> None:
    """Bind ``raw`` to ``cell``; text is forced to a string cell."""
    if text:
        value, as_text = ("" if raw is None else str(raw)), True
    else:
        value, as_text = xlsx_cell_value(raw, declared)
    cell.value = value
    if not as_text and isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        fmt = number_format_for(declared)
        if fmt:
            cell.number_format = fmt
    if as_text:
        # openpyxl binds ``=…`` as a formula and ``#N/A`` as an error:
        # data is never either.
        cell.data_type = "s"


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
        _set_cell(ws.cell(row=1, column=col), header, None, text=True)
    for row_idx, row in enumerate(rows, start=2):
        for col, raw in enumerate(row, start=1):
            if raw is None:
                continue
            _set_cell(
                ws.cell(row=row_idx, column=col),
                raw,
                declared[col - 1] if col <= len(declared) else None,
            )


def write_only_row(
    ws: Any,
    values: list[Any],
    declared_types: list[Any] | None = None,
    *,
    header: bool = False,
) -> list[Any]:
    """One row of ``WriteOnlyCell`` for a write-only sheet (object-store XLSX)."""
    from openpyxl.cell import WriteOnlyCell

    declared = list(declared_types or [])
    cells: list[Any] = []
    for idx, raw in enumerate(values):
        cell = WriteOnlyCell(ws)
        if raw is not None or header:
            _set_cell(cell, raw, declared[idx] if idx < len(declared) else None, text=header)
        cells.append(cell)
    return cells
