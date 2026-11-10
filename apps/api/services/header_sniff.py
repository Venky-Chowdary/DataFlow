"""Is row 1 of a tabular source a header, or the first record?

A header-less CSV export is common on SFTP drops. Taking its first line as
column names silently drops one record, and every later check (COUNT, the
Gate-8 checksum) reads through the same header rule, so the loss is proven
"matching" instead of caught (D317).

The rule is conservative, per column, in the spirit of ``csv.Sniffer``:

* a label over a typed column (``id`` above integers, ``active`` above
  ``true``/``false``) proves a header — one such column decides it;
* row 1 is data only when no column says header and at least one cell is a
  value a column name essentially never is: a boolean, a date, or a decimal or
  negative number that matches its column's type.

Plain integers are weak evidence (``2023,2024`` pivot headers exist), and an
all-text file carries no evidence at all, so both keep today's behaviour: row 1
is the header. ``has_header`` in the read options decides those explicitly.
"""

from __future__ import annotations

import datetime as _dt
import re
from decimal import Decimal
from typing import Any, Sequence

from services.tabular_rows import is_blank_row

__all__ = ["SNIFF_ROWS", "first_row_is_data"]

#: Value-bearing rows after row 1 that the decision looks at.
SNIFF_ROWS = 50

_INT = re.compile(r"^[+-]?\d+$")
_DECIMAL = re.compile(r"^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$")
_DATE = re.compile(
    r"^(\d{4}-\d{1,2}-\d{1,2}([ T]\d{1,2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?"
    r"|\d{1,2}/\d{1,2}/\d{2,4})$"
)
_BOOLS = frozenset({"true", "false"})


def _kind(cell: Any) -> tuple[str, bool] | None:
    """``(kind, strong)`` for one cell, ``None`` when it is empty.

    ``strong`` marks a value no column name realistically is.
    """
    if cell is None:
        return None
    if isinstance(cell, bool):
        return "bool", True
    if isinstance(cell, int):
        return "number", cell < 0
    if isinstance(cell, (float, Decimal)):
        return "number", True
    if isinstance(cell, (_dt.date, _dt.datetime, _dt.time)):
        return "date", True
    text = str(cell).strip()
    if not text:
        return None
    if text.casefold() in _BOOLS:
        return "bool", True
    if _INT.match(text):
        return "number", text.startswith("-")
    if _DECIMAL.match(text):
        return "number", True
    if _DATE.match(text):
        return "date", True
    return "text", False


def _column_kind(body: Sequence[Sequence[Any]], index: int) -> str | None:
    """The one typed kind every non-empty body cell has, else ``text``/``None``."""
    kinds: set[str] = set()
    for row in body:
        if index < len(row):
            found = _kind(row[index])
            if found is not None:
                kinds.add(found[0])
    if not kinds:
        return None
    return kinds.pop() if len(kinds) == 1 else "text"


def first_row_is_data(first: Sequence[Any], body: Sequence[Sequence[Any]]) -> bool:
    """True when ``first`` is a record, judged against the rows below it."""
    rows = [r for r in body if len(r) and not is_blank_row(r)]
    if not rows or not len(first) or is_blank_row(first):
        return False
    strong_data = False
    for index, cell in enumerate(first):
        found = _kind(cell)
        if found is None:
            continue
        column = _column_kind(rows, index)
        if column is None:
            continue
        kind, strong = found
        if kind == "text":
            if column != "text":
                return False  # a label over a typed column: a header
            continue
        if kind == column and strong:
            strong_data = True
    return strong_data
