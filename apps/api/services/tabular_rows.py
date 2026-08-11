"""Canonical "is this a record?" test for row-oriented file sources.

Spreadsheets and spreadsheet-exported CSVs carry rows that hold no value in
any field: a worksheet's used range is grown by formatting, and an exported
sheet ends in ``,,,,`` lines. Those rows are not records — loading them writes
destination rows whose every column is NULL, which is silent invention rather
than silent loss, and it makes source cardinality disagree with what was read.

CSV and Excel readers must share this single test so a row that is dropped
from the count is exactly the row that is dropped from the stream.
"""

from __future__ import annotations

from typing import Any, Iterable


def is_blank_row(values: Iterable[Any]) -> bool:
    """True when no cell in the row carries a value.

    ``0``, ``False`` and ``"0"`` are values. Only ``None`` and
    whitespace-only text count as empty.
    """
    return not any(_has_value(cell) for cell in values)


def _has_value(cell: Any) -> bool:
    if cell is None:
        return False
    if isinstance(cell, str):
        return cell.strip() != ""
    return True
