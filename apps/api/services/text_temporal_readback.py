"""Read-back of temporal values a text column holds (D225).

SQLite has no temporal storage class, so DataFlow's own DDL lands an Oracle
``DATE`` / ``TIMESTAMP`` (or any engine's datetime) in ``TEXT`` and writes the
ISO wire ``2026-10-01T00:00:00``. Sample inference names a column of such
cells ``DATE`` when every sampled cell is at midnight, because Excel serializes
a date-only cell that way. On a database read that guess is wrong: the cell
states a time of day, and the column DataFlow created holds one.

Reading it back as ``DATE`` made run 2 of a recurring sync grade the source's
``TIMESTAMP`` against a date-only destination it never had — "lossy / fidelity
collapse" on the table DataFlow itself created, with nothing to accept.
"""

from __future__ import annotations

import re
from typing import Any, Sequence

#: A calendar date followed by a clock reading (``T`` or space separator).
_DATE_WITH_CLOCK = re.compile(r"^\s*\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?")


def time_of_day_over_date(inferred: str, values: Sequence[Any]) -> str:
    """``TIMESTAMP`` when a text column sampled as ``DATE`` stores clock readings.

    Only the date-only verdict is revisited: a cell that writes ``HH:MM`` keeps
    a time of day whatever its value, so the column is a timestamp carrier. A
    column of bare ``YYYY-MM-DD`` cells stays ``DATE``.
    """
    if (inferred or "").strip().upper() != "DATE":
        return inferred
    for value in values:
        if isinstance(value, (bytes, bytearray)) or value is None:
            continue
        if _DATE_WITH_CLOCK.match(str(value)):
            return "TIMESTAMP"
    return inferred
