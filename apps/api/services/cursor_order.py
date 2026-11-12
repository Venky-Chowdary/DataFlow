"""One ordering per cursor column (audit C3, H4b, M4).

An incremental read has two comparators: the source engine orders the cursor
column for the pushed-down ``cursor > watermark`` predicate, and this process
orders cursor values when it picks a page's high mark, bounds a re-read window,
or advances the stored watermark. When they disagree a row sorts on one side
of the watermark for the engine and on the other for us, and it is skipped
forever while every run reports success.

* :func:`compare_values` is a **total order**. It used to infer a type from
  the pair being compared, so ``2 < 10`` (integers), ``10 < 1a`` (strings) and
  ``1a < 2`` (strings) all held at once and a batch's maximum depended on row
  order. Each value now has one class decided by the value alone, and classes
  have a fixed rank: numbers, then dates and times, then text.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from functools import lru_cache
from typing import Any, Iterable

# Class ranks of the total order. An empty value (a missing tie-break) ranks
# below everything: a bookmark that names a key is further on than one that
# does not.
_EMPTY, _NUMBER, _TEMPORAL, _TEXT = -1, 0, 1, 2

_INT_RE = re.compile(r"^[+-]?\d+$")
_NAIVE_ISO_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?)?$"
)
_OFFSET_RE = re.compile(r"(Z|z|[+-]\d{2}(?::?\d{2})?)$")
_CALENDAR_HINT_RE = re.compile(r"\d.*[/\-:.].*\d")


def _naive_key(text: str) -> tuple[datetime, Decimal] | None:
    """A wall-clock ISO date or date-time, read as UTC so it shares the
    temporal class with offset-bearing text (one key per value)."""
    match = _NAIVE_ISO_RE.match(text)
    if match is None:
        return None
    year, month, day, hh, mi, ss, frac = match.groups()
    try:
        moment = datetime(
            int(year), int(month), int(day), int(hh or 0), int(mi or 0), int(ss or 0),
            tzinfo=timezone.utc,
        )
    except ValueError:
        return None
    digits = (frac or "").rstrip("0")
    return moment, Decimal("0." + digits) if digits else Decimal(0)


def _calendar_key(text: str) -> tuple[datetime, Decimal] | None:
    """Unambiguous non-ISO calendars (``31/12/2024``) through the write-path parser."""
    if len(text) > 40 or not _CALENDAR_HINT_RE.search(text):
        return None
    from services.transform_engine import apply_transform

    parsed, err = apply_transform(text, "datetime")
    if parsed is None or err:
        return None
    try:
        moment = datetime.fromisoformat(str(parsed).replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    whole = moment.replace(microsecond=0)
    return whole.astimezone(timezone.utc), Decimal(moment.microsecond) / Decimal(1_000_000)


@lru_cache(maxsize=8192)
def _order_key(text: str) -> tuple[int, Any]:
    if not text:
        return _EMPTY, ""
    if _INT_RE.match(text):
        return _NUMBER, Decimal(int(text))
    from connectors.cursor_bind import cursor_instant_key

    instant = cursor_instant_key(text)
    if instant is not None:
        return _TEMPORAL, instant
    naive = _naive_key(text)
    if naive is not None:
        return _TEMPORAL, naive
    from services.transform_engine import decimal_wire_value, integer_wire_value

    whole = integer_wire_value(text)
    if whole is not None:
        return _NUMBER, Decimal(whole)
    number = decimal_wire_value(text)
    if number is not None:
        try:
            number = Decimal(number)
        except ArithmeticError:
            number = None
        if number is not None and number.is_finite():
            return _NUMBER, number
    calendar = _calendar_key(text)
    if calendar is not None:
        return _TEMPORAL, calendar
    return _TEXT, text


def order_key(value: Any) -> tuple[int, Any]:
    """``(class rank, key)`` of one cursor value — a function of the value alone.

    Digits are a number, never an epoch: a 10-digit and a 13-digit integer used
    to be read as seconds and milliseconds and compared the wrong way round.
    """
    return _order_key(str(value).strip())


def compare_values(a: Any, b: Any) -> int:
    """-1 / 0 / 1 under the total order; equal keys fall back to the text."""
    ka, kb = order_key(a), order_key(b)
    if ka != kb:
        return -1 if ka < kb else 1
    return 0


def max_value(values: Iterable[Any]) -> Any:
    """Largest value under the total order (first of equals), or None."""
    best: Any = None
    best_key: tuple[int, Any] | None = None
    for value in values:
        key = order_key(value)
        if best_key is None or key > best_key:
            best, best_key = value, key
    return best
