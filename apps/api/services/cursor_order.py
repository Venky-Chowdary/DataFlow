"""One ordering per cursor column (audit C3, H4b, M4).

An incremental read has two comparators: the source engine orders the cursor
column for the pushed-down ``cursor > watermark`` predicate, and this process
orders cursor values when it picks a page's high mark, bounds a re-read window,
or advances the stored watermark. When they disagree a row sorts on one side
of the watermark for the engine and on the other for us, and it is skipped
forever while every run reports success.

Two things make them agree:

* :func:`compare_values` is a **total order**. It used to infer a type from
  the pair being compared, so ``2 < 10`` (integers), ``10 < 1a`` (strings) and
  ``1a < 2`` (strings) all held at once and a batch's maximum depended on row
  order. Each value now has one class decided by the value alone, and classes
  have a fixed rank: numbers, then dates and times, then text.
* A text column is ordered by the engine as text, whatever it holds. Text
  order equals this module's order only for some shapes
  (:func:`text_cursor_shape`); a text cursor of any other shape is refused with
  the reason (:func:`text_cursor_refusal`) rather than read on two orderings.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from functools import lru_cache
from typing import Any, Iterable

# Class ranks of the total order. An empty value (a missing tie-break) ranks
# below everything: a bookmark that names a key is further on than one that
# does not.
_EMPTY, _NUMBER, _TEMPORAL, _TEXT = -1, 0, 1, 2

#: A wire number: an optional sign, ASCII digits, an optional "." fraction and
#: an optional exponent. Nothing locale-shaped (grouping, a decimal comma, a
#: currency sign, a boolean word) is a number: the order of a value never
#: depends on the request that happens to compare it (review M1).
_NUMBER_RE = re.compile(r"^[+-]?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$")
_NAIVE_ISO_RE = re.compile(
    r"^([0-9]{4})-([0-9]{2})-([0-9]{2})"
    r"(?:[T ]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]+))?)?)?$"
)
_OFFSET_RE = re.compile(r"(Z|z|[+-]\d{2}(?::?\d{2})?)$")
_DIGITS_RE = re.compile(r"^[0-9]+$")
#: A calendar date written in any order other than ISO's year-month-day
#: (``31/12/2023``, ``12/31/2023``, ``31.12.2023``, ``31-12-2023 10:00``). As
#: text it sorts by its leading day or month, never in time order.
_CALENDAR_TEXT_RE = re.compile(
    r"^([0-9]{1,2})([/.\-])([0-9]{1,2})\2([0-9]{4}|[0-9]{2})(?:[ T][0-9].*)?$"
)


def _is_calendar_text(text: str) -> bool:
    """A day/month or month/day date (with an optional time), year last.

    Both readings are tried; a value neither reads as a calendar date (a
    version '1.10.0', a code '7-40-12') is ordinary text.
    """
    match = _CALENDAR_TEXT_RE.match(text)
    if match is None:
        return False
    first, _sep, second, _year = match.groups()
    a, b = int(first), int(second)
    return (1 <= a <= 31 and 1 <= b <= 12) or (1 <= a <= 12 and 1 <= b <= 31)


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


@lru_cache(maxsize=8192)
def _order_key(text: str) -> tuple[int, Any]:
    """Class and key of one value — read from the value alone.

    No locale, contextvar or write-path parser is consulted: numbers are wire
    numbers, temporals are ISO-8601 forms, everything else is text.
    """
    if not text:
        return _EMPTY, ""
    if _NUMBER_RE.match(text):
        try:
            number = Decimal(text)
        except ArithmeticError:
            number = None
        if number is not None and number.is_finite():
            return _NUMBER, number
        return _TEXT, text
    from connectors.cursor_bind import cursor_instant_key

    instant = cursor_instant_key(text)
    if instant is not None:
        return _TEMPORAL, instant
    naive = _naive_key(text)
    if naive is not None:
        return _TEMPORAL, naive
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


# --------------------------------------------------------------------------
# Text columns
# --------------------------------------------------------------------------

_TEXT_TYPE_WORDS = ("CHAR", "TEXT", "STRING", "CLOB", "CITEXT")


def is_text_declared(declared: Any) -> bool:
    """True for a declared column type the source engine orders as text."""
    base = " ".join(str(declared or "").upper().split()).split("(", 1)[0]
    return bool(base) and any(word in base for word in _TEXT_TYPE_WORDS)


SHAPE_TEXT = "text"
SHAPE_PADDED_NUMBER = "zero_padded_number"
SHAPE_WALLCLOCK = "wallclock_text"
SHAPE_INSTANT = "instant_text"
SHAPE_NUMBER = "number_text"
SHAPE_MIXED_OFFSETS = "instant_text_mixed_offsets"
SHAPE_MIXED = "mixed"
SHAPE_EMPTY = "empty"
#: Dates written day/month or month/day: text order is not time order.
SHAPE_CALENDAR_TEXT = "non_iso_date_text"
#: ISO wall-clock text in more than one layout (separator, fraction width, or
#: date-only beside date-time): text order is not time order across layouts.
SHAPE_MIXED_LAYOUTS = "wallclock_text_mixed_layouts"

#: Shapes for which the engine's text order is this module's order.
ORDERABLE_TEXT_SHAPES = frozenset(
    {SHAPE_TEXT, SHAPE_PADDED_NUMBER, SHAPE_WALLCLOCK, SHAPE_INSTANT, SHAPE_EMPTY}
)


@dataclass(frozen=True)
class TextCursorShape:
    """What a text cursor column was observed to hold."""

    kind: str
    #: Offset spelling shared by every value of an ``instant_text`` column.
    offset: str = ""
    #: Two values that show why the shape is not orderable ("" when it is).
    low: str = ""
    high: str = ""

    @property
    def orderable(self) -> bool:
        return self.kind in ORDERABLE_TEXT_SHAPES


def _layout_of(text: str, any_separator: bool = False) -> str:
    """The layout of a value: every digit replaced by ``_``, separators kept.

    ``any_separator``: the date/time separator (position 10) matches either
    'T' or a space.
    """
    layout = re.sub(r"[0-9]", "_", text)
    if any_separator and len(layout) > 10 and layout[10] in "T ":
        layout = layout[:10] + "_" + layout[11:]
    return layout


def _split_fraction(layout: str) -> tuple[str, int]:
    """``(layout without its fraction, fraction width)`` of a wall-clock layout."""
    if len(layout) > 19 and layout[19] == ".":
        return layout[:19], len(layout) - 20
    return layout, 0


def wallclock_like_patterns(
    shape: TextCursorShape, values: Iterable[str], *, any_separator: bool = False
) -> tuple[str, ...]:
    """``LIKE`` patterns one wall-clock layout allows: its base, and with its fraction width."""
    if shape.kind != SHAPE_WALLCLOCK:
        return ()
    base, width = "", 0
    for value in values:
        b, w = _split_fraction(_layout_of(str(value), any_separator))
        base, width = b, max(width, w)
    if not base:
        return ()
    escaped = base.replace("\\", "\\\\").replace("%", "\\%")
    return (escaped,) + ((escaped + "." + "_" * width,) if width else ())


def _offset_of(text: str) -> str:
    match = _OFFSET_RE.search(text)
    return match.group(1) if match else ""


def text_cursor_shape(values: Iterable[Any], *, separator_normalized: bool = False) -> TextCursorShape:
    """Classify the values of a text cursor column by how text order treats them.

    ``separator_normalized``: the source orders ISO date-times with their 'T'
    rewritten to a space (SQLite routes read through ``sqlite_cursor_sort_expr``),
    so the separator is not part of a value's layout there.
    """
    texts = sorted({str(v).strip() for v in values if v is not None and str(v).strip() != ""})
    if not texts:
        return TextCursorShape(SHAPE_EMPTY)
    calendar = [t for t in texts if order_key(t)[0] == _TEXT and _is_calendar_text(t)]
    if calendar:
        # Never read as dates (one order per value), and never sorted as dates
        # by the source either: refused rather than read on text order.
        return TextCursorShape(SHAPE_CALENDAR_TEXT, low=calendar[0], high=calendar[-1])
    ranks = {order_key(t)[0] for t in texts}
    if len(ranks) > 1:
        # Values of more than one kind. Text order and this module's order may
        # still agree on them ('100' < 'A1' both ways); the shape is refused
        # only on a pair they rank differently.
        ordered = sorted(texts, key=order_key)
        for left, right in zip(ordered, ordered[1:]):
            if left > right:
                return TextCursorShape(SHAPE_MIXED, low=left, high=right)
        return TextCursorShape(SHAPE_TEXT)
    rank = next(iter(ranks))
    if rank == _TEXT:
        return TextCursorShape(SHAPE_TEXT)
    ordered = sorted(texts, key=lambda t: order_key(t)[1])
    # The first pair text order ranks the other way round, else the extremes.
    low, high = next(
        ((left, right) for left, right in zip(ordered, ordered[1:]) if left > right),
        (ordered[0], ordered[-1]),
    )
    if rank == _NUMBER:
        if all(_DIGITS_RE.match(t) for t in texts) and len({len(t) for t in texts}) == 1:
            # One width of plain digits: text order is numeric order. A single
            # value proves no width, so it is accepted on what it shows.
            return TextCursorShape(SHAPE_PADDED_NUMBER)
        # Even where numeric and text order agree on what was seen, they part
        # at the next width: '9' then '10'.
        return TextCursorShape(SHAPE_NUMBER, low=low, high=high)
    from connectors.cursor_bind import cursor_instant_key

    aware = [t for t in texts if cursor_instant_key(t) is not None]
    if not aware:
        # One ISO layout only: 'T' sorts after ' ', a longer fraction after a
        # shorter one, and a date-only value before the date-times of its day,
        # so two layouts interleave differently as text than in time.
        # A value without a fraction beside one fraction width is safe: it is
        # a prefix of the same second and sorts first, and fraction digits
        # compare left to right (Python's isoformat drops a zero fraction).
        bases: dict[str, str] = {}
        widths: dict[int, str] = {}
        for t in texts:
            base, width = _split_fraction(_layout_of(t, separator_normalized))
            bases.setdefault(base, t)
            widths.setdefault(width, t)
        if len(bases) > 1 or len(set(widths) - {0}) > 1:
            pair = list(bases.values())[:2] if len(bases) > 1 else [
                widths[w] for w in sorted(set(widths) - {0})[:2]
            ]
            return TextCursorShape(SHAPE_MIXED_LAYOUTS, low=min(pair), high=max(pair))
        return TextCursorShape(SHAPE_WALLCLOCK)
    offsets = {_offset_of(t) for t in aware}
    if len(aware) == len(texts) and len(offsets) == 1 and len({len(t) for t in texts}) == 1:
        return TextCursorShape(SHAPE_INSTANT, offset=next(iter(offsets)))
    return TextCursorShape(SHAPE_MIXED_OFFSETS, low=low, high=high)


def text_cursor_refusal(column: str, shape: TextCursorShape) -> str:
    """Why a text cursor of this shape cannot bound an incremental read, or ``""``."""
    if shape.orderable:
        return ""
    lead = (
        f"Cursor '{column}' is a text column, so the source orders it as text"
    )
    remedy = (
        " Use a column the source types as a number or a timestamp (or cast it in a "
        "view), or run this sync as a full refresh."
    )
    if shape.kind == SHAPE_NUMBER:
        return (
            f"{lead}, but it holds numbers: as text '{shape.high or '10'}' sorts before "
            f"'{shape.low or '9'}', so an incremental read past the watermark would skip "
            "rows without an error. Zero-padded values of one width are the only numbers "
            "text order keeps in sequence." + remedy
        )
    if shape.kind == SHAPE_CALENDAR_TEXT:
        return (
            f"{lead}, but it holds dates written day/month or month/day ('{shape.low}'"
            + (f", '{shape.high}'" if shape.high and shape.high != shape.low else "")
            + "): as text they sort by their leading day or month, not by date, so an "
            "incremental read past the watermark would skip rows without an error. Only "
            "ISO-8601 dates (YYYY-MM-DD, in one layout) sort in time order as text."
            + remedy
        )
    if shape.kind == SHAPE_MIXED_LAYOUTS:
        return (
            f"{lead}, but it holds date-times in more than one layout ('{shape.low}' and "
            f"'{shape.high}'): a 'T' or a space between date and time, fractions of "
            "different widths, or a date beside date-times sort differently as text than "
            "in time, so an incremental read past the watermark would skip rows without "
            "an error. Only one ISO layout (one separator, one fraction width) keeps text "
            "order in time order." + remedy
        )
    if shape.kind == SHAPE_MIXED_OFFSETS:
        return (
            f"{lead}, but it holds timestamps written with more than one UTC offset or "
            f"precision: '{shape.high}' is a later moment than '{shape.low}' and sorts "
            "before it as text, so an incremental read past the watermark would skip rows "
            "without an error. Timestamps in one offset and one precision are the only "
            "ones text order keeps in time order." + remedy
        )
    return (
        f"{lead}, but its values are of more than one kind: as text '{shape.high}' "
        f"sorts before '{shape.low}', while the sync ranks it after. There is no single "
        "order the source and the sync both apply to them, so an incremental read could "
        "skip rows without an error." + remedy
    )


# --------------------------------------------------------------------------
# Observing a text cursor on the live source
# --------------------------------------------------------------------------

_SAMPLE_ROWS = 200


def like_pattern_for(shape: TextCursorShape, example: str, *, any_separator: bool = False) -> str:
    """``LIKE`` pattern every value of this shape matches, or ``""``.

    A zero-padded number or a one-offset timestamp is a fixed width (and a
    fixed suffix); a wall clock is one ISO layout, separators included. Any
    row outside the pattern is a row text order misplaces.
    """
    width = len(example)
    if shape.kind == SHAPE_PADDED_NUMBER and width:
        return "_" * width
    if shape.kind == SHAPE_WALLCLOCK and width:
        patterns = wallclock_like_patterns(shape, [example], any_separator=any_separator)
        return patterns[-1] if patterns else ""
    if shape.kind == SHAPE_INSTANT and width > len(shape.offset):
        suffix = shape.offset.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        return "_" * (width - len(shape.offset)) + suffix
    return ""


def probe_text_cursor(
    cfg: dict[str, Any],
    table: str,
    column: str,
    *,
    utc_watermark: bool = False,
) -> str:
    """Why this source's text cursor cannot bound an incremental read, or ``""``.

    Whether the column is ordered as text is the source's fact, not the sync's
    inferred type: its declared catalog type, or — on SQLite, where a column
    of any declared type stores and compares text as text — the values
    themselves. The shape is judged on a sample of the column with its own MIN
    and MAX; a shape that is orderable only at one width (zero-padded numbers,
    one-offset timestamps) is then checked on every row with one ``LIKE``.

    ``utc_watermark``: the sync keeps this route's watermark as a UTC instant
    (it types the column zone-bearing), so text timestamps must be UTC too.

    Raises when the source cannot be read: an unmeasured cursor is the
    caller's to report, not a pass.
    """
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table
    from services.sql_object_identity import resolve_object_identity

    engine = _engine(cfg)
    schema, bare = split_qualified_table(table, (cfg.get("schema") or "").strip() or None)
    ident = resolve_object_identity(engine, bare, schema, columns=[column])
    if not (ident.resolved and ident.exists) or column not in ident.columns:
        return ""
    stored = str(ident.columns[column])
    by_storage = engine.dialect.name == "sqlite"
    if not by_storage:
        declared = next(
            (
                str(c.get("type"))
                for c in sa.inspect(engine).get_columns(ident.table, schema=ident.schema or None)
                if str(c.get("name")) == stored
            ),
            "",
        )
        if not is_text_declared(declared):
            return ""
    tbl = sa.table(
        sa.sql.quoted_name(ident.table, True),
        schema=sa.sql.quoted_name(ident.schema, True) if ident.schema else None,
    )
    col = sa.column(sa.sql.quoted_name(stored, True))
    with engine.connect() as conn:
        values = [
            r[0]
            for r in conn.execute(
                sa.select(col).select_from(tbl).where(col.is_not(None)).limit(_SAMPLE_ROWS)
            )
        ]
        low_high = conn.execute(sa.select(sa.func.min(col), sa.func.max(col)).select_from(tbl)).first()
        values.extend(v for v in (low_high or ()) if v is not None)
        values = [v.strip() for v in values if isinstance(v, str) and v.strip()]
        if not values:
            return ""
        shape = text_cursor_shape(values, separator_normalized=by_storage)
        reason = text_cursor_refusal(column, shape)
        if reason:
            return reason
        if utc_watermark and shape.kind == SHAPE_INSTANT and not _is_utc_offset(shape.offset):
            return (
                f"Cursor '{column}' holds timestamps as text, all written with the "
                f"offset '{shape.offset}'. The source orders them as text while the sync "
                "keeps the watermark in UTC, so the two would compare different clock "
                "times and an incremental read could skip rows without an error. Store "
                "the timestamps in UTC, use a column the source types as a timestamp, "
                "or run this sync as a full refresh."
            )
        # Confirm the shape on every row: rows outside the allowed pattern(s)
        # are fetched one at a time and the shape judged again with them (a
        # wall clock may show its fraction width only in a later row).
        for _round in range(3):
            patterns = (
                wallclock_like_patterns(shape, values, any_separator=by_storage)
                if shape.kind == SHAPE_WALLCLOCK
                else tuple(p for p in [like_pattern_for(shape, values[0])] if p)
            )
            if not patterns:
                return ""
            odd = conn.execute(
                sa.select(col)
                .select_from(tbl)
                .where(col.is_not(None), *(col.not_like(p, escape="\\") for p in patterns))
                .limit(1)
            ).first()
            if odd is None:
                return ""
            values = [*values, str(odd[0]).strip()]
            shape = text_cursor_shape(values, separator_normalized=by_storage)
            reason = text_cursor_refusal(column, shape)
            if reason:
                return reason
    return ""


def _is_utc_offset(offset: str) -> bool:
    return offset in ("Z", "z") or offset.lstrip("+-").replace(":", "").strip("0") == ""
