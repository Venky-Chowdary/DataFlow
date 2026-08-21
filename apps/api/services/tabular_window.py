"""The declared read window, applied to any row-oriented source.

Excel and CSV both carry preamble titles, a header that is not on the first
line, and a totals row at the bottom. The narrowing rule must be *one* rule:
if the CSV reader counted a formatting-only line as a skipped data row and the
Excel reader did not, then the same declaration would mean two different
populations and reconciliation would compare rows nobody read.

Every function here is streaming and bounded: the footer costs ``skip_footer``
rows of memory, never the population.
"""

from __future__ import annotations

from collections import deque
from typing import Any, Callable, Iterator, Sequence

from services.read_options import ReadOptions, ReadOptionsError
from services.tabular_rows import is_blank_row

__all__ = ["header_and_rows", "row_to_record", "synthetic_headers", "windowed_rows"]


def row_to_record(
    headers: Sequence[str],
    row: Sequence[Any],
    *,
    source_label: str = "Row",
    missing: Any = "",
) -> dict[str, Any]:
    """One record, or a refusal when the row carries cells the header cannot name.

    A short row is padded: a trailing empty field is how delimited text spells
    "no value here". A row *wider* than the header is refused, because the only
    other options are dropping a value nobody declared or inventing a column
    name mid-stream — and a dropped cell would be silent data loss that Gate-8
    would then hash as if it were the source.

    Extra cells that are all empty (a trailing delimiter, a formatting-only
    Excel column) carry nothing, so they are ignored rather than refused.
    """
    width = len(headers)
    if len(row) > width and any(
        c is not None and str(c).strip() != "" for c in row[width:]
    ):
        raise ValueError(
            f"{source_label} has {len(row)} value-bearing cells but the header "
            f"names {width} column(s); refuse silent column drop — fix the "
            "header row or the row itself"
        )
    return {
        name: (row[i] if i < len(row) else missing) for i, name in enumerate(headers)
    }


def synthetic_headers(width: int) -> list[str]:
    """Names for a headerless source — positional, so a rerun names them the same."""
    return [f"col_{i}" for i in range(max(0, width))]


def header_and_rows(
    rows: Iterator[Sequence[Any]],
    options: ReadOptions,
    *,
    header_names: Callable[[Sequence[Any]], list[str]],
    source_label: str = "Source",
) -> tuple[list[str], Iterator[Sequence[Any]]]:
    """Header names plus the data rows inside the declared window.

    Preamble rows above ``header_row`` are consumed and discarded — they are
    not records and must not reach the profiler. ``skip_rows`` and
    ``skip_footer`` count *value-bearing* rows only, matching what the operator
    sees rather than the physical line count.

    An empty source is reported as empty (``[]`` headers) so callers keep their
    own "no header row" wording. A header row the operator *named* and that
    does not exist is a refusal, because guessing another row would ingest a
    different population under an approved plan.
    """
    if options.has_header:
        explicit = options.header_row > 1
        header: Sequence[Any] | None = None
        for _ in range(options.header_row):
            header = next(rows, None)
            if header is None:
                if not explicit:
                    return [], iter(())
                raise ReadOptionsError(
                    f"{source_label} has fewer than {options.header_row} row(s); "
                    "header_row points past the end of the data"
                )
        if header is None or len(header) == 0:
            return [], iter(())
        if explicit and is_blank_row(header):
            raise ReadOptionsError(
                f"Row {options.header_row} is blank, so it cannot be the header. "
                "Point header_row at the row holding the column names, or set "
                "header_row=0 to synthesize names."
            )
        headers = header_names(header)
    else:
        # Names come from the width of the first value-bearing row, which must
        # therefore be replayed as data rather than consumed as a header.
        first = next((r for r in rows if len(r) and not is_blank_row(r)), None)
        if first is None:
            return [], iter(())
        headers = synthetic_headers(len(first))
        rows = _chain_rows(first, rows)

    return headers, windowed_rows(rows, options)


def _chain_rows(
    first: Sequence[Any], rest: Iterator[Sequence[Any]]
) -> Iterator[Sequence[Any]]:
    yield first
    yield from rest


def windowed_rows(
    rows: Iterator[Sequence[Any]], options: ReadOptions
) -> Iterator[Sequence[Any]]:
    """Value-bearing rows with the head and tail the options exclude."""
    values = (row for row in rows if not is_blank_row(row))

    if options.skip_rows:
        skipped = 0
        for _ in values:
            skipped += 1
            if skipped >= options.skip_rows:
                break
        else:
            # Fewer rows than the operator asked to skip: an empty population is
            # the honest answer, and the row counts reported everywhere agree.
            return

    if not options.skip_footer:
        yield from values
        return

    # Emit a row only once ``skip_footer`` further rows have been seen, so the
    # last N are never emitted and only N rows are ever held.
    tail: deque[Sequence[Any]] = deque(maxlen=options.skip_footer)
    for row in values:
        if len(tail) == options.skip_footer:
            yield tail[0]
        tail.append(row)
