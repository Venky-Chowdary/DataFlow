"""How a streaming pass records what the *reader* counted, for Gate-8.

Split out of ``stream.py`` (a god module under a size budget). Conservation is
only meaningful when the source count comes from the read side: deriving it from
what the writer acknowledged balances any short read against itself, which is
the one thing this accounting exists to prevent.

The distinction that matters here is between a source count that is **zero** and
one that is **absent**. They are not the same claim, and conflating them broke
incremental sync: a steady-state run reads nothing past its watermark — the
normal case for a schedule, not an error — and treating that as "unmeasured"
made Gate-8 refuse a run that had correctly done nothing.
"""

from __future__ import annotations

from typing import Any


def stamp_source_row_count(
    dest_summary: dict[str, Any],
    *,
    reader_count: int,
    rows_written: int,
) -> None:
    """Record the reader's population count on the summary Gate-8 reads.

    ``reader_count`` is the committed offset — the sum of every committed
    batch's source rows — which is authoritative for the whole stream, unlike a
    per-batch writer stamp that may have merged into the summary.

    A zero read is recorded as a measured zero only when the writer also
    acknowledged nothing. Zero read alongside rows written means the two counts
    disagree, and inventing conservation from the writer's acknowledgement there
    is exactly the circular balance this guards against, so it stays unmeasured.
    """
    if reader_count > 0:
        dest_summary["source_row_count"] = reader_count
        dest_summary["source_row_count_source"] = "committed_offset"
        return
    if int(rows_written or 0) == 0:
        # The read loop completed and nothing was in scope. Conservation holds
        # trivially: nothing was read and nothing was written.
        dest_summary["source_row_count"] = 0
        dest_summary["source_row_count_source"] = "committed_offset_empty"
        return
    existing = dest_summary.get("source_row_count")
    if isinstance(existing, int) and existing > 0:
        return
    dest_summary["source_row_count_source"] = "unmeasured"
    dest_summary.pop("source_row_count", None)


def stamp_incremental_no_op(dest_summary: dict[str, Any]) -> None:
    """Record an incremental pass that found nothing past its watermark.

    This is a *measured* zero — the reader ran and the answer was none — not an
    absent measurement. Leaving it unstamped made Gate-8 refuse the steady state
    of every incremental schedule, because most ticks have no new rows.
    """
    dest_summary["source_row_count"] = 0
    dest_summary["source_row_count_source"] = "incremental_watermark_empty"
