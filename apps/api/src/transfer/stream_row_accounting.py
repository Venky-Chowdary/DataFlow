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


def begin_table_population(checkpoint: Any) -> None:
    """Start a fresh population count for one table of a sequential multi-stream job.

    The job checkpoint is shared across streams so a crash can resume the job,
    but row offset / keyset / quarantine ledgers are *per table*. Leaving the
    parent's offset in place made the child start at that count: Gate-8 then
    compared two tables' source rows to the last table's destination.
    """
    if checkpoint is None:
        return
    checkpoint.offset = 0
    checkpoint.chunk_index = 0
    checkpoint.rows_processed = 0
    checkpoint.file_offset = 0
    checkpoint.cursor_value = None
    checkpoint.cursor_column = ""
    checkpoint.dynamodb_cursor = None
    checkpoint.es_search_after = None
    checkpoint.redis_scan_state = None
    checkpoint.kafka_cursor = None
    checkpoint.rejected_rows = 0
    checkpoint.coerced_null_rows = 0
    checkpoint.rejected_details = []
    checkpoint.rejected_details_truncated = 0


def stamp_incremental_no_op(dest_summary: dict[str, Any]) -> None:
    """Record an incremental pass that found nothing past its watermark.

    This is a *measured* zero — the reader ran and the answer was none — not an
    absent measurement. Leaving it unstamped made Gate-8 refuse the steady state
    of every incremental schedule, because most ticks have no new rows.
    """
    dest_summary["source_row_count"] = 0
    dest_summary["source_row_count_source"] = "incremental_watermark_empty"


def _raw_page_marked(batch: Any) -> bool:
    """True when this page already recorded how the source handed it over.

    Rewriting a page (source filter, shaping recipe) is not idempotent, and the
    first page is prepared twice — its DDL is committed before any worker starts.
    The mark is what proves a page is rewritten exactly once.
    """
    if batch is None:
        return False
    try:
        return batch.raw_page_rows is not None
    except AttributeError:
        return False


def _mark_raw_page(batch: Any, rows: int, cursor: str, keyset: str) -> bool:
    """Record the page as the source handed it over; False if it cannot hold it."""
    try:
        batch.raw_page_rows = int(rows)
        batch.raw_page_cursor = str(cursor or "")
        batch.raw_page_keyset = str(keyset or "")
    except AttributeError:
        return False
    return True


def _raw_page_rows(batch: Any) -> int:
    """How many rows the source handed over for this page.

    Unmarked pages were never rewritten, so their surviving rows *are* the page.
    """
    if batch is None:
        return 0
    try:
        marked = batch.raw_page_rows
    except AttributeError:
        marked = None
    if marked is None:
        return len(batch.rows or [])
    return int(marked)


def _raw_page_filtered(batch: Any) -> int:
    """Rows the declared source filter removed from this page (0 if none)."""
    if batch is None:
        return 0
    try:
        return int(batch.raw_page_filtered or 0)
    except AttributeError:
        return 0


def _raw_page_cursor_bounded(batch: Any) -> int:
    """Rows this page held that the incremental cursor bound excluded (0 if none).

    A source that cannot push the bound into its read call hands the whole
    keyspace over, so the bound is applied to the page. Those rows are outside
    this run's read scope — not a removal charged to a filter or a recipe.
    """
    if batch is None:
        return 0
    try:
        return int(batch.raw_page_cursor_bounded or 0)
    except AttributeError:
        return 0


def _raw_page_cursor(batch: Any) -> str:
    """The page's highest cursor value before it was rewritten ("" if unmarked)."""
    if batch is None:
        return ""
    try:
        return str(batch.raw_page_cursor or "")
    except AttributeError:
        return ""


def _raw_page_keyset(batch: Any) -> str:
    """The page's keyset bookmark before it was rewritten ("" if unmarked)."""
    if batch is None:
        return ""
    try:
        return str(batch.raw_page_keyset or "")
    except AttributeError:
        return ""
