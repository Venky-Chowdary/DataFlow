"""Shared snapshot-scan closer — one SELECT + fetchmany, no OFFSET pages.

SQL warehouse sources hold a cursor across stream chunks. Closing is the same
algorithm: drop the result/cursor, then the connection/engine if this scan
opened it. OFFSET pagination is O(n²) and can skip/duplicate under concurrent
writes; this is the Fivetran/Debezium-class sequential scan.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

logger = logging.getLogger(__name__)

#: Sources that stream a snapshot with one SELECT + fetchmany (no OFFSET pages).
SNAPSHOT_SCAN_SOURCES = frozenset(
    {
        "snowflake",
        "mysql",
        "postgresql",
        "redshift",
        "bigquery",
        "generic_sql",
        "sqlserver",
        "oracle",
        "databricks",
        "sqlite",
        # MongoDB: one find().sort(_id) + getmore. .skip(offset) is O(n²) and
        # drifts under concurrent inserts (same cliff as SQL OFFSET).
        "mongodb",
    }
)


#: Snapshot-scan readers that accept ``filter_column`` / ``filter_after`` and
#: push ``WHERE cursor > watermark`` into the one held SELECT. An incremental
#: run on a table with no unique tie-break must page this way: a keyset seek
#: on the cursor alone skips every peer row sharing a page-edge cursor value.
FILTERED_SCAN_SOURCES = frozenset(
    {"postgresql", "redshift", "mysql", "sqlite", "generic_sql", "sqlserver", "oracle"}
)


def scan_filter_value(filter_column: str, filter_after: Any) -> str | None:
    """Decode the watermark a filtered snapshot scan binds, or None for no filter.

    The bound is the *run* watermark (a cursor-only bookmark), never a page
    edge — the scan itself pages with ``fetchmany``. A composite bookmark
    cannot be bound against the cursor column alone and is refused here so a
    mis-decoded watermark never silently reads zero rows.
    """
    from services.keyset_pagination import (
        present_cursor_bookmark,
        split_cursor_bookmark,
    )

    if not filter_column:
        return None
    bookmark = present_cursor_bookmark(filter_after)
    if bookmark is None:
        return None
    value, _ = split_cursor_bookmark(bookmark, has_tiebreak=False)
    return value


def publish_scan_order(scan_state: dict[str, Any], order_cols: Sequence[str]) -> None:
    """Record the ORDER BY a snapshot scan actually opened with.

    The scan→keyset handoff in the stream engine seeks past the first scan page
    on the keyset columns; that is only sound when the scan was ordered by
    those same columns. Readers publish their order here so the engine can
    refuse the seek instead of skipping every row the scan order left behind.
    """
    scan_state["order_cols"] = [str(c) for c in order_cols if c]


def scan_order_supports_seek(
    scan_order: Sequence[str] | None, keyset_order_cols: Sequence[str]
) -> bool:
    """True when a keyset seek may continue a snapshot scan's first page.

    Requires the keyset columns to be a leading prefix (case-insensitive) of the
    published scan order. An unpublished order is *not* trusted: a SQLite scan
    ordered by ``rowid`` while the seek runs on a ``BIGINT PRIMARY KEY`` handed
    the engine ids 2501..22500 first and then sought ``id > 22500`` — the 2,500
    rows the heap had moved to the end were never read.
    """
    if scan_order is None:
        return False
    seek = [str(c).lower() for c in keyset_order_cols if c]
    order = [str(c).lower() for c in scan_order if c]
    if not seek or len(seek) > len(order):
        return False
    return order[: len(seek)] == seek


def fetch_scan_page(cur: Any, batch_size: int) -> list[Any]:
    """Page a held snapshot cursor.

    Production DBAPI cursors return a list from ``fetchmany``. Unit-test
    doubles that only stub ``fetchall`` return a non-sequence from
    ``fetchmany`` — or omit ``fetchmany`` entirely — so fall back in both
    cases (a valid minimal-DBAPI cursor need not implement ``fetchmany``).
    """
    try:
        raw = cur.fetchmany(max(1, int(batch_size)))
    except AttributeError:
        raw = None
    if isinstance(raw, (list, tuple)):
        return list(raw)
    raw = cur.fetchall()
    return list(raw or [])


def drop_batch_prefix(batch: Any, drop: int) -> Any:
    """Keep the tail of a scan page after a bookmark-less resume skip.

    The remaining rows are this pass's source consumption, so the raw-page
    mark is cleared and restamped by the write loop.
    """
    cut = max(0, int(drop or 0))
    rows = list(getattr(batch, "rows", None) or [])
    if cut <= 0 or batch is None:
        return batch
    batch.rows = rows[cut:]
    try:
        batch.offset = int(getattr(batch, "offset", 0) or 0) + cut
    except (TypeError, ValueError, AttributeError):
        pass
    if hasattr(batch, "raw_page_rows"):
        batch.raw_page_rows = None
        batch.raw_page_cursor = ""
        batch.raw_page_keyset = ""
        batch.raw_page_filtered = 0
    return batch


def align_snapshot_resume(probe: Any, skip_rows: int, read_next: Any) -> Any:
    """Advance a held snapshot past rows the checkpoint already committed.

    One SELECT + fetchmany, then discard the prefix. OFFSET pages would be
    O(n²) and can skip/duplicate under concurrent inserts — the same cliff
    the first-run scan exists to avoid. Seeking from the top of a keyed
    table is the other owner (keyset); this path is for a resume that
    carries a row count but no bookmark.
    """
    skip = max(0, int(skip_rows or 0))
    if skip <= 0 or probe is None:
        return probe
    skipped = 0
    batch = probe
    while batch is not None and getattr(batch, "rows", None):
        n = len(batch.rows)
        if skipped + n <= skip:
            skipped += n
            batch = read_next()
            continue
        return drop_batch_prefix(batch, skip - skipped)
    return batch


def close_table_scan(scan_state: dict[str, Any] | None) -> None:
    """Release the snapshot cursor/connection held by ``read_table_scan_batch``."""
    if not scan_state:
        return
    result = scan_state.pop("result", None)
    cur = scan_state.pop("cur", None)
    conn = scan_state.pop("conn", None)
    engine = scan_state.pop("engine", None)
    client = scan_state.pop("client", None)
    scan_state.pop("iter", None)
    scan_state.pop("rows", None)
    scan_state.pop("local_rows", None)
    scan_state.clear()
    for obj in (result, cur, conn, client):
        if obj is None:
            continue
        try:
            obj.close()
        except Exception as exc:
            logger.debug("snapshot scan close skipped: %s", exc)
    if engine is not None:
        try:
            from services.engine_pool import release_engine

            release_engine(engine)
        except Exception as exc:
            logger.debug("snapshot scan engine release skipped: %s", exc)
