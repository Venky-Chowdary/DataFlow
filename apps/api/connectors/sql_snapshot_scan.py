"""Shared snapshot-scan closer — one SELECT + fetchmany, no OFFSET pages.

SQL warehouse sources hold a cursor across stream chunks. Closing is the same
algorithm: drop the result/cursor, then the connection/engine if this scan
opened it. OFFSET pagination is O(n²) and can skip/duplicate under concurrent
writes; this is the Fivetran/Debezium-class sequential scan.
"""

from __future__ import annotations

import logging
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
