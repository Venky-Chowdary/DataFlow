"""Execute Debezium-style incremental snapshot chunks during CDC poll.

While streaming, claim a pending signal and emit INSERT ChangeBatches for
PK-ordered chunks until the table is exhausted, then mark the signal complete.
CDC events continue to flow between chunks (at-least-once; destination upserts).
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Iterator

from services.cdc_engine import ChangeBatch
from services.cdc_incremental_snapshot import (
    SnapshotSignal,
    claim_next_signal,
    complete_signal,
    mark_chunk,
    update_signal,
)

logger = logging.getLogger(__name__)

RowFetcher = Callable[[SnapshotSignal], tuple[list[dict[str, Any]], str | None, bool]]


def interleave_incremental_snapshot(
    source_key: str,
    *,
    table: str,
    fetch_chunk: RowFetcher,
    max_chunks_per_poll: int = 1,
) -> Iterator[ChangeBatch]:
    """Yield snapshot chunks for at most ``max_chunks_per_poll`` claimed signals.

    ``fetch_chunk(signal)`` must return ``(rows, last_pk_or_none, done)``.
    """
    chunks = 0
    while chunks < max(1, int(max_chunks_per_poll)):
        sig = claim_next_signal(source_key, table=table)
        if sig is None:
            return
        try:
            rows, last_pk, done = fetch_chunk(sig)
        except Exception as exc:
            logger.warning("Incremental snapshot chunk failed for %s.%s: %s", source_key, table, exc)
            update_signal(sig.id, status="failed", error=str(exc)[:500])
            return
        if rows:
            yield ChangeBatch(
                inserts=rows,
                resume_token={
                    "incremental_snapshot": True,
                    "signal_id": sig.id,
                    "table": table,
                    "last_pk": last_pk or sig.last_pk,
                    "rows_snapshotted": sig.rows_snapshotted + len(rows),
                },
            )
            mark_chunk(sig.id, last_pk=last_pk or "", rows=len(rows))
        if done or not rows:
            complete_signal(sig.id)
            chunks += 1
            continue
        chunks += 1
        # Leave signal running for the next poll cycle.
        return
