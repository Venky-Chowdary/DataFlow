"""Execute Debezium-style incremental snapshot chunks during CDC poll.

While streaming, claim a pending signal and emit INSERT ChangeBatches for
PK-ordered chunks until the table is exhausted, then mark the signal complete.

Uses DDD-3 snapshot windows: when ``stream_events_during_chunk`` is provided,
live events for the same PK replace snapshot READ rows before emit
(stream-wins). CDC continues between chunks (at-least-once; destination upserts).
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Callable, Iterator, Optional

from services.cdc_engine import ChangeBatch
from services.cdc_incremental_snapshot import (
    SnapshotSignal,
    claim_next_signal,
    complete_signal,
    mark_chunk,
    update_signal,
)
from services.cdc_snapshot_window import SnapshotWindow

logger = logging.getLogger(__name__)

RowFetcher = Callable[[SnapshotSignal], tuple[list[dict[str, Any]], Optional[str], bool]]
StreamDuringChunk = Callable[[SnapshotSignal], list[dict[str, Any]]]


def interleave_incremental_snapshot(
    source_key: str,
    *,
    table: str,
    fetch_chunk: RowFetcher,
    max_chunks_per_poll: int = 1,
    stream_events_during_chunk: StreamDuringChunk | None = None,
) -> Iterator[ChangeBatch]:
    """Yield snapshot chunks for at most ``max_chunks_per_poll`` claimed signals.

    ``fetch_chunk(signal)`` must return ``(rows, last_pk_or_none, done)``.
    Optional ``stream_events_during_chunk`` returns live events seen while the
    chunk SELECT ran (op + row / pk) for DDD-3 stream-wins collision resolution.
    """
    chunks = 0
    while chunks < max(1, int(max_chunks_per_poll)):
        sig = claim_next_signal(source_key, table=table)
        if sig is None:
            return
        window_id = f"{sig.id}:{uuid.uuid4().hex[:8]}"
        win = SnapshotWindow(window_id=window_id, primary_key=sig.primary_key or "id")
        try:
            win.open_window()
            rows, last_pk, done = fetch_chunk(sig)
            win.add_snapshot_rows(rows)
            stream_events: list[dict[str, Any]] = []
            if stream_events_during_chunk is not None:
                try:
                    stream_events = list(stream_events_during_chunk(sig) or [])
                except Exception as exc:
                    logger.warning(
                        "Stream peek during snapshot window failed for %s.%s: %s",
                        source_key,
                        table,
                        exc,
                    )
            for ev in stream_events:
                op = str(ev.get("op") or ev.get("__op") or "u")
                row = ev.get("row") if isinstance(ev.get("row"), dict) else {
                    k: v for k, v in ev.items() if k not in {"op", "__op", "row", "pk"}
                }
                win.apply_stream_event(op=op, row=row, pk=ev.get("pk"))
            emitted = win.close_window()
            stats = win.stats()
        except Exception as exc:
            logger.warning("Incremental snapshot chunk failed for %s.%s: %s", source_key, table, exc)
            update_signal(sig.id, status="failed", error=str(exc)[:500])
            return
        inserts = [r for r in emitted if not r.get("__deleted")]
        deletes = [str(r.get(sig.primary_key or "id")) for r in emitted if r.get("__deleted")]
        if inserts or deletes:
            yield ChangeBatch(
                inserts=inserts,
                deletes=deletes,
                resume_token={
                    "incremental_snapshot": True,
                    "signal_id": sig.id,
                    "table": table,
                    "last_pk": last_pk or sig.last_pk,
                    "rows_snapshotted": sig.rows_snapshotted + len(rows),
                    "snapshot_window": {
                        "window_id": window_id,
                        "stream_overrides": stats.get("stream_overrides", 0),
                        "snapshot_rows": stats.get("snapshot_rows", 0),
                    },
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
