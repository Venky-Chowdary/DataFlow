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


def _snapshot_low_watermark(sig: SnapshotSignal) -> dict[str, str]:
    """Resume-token fields that let ``extract_cdc_lsn`` stamp a snapshot chunk.

    Returns the log position captured immediately *before* the chunk SELECT (the
    DDD-3 low watermark), in whichever form the source engine reports it. An
    empty dict means this reader supplies no position, in which case the chunk
    stays unstamped and the destination guard degrades to plain upsert.

    MySQL preference order is deliberate: binlog ``file:pos`` first, GTID
    second. Streaming events stamp ``_df_lsn`` from ``file:pos``. A snapshot
    chunk stamped with ``gtid:…`` is a different LSN family, so
    ``compare_lsn`` returns 0 and every later change to that PK is discarded
    forever. GTID remains on the signal for the peek filter (DBZ-3577); it
    just must not become the row stamp.
    """
    lsn_low = str(getattr(sig, "lsn_low", "") or "").strip()
    if lsn_low:
        # A MySQL file:pos string stored in lsn_low — expand so extract_cdc_lsn
        # formats it the same way streaming tokens do.
        if ":" in lsn_low and not lsn_low.lower().startswith("gtid:"):
            file_name, _, pos = lsn_low.rpartition(":")
            if file_name and pos.isdigit():
                return {"file": file_name, "pos": pos}
        return {"lsn": lsn_low}
    gtid_low = str(getattr(sig, "gtid_low", "") or "").strip()
    if gtid_low:
        return {"gtid": gtid_low}
    return {}


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
        from services.cdc_snapshot_window import _pk_value

        inserts = [r for r in emitted if not r.get("__deleted")]
        deletes: list[str] = []
        for r in emitted:
            if not r.get("__deleted"):
                continue
            key = _pk_value(r, sig.primary_key or "id")
            if key:
                deletes.append(key)
        if inserts or deletes:
            window_meta: dict[str, Any] = {
                "window_id": window_id,
                "stream_overrides": stats.get("stream_overrides", 0),
                "snapshot_rows": stats.get("snapshot_rows", 0),
            }
            gtid_low = str(getattr(sig, "gtid_low", "") or "")
            gtid_high = str(getattr(sig, "gtid_high", "") or "")
            if gtid_low or gtid_high:
                window_meta["gtid_low"] = gtid_low
                window_meta["gtid_high"] = gtid_high
            resume_token: dict[str, Any] = {
                "incremental_snapshot": True,
                "signal_id": sig.id,
                "table": table,
                "last_pk": last_pk or sig.last_pk,
                "rows_snapshotted": sig.rows_snapshotted + len(rows),
                "snapshot_window": window_meta,
            }
            watermark = _snapshot_low_watermark(sig)
            if watermark:
                # Stamp the chunk with the log position observed *before* the
                # chunk SELECT ran. Without it these rows land with a NULL
                # `_df_lsn`, and every monotonic MERGE guard treats NULL as
                # "accept anything" — so an arbitrarily stale redelivered event
                # could overwrite a freshly snapshotted row. The low watermark is
                # the correct stamp: events older than it lose (they are already
                # reflected in the read), events newer than it win.
                resume_token.update(watermark)
            yield ChangeBatch(
                inserts=inserts,
                deletes=deletes,
                # Tag the owning table. Untagged batches fell through the shared
                # multi-table demux's final `return tables[0]`, so snapshot rows
                # for table B were written into table A with no error.
                table=table,
                resume_token=resume_token,
            )
        # Record progress whenever the chunk actually read rows, even if the
        # snapshot window emitted nothing (every row superseded by a stream
        # delete, say). Gating this on `inserts or deletes` left `last_pk`
        # unchanged, so the signal re-read the same range on every poll forever.
        if rows:
            mark_chunk(sig.id, last_pk=last_pk or "", rows=len(rows))
        if done or not rows:
            complete_signal(sig.id)
            chunks += 1
            continue
        chunks += 1
        # Leave signal running for the next poll cycle.
        return
