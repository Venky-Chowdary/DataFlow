"""Honest transfer progress + throttled Mongo status writes."""

from __future__ import annotations

import time
from typing import Callable


# Phase floors/caps — writing uses row ratio inside the write band only.
_PHASE_READING = 2
_PHASE_PREFLIGHT = 5
_PHASE_WRITE_FLOOR = 5
_PHASE_WRITE_CEILING = 98
_PHASE_RECONCILE = 99
_PHASE_COMPLETE = 100


def compute_transfer_progress_pct(
    *,
    phase: str = "writing",
    rows_processed: int = 0,
    total_rows: int | None = 0,
    chunk: int = 0,
    chunks: int = 0,
) -> int | None:
    """Return an honest 0–100 progress percentage.

    Writing progress is ``rows_processed / total_rows`` (never chunk theater).
    Returns ``None`` when the work has no known denominator (CDC / continuous)
    so callers can omit a misleading percentage.
    """
    phase_l = (phase or "writing").strip().lower()
    if phase_l in {"complete", "completed", "completed_with_quarantine", "success"}:
        return _PHASE_COMPLETE
    if phase_l in {"failed", "cancelled", "canceled"}:
        return None
    if phase_l in {"reading"}:
        return _PHASE_READING
    if phase_l in {"preflight", "quality_check", "mapping"}:
        return _PHASE_PREFLIGHT
    if phase_l in {"reconcile", "reconciling", "verification"}:
        return _PHASE_RECONCILE

    total = int(total_rows or 0)
    rows = max(0, int(rows_processed or 0))
    if total > 0:
        # Exact row ratio inside the write band; leave 99 for reconcile, 100 for done.
        ratio = min(1.0, rows / total)
        pct = int(_PHASE_WRITE_FLOOR + ratio * (_PHASE_WRITE_CEILING - _PHASE_WRITE_FLOOR))
        if rows >= total:
            pct = _PHASE_WRITE_CEILING
        return max(_PHASE_WRITE_FLOOR, min(_PHASE_WRITE_CEILING, pct))

    # Unknown total: never invent a high % from chunk index (that caused 90% stalls).
    # Only report a tiny start signal on the first chunk so the bar isn't stuck at 0.
    if chunk <= 0:
        return _PHASE_WRITE_FLOOR
    return None


def schema_policy_implies_backfill(schema_policy: str | None) -> bool:
    """propagate_* schema policies require additive destination columns."""
    return (schema_policy or "").strip().lower() in {
        "propagate_columns",
        "propagate_all",
    }


def effective_backfill_new_fields(
    *,
    backfill_new_fields: bool = False,
    schema_policy: str | None = None,
) -> bool:
    """Honor explicit backfill toggle or propagate schema policies."""
    return bool(backfill_new_fields) or schema_policy_implies_backfill(schema_policy)


class ThrottledCheckpoint:
    """Limit MongoDB job status writes during batched transfers."""

    def __init__(
        self,
        callback: Callable[..., None],
        *,
        min_interval_sec: float = 1.0,
    ) -> None:
        self._callback = callback
        self._min_interval = min_interval_sec
        self._last_at = 0.0
        self._last_rows = -1

    def __call__(self, chunk: int, chunks: int, rows: int, checkpoint: dict | None = None) -> None:
        now = time.time()
        rows_i = int(rows or 0)
        # Always flush first/last chunk, meaningful row advances, or interval.
        row_advanced = rows_i != self._last_rows and (
            self._last_rows < 0 or rows_i - self._last_rows >= 1
        )
        if (
            chunk <= 1
            or chunk >= chunks
            or now - self._last_at >= self._min_interval
            or (row_advanced and now - self._last_at >= 0.4)
        ):
            self._last_at = now
            self._last_rows = rows_i
            self._callback(chunk, chunks, rows, checkpoint=checkpoint)
