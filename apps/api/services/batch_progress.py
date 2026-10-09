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


def row_count_label(total_rows: int | None) -> str:
    """Operator-facing row count, or ``"an unknown number of"`` when unmeasured.

    Sources with no cheap cardinality — a DynamoDB Scan, a Kafka topic, a search
    index — report ``None`` rather than inventing a total. Progress messages
    formatted that with ``{total_rows:,}``, which raises on ``None``, so an
    honest "we do not know yet" killed the transfer before its first batch.
    """
    if total_rows is None:
        return "an unknown number of"
    return f"{int(total_rows):,}"


def selected_contract_names(stream_contracts: list | None) -> list[str]:
    """Selected stream names, in contract order, with blanks dropped."""
    names: list[str] = []
    for raw in stream_contracts or []:
        if not isinstance(raw, dict) or raw.get("selected", True) is False:
            continue
        name = str(raw.get("name") or raw.get("stream") or "").strip()
        if name and name not in names:
            names.append(name)
    return names


def opening_analysis_message(stream_contracts: list | None = None) -> str:
    """The pre-write peek is the restored endpoint when several tables are selected."""
    if len(selected_contract_names(stream_contracts)) >= 2:
        return "Analyzing the restored endpoint…"
    return "Analyzing source table…"


def opening_batch_message(
    total_rows: int | None,
    stream_contracts: list | None = None,
) -> str:
    """Row count on the opening write line.

    For one table that count is the transfer. For several tables the peek is
    the restored endpoint, and each table is written on its own afterwards.
    """
    label = row_count_label(total_rows)
    count = len(selected_contract_names(stream_contracts))
    if count >= 2:
        return (
            f"Streaming {label} rows on the restored endpoint, "
            f"then each of {count} tables…"
        )
    return f"Streaming {label} rows in batches…"


def batch_write_message(
    chunk: int,
    chunks: int,
    rows: int,
    *,
    is_cdc: bool = False,
    checkpoint: dict | None = None,
    stream_contracts: list | None = None,
) -> str:
    """Name the table on a multi-table batch so identical counts stay distinct.

    Two tables of 2 rows otherwise emit the same sentence. The event log keeps
    a line only when the message changes, so the second table disappeared.
    """
    stream = ""
    if isinstance(checkpoint, dict):
        stream = str(checkpoint.get("cdc_stream") or "").strip()
    multi = len(selected_contract_names(stream_contracts)) >= 2 and bool(stream)
    if is_cdc:
        base = f"CDC applied {int(rows):,} change(s)"
    else:
        base = f"Writing batch {int(chunk)}/{int(chunks)} ({int(rows):,} rows)"
    if multi:
        return f"{base} on {stream}…"
    return f"{base}…"


def schema_policy_implies_backfill(schema_policy: str | None) -> bool:
    """propagate_* schema policies require additive destination columns."""
    return (schema_policy or "").strip().lower() in {
        "propagate_columns",
        "propagate_all",
    }


def mappings_require_new_columns(mappings: list | None) -> bool:
    """True when any mapping intentionally proposes ADD COLUMN / create-new DDL."""
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        if m.get("create_new"):
            return True
        strategy = str(m.get("assignment_strategy") or "")
        if strategy in {"create_compatible_new", "identity_passthrough"}:
            # identity_passthrough on an existing dest is not ADD — only create_compatible_new
            # always means a new physical column. identity on empty dest is CREATE TABLE.
            if strategy == "create_compatible_new":
                return True
    return False


def effective_backfill_new_fields(
    *,
    backfill_new_fields: bool = False,
    schema_policy: str | None = None,
    mappings: list | None = None,
) -> bool:
    """Honor explicit backfill toggle, propagate schema policies, or create_new maps.

    create_compatible_new mappings (e.g. Mongo ObjectId → new VARCHAR beside DECIMAL id)
    must ADD COLUMN on an existing destination — otherwise Snowflake fails with
    invalid identifier on names like id_text.
    """
    return (
        bool(backfill_new_fields)
        or schema_policy_implies_backfill(schema_policy)
        or mappings_require_new_columns(mappings)
    )


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
