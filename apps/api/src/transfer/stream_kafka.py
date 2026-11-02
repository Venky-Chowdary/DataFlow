"""Kafka consumer-offset ledger for a streamed run (re-exported by src.transfer.stream)."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("src.transfer.stream")


def _kafka_accumulate_offsets(
    dest_summary: dict[str, Any], kafka_cursor: dict[str, Any]
) -> dict[str, Any]:
    """Accumulate a run's uncommitted Kafka offsets (D85).

    Merges the chunk's ``pending_offsets`` with everything already accumulated
    in ``dest_summary['kafka_offsets']`` (max offset wins per partition), stamps
    the cumulative range back on the cursor and on the job record, and returns
    the updated cursor. Nothing here commits to the broker — the engine commits
    the group offsets exactly once after the job is accepted.
    """
    merged: dict[tuple[str, int], int] = {}

    def _add(items: Any) -> None:
        for item in items or []:
            try:
                key = (str(item["topic"]), int(item["partition"]))
                merged[key] = max(merged.get(key, -1), int(item["offset"]))
            except (KeyError, TypeError, ValueError):
                continue

    prev = dest_summary.get("kafka_offsets") or {}
    _add(prev.get("pending_offsets"))
    _add(kafka_cursor.get("pending_offsets"))
    cumulative = [
        {"topic": t, "partition": p, "offset": o}
        for (t, p), o in sorted(merged.items())
    ]
    kafka_cursor = {**kafka_cursor, "pending_offsets": cumulative}
    dest_summary["kafka_offsets"] = {
        "group_id": kafka_cursor.get("group_id"),
        "topic": kafka_cursor.get("topic"),
        # Job audit: the broker position the run started from (the uncommitted
        # range carried in by a resumed run) and the end offsets committed on
        # acceptance.
        "start_offsets": prev.get("start_offsets") or prev.get("pending_offsets") or [],
        # Where this run began reading: "earliest" (a full refresh, or no
        # committed position) or "committed" (an incremental continuation).
        "start_position": prev.get("start_position") or kafka_cursor.get("start_position"),
        "pending_offsets": cumulative,
    }
    return kafka_cursor


def _kafka_failure_offset_note(
    src_type: str, dest_summary: dict[str, Any], exc: BaseException
) -> None:
    """D85 — a failed Kafka run's message must state the offset position.

    Group offsets are never committed inside a run, so on failure the broker
    position is untouched; the exception carries the pending range so the
    operator sees exactly where consumption will resume.
    """
    if src_type != "kafka":
        return
    pending = (dest_summary.get("kafka_offsets") or {}).get("pending_offsets") or []
    if not pending:
        return
    exc.args = (
        *exc.args,
        f"Kafka consumer offsets NOT committed (pending: {pending}); "
        "the next run re-reads from the last committed position.",
    )
