"""Debezium DDD-3 snapshot window buffer (stream-wins PK collision).

Algorithm (incremental snapshot chunk while WAL streams):

1. ``open_window(window_id)`` — begin buffering snapshot READ rows by PK.
2. Snapshot chunk rows enter the buffer as op=``r`` (read).
3. Live stream events for the same PK during the open window **replace** the
   buffered snapshot row (stream wins — newer than the SELECT).
4. ``close_window(window_id)`` — emit remaining buffered rows, then clear.

Delivery remains at-least-once; destinations must upsert with LSN/PK guards.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable


@dataclass
class WindowRow:
    pk: str
    row: dict[str, Any]
    op: str = "r"  # r = snapshot read, c/u/d = stream
    source: str = "snapshot"  # snapshot | stream


@dataclass
class SnapshotWindow:
    window_id: str
    primary_key: str = "id"
    open: bool = False
    buffer: dict[str, WindowRow] = field(default_factory=dict)
    stream_overrides: int = 0
    snapshot_rows: int = 0

    def open_window(self) -> None:
        self.open = True
        self.buffer.clear()
        self.stream_overrides = 0
        self.snapshot_rows = 0

    def close_window(self) -> list[dict[str, Any]]:
        """Return buffered rows (stream-wins applied) and close the window."""
        rows = [wr.row for wr in self.buffer.values() if wr.op != "d"]
        deletes = [wr.pk for wr in self.buffer.values() if wr.op == "d"]
        self.open = False
        self.buffer.clear()
        # Encode deletes as tombstone rows with __deleted for callers that need them
        out = list(rows)
        for pk in deletes:
            out.append({self.primary_key: pk, "__deleted": True, "__op": "d"})
        return out

    def add_snapshot_rows(self, rows: Iterable[dict[str, Any]]) -> None:
        if not self.open:
            raise RuntimeError("snapshot window is not open")
        for row in rows:
            pk = _pk_value(row, self.primary_key)
            if pk is None:
                continue
            # Do not overwrite a stream event already seen in this window.
            existing = self.buffer.get(pk)
            if existing and existing.source == "stream":
                continue
            self.buffer[pk] = WindowRow(pk=pk, row=dict(row), op="r", source="snapshot")
            self.snapshot_rows += 1

    def apply_stream_event(
        self,
        *,
        op: str,
        row: dict[str, Any] | None = None,
        pk: str | None = None,
    ) -> None:
        """Apply a live CDC event inside an open window (stream wins)."""
        if not self.open:
            return
        key = pk if pk is not None else _pk_value(row or {}, self.primary_key)
        if key is None:
            return
        existing = self.buffer.get(key)
        if existing and existing.source == "snapshot":
            self.stream_overrides += 1
        op_l = (op or "u").lower()
        if op_l in {"d", "delete"}:
            self.buffer[key] = WindowRow(
                pk=key,
                row={self.primary_key: key, "__deleted": True, "__op": "d"},
                op="d",
                source="stream",
            )
            return
        payload = dict(row or {})
        if self.primary_key not in payload:
            payload[self.primary_key] = key
        payload["__op"] = "u" if op_l in {"u", "update"} else "c"
        self.buffer[key] = WindowRow(pk=key, row=payload, op=op_l[0] if op_l else "u", source="stream")

    def stats(self) -> dict[str, Any]:
        return {
            "window_id": self.window_id,
            "open": self.open,
            "buffered": len(self.buffer),
            "snapshot_rows": self.snapshot_rows,
            "stream_overrides": self.stream_overrides,
            "primary_key": self.primary_key,
        }


def _pk_value(row: dict[str, Any], primary_key: str) -> str | None:
    if primary_key in row and row[primary_key] is not None:
        return str(row[primary_key])
    # Case-insensitive fallback
    lower = {str(k).lower(): v for k, v in row.items()}
    val = lower.get(primary_key.lower())
    return str(val) if val is not None else None


def merge_snapshot_chunk_with_stream(
    *,
    window_id: str,
    primary_key: str,
    snapshot_rows: list[dict[str, Any]],
    stream_events: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Convenience: open → buffer snapshot → apply stream events → close.

    Each stream event dict needs ``op`` (c/u/d) and ``row`` or top-level fields
    including the primary key.
    """
    win = SnapshotWindow(window_id=window_id, primary_key=primary_key)
    win.open_window()
    win.add_snapshot_rows(snapshot_rows)
    for ev in stream_events:
        op = str(ev.get("op") or ev.get("__op") or "u")
        row = ev.get("row") if isinstance(ev.get("row"), dict) else {
            k: v for k, v in ev.items() if k not in {"op", "__op", "row"}
        }
        win.apply_stream_event(op=op, row=row, pk=ev.get("pk"))
    emitted = win.close_window()
    return emitted, win.stats()
