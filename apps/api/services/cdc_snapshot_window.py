"""Debezium DDD-3 snapshot window buffer (stream-wins PK collision).

Algorithm (incremental snapshot chunk while WAL streams):

1. ``open_window(window_id)`` — begin buffering snapshot READ rows by PK.
2. Snapshot chunk rows enter the buffer as op=``r`` (read).
3. Live stream events for the same PK during the open window **replace** the
   buffered snapshot row (stream wins — newer than the SELECT).
4. ``close_window(window_id)`` — emit remaining buffered rows, then clear.

Composite primary keys are supported (``primary_key=["a","b"]``) — Debezium
chunk SELECT uses multi-column order; the window key is a stable join of
column values so stream collisions still win per logical row.

Delivery remains at-least-once; destinations must upsert with LSN/PK guards.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

# Unit separator — unlikely in PK string values; stable for composite keys.
_PK_SEP = "\x1f"


def _pk_columns(primary_key: str | Sequence[str]) -> list[str]:
    """Normalize a primary key declaration into an ordered column list.

    Accepts a list/tuple of columns, or a single string that is either one
    column or a comma-/semicolon-joined composite (``order_id,line_id``). The
    streaming CDC path and the incremental-snapshot path share this helper so
    a composite key cannot be treated as one literal column name in one place
    and as a list in another — that mismatch is what made CDC silently fall
    through to append-only writes with zero deletes.
    """
    if isinstance(primary_key, (list, tuple)):
        cols = [str(c).strip() for c in primary_key if str(c).strip()]
        if not cols:
            raise ValueError("primary_key list must contain at least one column")
        return cols
    raw = str(primary_key or "").strip()
    if not raw:
        raise ValueError("primary_key is required")
    if "," in raw or ";" in raw:
        cols = [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
        if not cols:
            raise ValueError("primary_key is required")
        return cols
    return [raw]


def _row_get(row: dict[str, Any], col: str) -> Any:
    if col in row and row[col] is not None:
        return row[col]
    lower = {str(k).lower(): v for k, v in row.items()}
    return lower.get(col.lower())


def _pk_value(row: dict[str, Any], primary_key: str | Sequence[str]) -> str | None:
    cols = _pk_columns(primary_key)
    parts: list[str] = []
    for col in cols:
        val = _row_get(row, col)
        if val is None:
            return None
        parts.append(str(val))
    return _PK_SEP.join(parts) if len(parts) > 1 else parts[0]


def _pk_row_dict(primary_key: str | Sequence[str], key: str) -> dict[str, Any]:
    cols = _pk_columns(primary_key)
    if len(cols) == 1:
        return {cols[0]: key}
    parts = key.split(_PK_SEP)
    return {cols[i]: (parts[i] if i < len(parts) else None) for i in range(len(cols))}


def keyset_successor_predicate(
    quoted_pk_columns: Sequence[str],
    last_pk: str,
    placeholder: str = "%s",
) -> tuple[str, list[Any]]:
    """Build a strict lexicographic ``> last_pk`` predicate for a chunk read.

    For key columns ``(a, b, c)`` this produces::

        (a > ?) OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?)

    which is the only correct successor for a composite ordering — a naive
    ``a > ?`` would skip every remaining row sharing the last chunk's leading
    key value, and a per-column ``AND`` chain would skip rows that differ only
    in the trailing column.

    Shared by the MySQL and Postgres incremental-snapshot readers so both agree
    on chunk boundaries and on the ``_PK_SEP`` encoding of ``last_pk``.

    Raises ``ValueError`` when ``last_pk`` does not carry one part per key
    column, because a silent arity mismatch would read the wrong range.
    """
    cols = list(quoted_pk_columns)
    if not cols:
        raise ValueError("keyset predicate requires at least one primary key column")
    parts = last_pk.split(_PK_SEP) if len(cols) > 1 else [last_pk]
    if len(parts) != len(cols):
        raise ValueError(
            f"composite last_pk arity mismatch: expected {len(cols)} parts, "
            f"got {len(parts)}"
        )
    clauses: list[str] = []
    params: list[Any] = []
    for i, col in enumerate(cols):
        equalities = " AND ".join(f"{cols[j]} = {placeholder}" for j in range(i))
        greater = f"{col} > {placeholder}"
        clauses.append(f"({greater})" if not equalities else f"({equalities} AND {greater})")
        params.extend(parts[:i])
        params.append(parts[i])
    return " OR ".join(clauses), params


@dataclass
class WindowRow:
    pk: str
    row: dict[str, Any]
    op: str = "r"  # r = snapshot read, c/u/d = stream
    source: str = "snapshot"  # snapshot | stream


@dataclass
class SnapshotWindow:
    window_id: str
    primary_key: str | list[str] = "id"
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
            tomb = _pk_row_dict(self.primary_key, pk)
            tomb["__deleted"] = True
            tomb["__op"] = "d"
            out.append(tomb)
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
            tomb = _pk_row_dict(self.primary_key, key)
            tomb["__deleted"] = True
            tomb["__op"] = "d"
            self.buffer[key] = WindowRow(
                pk=key,
                row=tomb,
                op="d",
                source="stream",
            )
            return
        payload = dict(row or {})
        # Ensure all PK columns are present for composite keys.
        for col, val in _pk_row_dict(self.primary_key, key).items():
            if col not in payload:
                payload[col] = val
        payload["__op"] = "u" if op_l in {"u", "update"} else "c"
        self.buffer[key] = WindowRow(
            pk=key, row=payload, op=op_l[0] if op_l else "u", source="stream"
        )

    def stats(self) -> dict[str, Any]:
        return {
            "window_id": self.window_id,
            "open": self.open,
            "buffered": len(self.buffer),
            "snapshot_rows": self.snapshot_rows,
            "stream_overrides": self.stream_overrides,
            "primary_key": self.primary_key,
        }


def merge_snapshot_chunk_with_stream(
    *,
    window_id: str,
    primary_key: str | list[str],
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
