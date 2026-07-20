"""Debezium-style incremental snapshot signals for native CDC.

Allows adding a table (or backfilling a PK range) without a full cutover:
emit a signal → connector reads snapshot chunks interleaved with CDC events
→ mark complete when watermark caught up.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Iterator

_LOCK = threading.RLock()
_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_PATH = os.path.join(_DATA_DIR, "cdc_incremental_snapshots.json")


@dataclass
class SnapshotSignal:
    id: str
    source_key: str
    table: str
    status: str = "pending"  # pending | running | completed | failed | cancelled
    primary_key: str = "id"
    chunk_size: int = 1000
    last_pk: str = ""
    rows_snapshotted: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SnapshotSignal":
        return cls(
            id=str(d.get("id") or ""),
            source_key=str(d.get("source_key") or ""),
            table=str(d.get("table") or ""),
            status=str(d.get("status") or "pending"),
            primary_key=str(d.get("primary_key") or "id"),
            chunk_size=int(d.get("chunk_size") or 1000),
            last_pk=str(d.get("last_pk") or ""),
            rows_snapshotted=int(d.get("rows_snapshotted") or 0),
            created_at=float(d.get("created_at") or time.time()),
            updated_at=float(d.get("updated_at") or time.time()),
            error=str(d.get("error") or ""),
        )


def _load() -> list[dict[str, Any]]:
    if not os.path.isfile(_PATH):
        return []
    try:
        with open(_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return list(data.get("signals") or [])
    except Exception:
        return []


def _save(signals: list[dict[str, Any]]) -> None:
    from pathlib import Path

    from services.atomic_file import write_json_atomic

    write_json_atomic(Path(_PATH), {"signals": signals})


@contextmanager
def _signal_store_lock() -> Iterator[None]:
    """Process + cross-process lock so concurrent CDC jobs cannot tear the store."""
    os.makedirs(_DATA_DIR, exist_ok=True)
    lock_path = _PATH + ".lock"
    lock_f = open(lock_path, "a+", encoding="utf-8")
    locked = False
    try:
        try:
            import fcntl

            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
            locked = True
        except Exception:
            locked = False
        with _LOCK:
            yield
    finally:
        if locked:
            try:
                import fcntl

                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
        try:
            lock_f.close()
        except Exception:
            pass


def request_incremental_snapshot(
    source_key: str,
    table: str,
    *,
    primary_key: str = "id",
    chunk_size: int = 1000,
) -> SnapshotSignal:
    """Enqueue an incremental snapshot for ``table`` on ``source_key``."""
    sig = SnapshotSignal(
        id=f"snap_{uuid.uuid4().hex[:12]}",
        source_key=source_key,
        table=table,
        primary_key=primary_key,
        chunk_size=max(1, min(int(chunk_size), 50_000)),
    )
    with _signal_store_lock():
        rows = _load()
        rows.append(sig.to_dict())
        _save(rows)
    return sig


def list_signals(source_key: str = "", *, status: str = "") -> list[SnapshotSignal]:
    with _signal_store_lock():
        rows = _load()
    out = [SnapshotSignal.from_dict(r) for r in rows]
    if source_key:
        out = [s for s in out if s.source_key == source_key]
    if status:
        out = [s for s in out if s.status == status]
    return sorted(out, key=lambda s: s.created_at, reverse=True)


def claim_next_signal(source_key: str, table: str = "") -> SnapshotSignal | None:
    """Atomically claim the next signal for this source.

    Prefers an in-progress (``running``) signal so chunked snapshots resume,
    then the oldest ``pending`` signal. Optional ``table`` filters the claim.
    """
    with _signal_store_lock():
        rows = _load()
        # 1) Resume running
        for i, r in enumerate(rows):
            if r.get("source_key") != source_key or r.get("status") != "running":
                continue
            if table and r.get("table") and r.get("table") != table:
                continue
            r["updated_at"] = time.time()
            rows[i] = r
            _save(rows)
            return SnapshotSignal.from_dict(r)
        # 2) Claim pending
        for i, r in enumerate(rows):
            if r.get("source_key") != source_key or r.get("status") != "pending":
                continue
            if table and r.get("table") and r.get("table") != table:
                continue
            r["status"] = "running"
            r["updated_at"] = time.time()
            rows[i] = r
            _save(rows)
            return SnapshotSignal.from_dict(r)
    return None


def update_signal(signal_id: str, **fields: Any) -> SnapshotSignal | None:
    with _signal_store_lock():
        rows = _load()
        for i, r in enumerate(rows):
            if r.get("id") == signal_id:
                r.update({k: v for k, v in fields.items() if v is not None})
                r["updated_at"] = time.time()
                rows[i] = r
                _save(rows)
                return SnapshotSignal.from_dict(r)
    return None


def mark_chunk(signal_id: str, *, last_pk: str, rows: int) -> SnapshotSignal | None:
    with _signal_store_lock():
        rows_data = _load()
        for i, r in enumerate(rows_data):
            if r.get("id") == signal_id:
                r["last_pk"] = last_pk
                r["rows_snapshotted"] = int(r.get("rows_snapshotted") or 0) + int(rows)
                r["updated_at"] = time.time()
                rows_data[i] = r
                _save(rows_data)
                return SnapshotSignal.from_dict(r)
    return None


def complete_signal(signal_id: str, *, error: str = "") -> SnapshotSignal | None:
    status = "failed" if error else "completed"
    return update_signal(signal_id, status=status, error=error)


def get_signal(signal_id: str) -> SnapshotSignal | None:
    with _signal_store_lock():
        for r in _load():
            if r.get("id") == signal_id:
                return SnapshotSignal.from_dict(r)
    return None


def cancel_signal(signal_id: str) -> SnapshotSignal | None:
    """Cancel a pending/running incremental snapshot (Debezium signal stop)."""
    with _signal_store_lock():
        rows = _load()
        for i, r in enumerate(rows):
            if r.get("id") != signal_id:
                continue
            status = str(r.get("status") or "")
            if status in {"completed", "failed", "cancelled"}:
                return SnapshotSignal.from_dict(r)
            r["status"] = "cancelled"
            r["updated_at"] = time.time()
            rows[i] = r
            _save(rows)
            return SnapshotSignal.from_dict(r)
    return None
