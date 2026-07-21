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
_MONGO_COLL = "cdc_incremental_snapshots"


def _signals_coll():
    from services.control_plane_store import mongo_collection

    return mongo_collection(_MONGO_COLL)


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


def _doc_to_signal(doc: dict[str, Any]) -> SnapshotSignal:
    row = dict(doc)
    row.pop("_id", None)
    if not row.get("id") and doc.get("_id") is not None:
        row["id"] = str(doc["_id"])
    return SnapshotSignal.from_dict(row)


def _load() -> list[dict[str, Any]]:
    coll = _signals_coll()
    if coll is not None:
        try:
            docs = list(coll.find().sort("created_at", -1).limit(500))
            out: list[dict[str, Any]] = []
            for d in docs:
                row = dict(d)
                row.pop("_id", None)
                if not row.get("id") and d.get("_id") is not None:
                    row["id"] = str(d["_id"])
                out.append(row)
            return out
        except Exception:
            pass
    if not os.path.isfile(_PATH):
        return []
    try:
        with open(_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return list(data.get("signals") or [])
    except Exception:
        return []


def _save(signals: list[dict[str, Any]]) -> None:
    coll = _signals_coll()
    if coll is not None:
        for row in signals[-500:]:
            sid = str(row.get("id") or "")
            if not sid:
                continue
            doc = dict(row)
            doc["_id"] = sid
            coll.replace_one({"_id": sid}, doc, upsert=True)
        return
    from pathlib import Path

    from services.atomic_file import write_json_atomic

    write_json_atomic(Path(_PATH), {"signals": signals})


def _upsert_signal(sig: SnapshotSignal | dict[str, Any]) -> SnapshotSignal:
    row = sig.to_dict() if isinstance(sig, SnapshotSignal) else dict(sig)
    sid = str(row.get("id") or "")
    coll = _signals_coll()
    if coll is not None and sid:
        doc = dict(row)
        doc["_id"] = sid
        coll.replace_one({"_id": sid}, doc, upsert=True)
        return SnapshotSignal.from_dict(row)
    with _signal_store_lock():
        rows = _load()
        found = False
        for i, r in enumerate(rows):
            if r.get("id") == sid:
                rows[i] = row
                found = True
                break
        if not found:
            rows.append(row)
        _save(rows)
    return SnapshotSignal.from_dict(row)


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
    return _upsert_signal(sig)


def list_signals(source_key: str = "", *, status: str = "") -> list[SnapshotSignal]:
    coll = _signals_coll()
    if coll is not None:
        try:
            query: dict[str, Any] = {}
            if source_key:
                query["source_key"] = source_key
            if status:
                query["status"] = status
            docs = list(coll.find(query).sort("created_at", -1).limit(500))
            return [_doc_to_signal(d) for d in docs]
        except Exception:
            pass
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
    coll = _signals_coll()
    if coll is not None:
        try:
            from pymongo import ReturnDocument

            base: dict[str, Any] = {"source_key": source_key}
            if table:
                base["table"] = table
            # Resume running
            doc = coll.find_one_and_update(
                {**base, "status": "running"},
                {"$set": {"updated_at": time.time()}},
                sort=[("created_at", 1)],
                return_document=ReturnDocument.AFTER,
            )
            if doc:
                return _doc_to_signal(doc)
            # Claim pending
            doc = coll.find_one_and_update(
                {**base, "status": "pending"},
                {"$set": {"status": "running", "updated_at": time.time()}},
                sort=[("created_at", 1)],
                return_document=ReturnDocument.AFTER,
            )
            if doc:
                return _doc_to_signal(doc)
            return None
        except Exception:
            pass
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
    coll = _signals_coll()
    if coll is not None:
        try:
            from pymongo import ReturnDocument

            updates = {k: v for k, v in fields.items() if v is not None}
            updates["updated_at"] = time.time()
            doc = coll.find_one_and_update(
                {"_id": signal_id},
                {"$set": updates},
                return_document=ReturnDocument.AFTER,
            )
            if doc:
                return _doc_to_signal(doc)
        except Exception:
            pass
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
    coll = _signals_coll()
    if coll is not None:
        try:
            from pymongo import ReturnDocument

            doc = coll.find_one_and_update(
                {"_id": signal_id, "status": {"$in": ["pending", "running"]}},
                {
                    "$set": {
                        "last_pk": last_pk,
                        "status": "running",
                        "updated_at": time.time(),
                    },
                    "$inc": {"rows_snapshotted": int(rows)},
                },
                return_document=ReturnDocument.AFTER,
            )
            if doc:
                return _doc_to_signal(doc)
            existing = coll.find_one({"_id": signal_id})
            return _doc_to_signal(existing) if existing else None
        except Exception:
            pass
    with _signal_store_lock():
        rows_data = _load()
        for i, r in enumerate(rows_data):
            if r.get("id") == signal_id:
                # Do not resurrect a cancelled/failed/completed signal mid-chunk.
                if r.get("status") not in ("pending", "running"):
                    return SnapshotSignal.from_dict(r)
                r["last_pk"] = last_pk
                r["rows_snapshotted"] = int(r.get("rows_snapshotted") or 0) + int(rows)
                r["status"] = "running"
                r["updated_at"] = time.time()
                rows_data[i] = r
                _save(rows_data)
                return SnapshotSignal.from_dict(r)
    return None


def complete_signal(signal_id: str, *, error: str = "") -> SnapshotSignal | None:
    coll = _signals_coll()
    if coll is not None:
        try:
            from pymongo import ReturnDocument

            existing = coll.find_one({"_id": signal_id})
            if not existing:
                return None
            if existing.get("status") == "cancelled":
                return _doc_to_signal(existing)
            status = "failed" if error else "completed"
            updates: dict[str, Any] = {"status": status, "updated_at": time.time()}
            if error:
                updates["error"] = error
            doc = coll.find_one_and_update(
                {"_id": signal_id},
                {"$set": updates},
                return_document=ReturnDocument.AFTER,
            )
            return _doc_to_signal(doc) if doc else None
        except Exception:
            pass
    with _signal_store_lock():
        rows = _load()
        for i, r in enumerate(rows):
            if r.get("id") != signal_id:
                continue
            if r.get("status") == "cancelled":
                return SnapshotSignal.from_dict(r)
            status = "failed" if error else "completed"
            r["status"] = status
            if error:
                r["error"] = error
            r["updated_at"] = time.time()
            rows[i] = r
            _save(rows)
            return SnapshotSignal.from_dict(r)
    return None


def get_signal(signal_id: str) -> SnapshotSignal | None:
    coll = _signals_coll()
    if coll is not None:
        try:
            doc = coll.find_one({"_id": signal_id}) or coll.find_one({"id": signal_id})
            if doc:
                return _doc_to_signal(doc)
        except Exception:
            pass
    with _signal_store_lock():
        for r in _load():
            if r.get("id") == signal_id:
                return SnapshotSignal.from_dict(r)
    return None


def cancel_signal(signal_id: str) -> SnapshotSignal | None:
    """Cancel a pending/running incremental snapshot (Debezium signal stop)."""
    coll = _signals_coll()
    if coll is not None:
        try:
            from pymongo import ReturnDocument

            existing = coll.find_one({"_id": signal_id})
            if not existing:
                return None
            status = str(existing.get("status") or "")
            if status in {"completed", "failed", "cancelled"}:
                return _doc_to_signal(existing)
            doc = coll.find_one_and_update(
                {"_id": signal_id},
                {"$set": {"status": "cancelled", "updated_at": time.time()}},
                return_document=ReturnDocument.AFTER,
            )
            return _doc_to_signal(doc) if doc else None
        except Exception:
            pass
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
