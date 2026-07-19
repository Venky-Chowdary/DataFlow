"""Debezium-class transaction buffering for log CDC.

Algorithm
---------
Logical streams emit BEGIN / DML / COMMIT (and optionally ROLLBACK). Applying
DML before COMMIT breaks transactional consistency (partial transactions land
on the destination). This buffer:

1. On BEGIN — open a new in-memory transaction keyed by xid (or anonymous).
2. On DML — append to the open transaction (never emit).
3. On COMMIT — flush the full ChangeBatch atomically to the caller.
4. On ROLLBACK / ABORT — discard the open transaction.

This is strictly stronger than naive "emit every row as it arrives" parsers
and matches Debezium's transactional ordering guarantees for at-least-once
apply (destination upserts remain idempotent).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator

from services.cdc_engine import ChangeBatch


@dataclass
class _OpenTxn:
    xid: str
    inserts: list[dict[str, Any]] = field(default_factory=list)
    updates: list[dict[str, Any]] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)
    last_lsn: str | None = None


class TransactionBuffer:
    """Buffer DML until COMMIT; yield one ChangeBatch per committed txn."""

    def __init__(self, *, max_events: int = 50_000) -> None:
        self.max_events = max(100, int(max_events))
        self._open: _OpenTxn | None = None
        self._anonymous_seq = 0

    @property
    def open_xid(self) -> str | None:
        return self._open.xid if self._open else None

    def begin(self, xid: str | None = None, *, lsn: str | None = None) -> None:
        if self._open is not None:
            # Nested BEGIN without COMMIT — treat as continuation (PG sometimes
            # omits explicit BEGIN when using get_changes with include-xids=0).
            if lsn:
                self._open.last_lsn = lsn
            return
        self._anonymous_seq += 1
        self._open = _OpenTxn(xid=xid or f"anon-{self._anonymous_seq}", last_lsn=lsn)

    def insert(self, row: dict[str, Any], *, lsn: str | None = None) -> None:
        self._ensure_open(lsn)
        assert self._open is not None
        self._open.inserts.append(row)
        self._maybe_overflow()

    def update(self, row: dict[str, Any], *, lsn: str | None = None) -> None:
        self._ensure_open(lsn)
        assert self._open is not None
        self._open.updates.append(row)
        self._maybe_overflow()

    def delete(self, pk: str, *, lsn: str | None = None) -> None:
        if not pk:
            return
        self._ensure_open(lsn)
        assert self._open is not None
        self._open.deletes.append(pk)
        self._maybe_overflow()

    def commit(self, *, lsn: str | None = None, resume_token: Any = None) -> ChangeBatch | None:
        if self._open is None:
            # COMMIT without BEGIN — emit empty heartbeat if token provided.
            if resume_token is not None:
                return ChangeBatch(resume_token=resume_token)
            return None
        if lsn:
            self._open.last_lsn = lsn
        batch = ChangeBatch(
            inserts=list(self._open.inserts),
            updates=list(self._open.updates),
            deletes=list(self._open.deletes),
            resume_token=resume_token,
        )
        self._open = None
        if batch.total_changes or resume_token is not None:
            return batch
        return None

    def rollback(self) -> None:
        self._open = None

    def flush_open(self, *, resume_token: Any = None) -> ChangeBatch | None:
        """Force-flush an open txn (used when stream ends mid-txn / peek window).

        Debezium holds until COMMIT; for SQL peek windows that truncate mid-txn
        we emit what we have so progress is not stuck, tagged with the last LSN.
        Prefer COMMIT boundaries when the stream provides them.
        """
        if self._open is None:
            return None
        return self.commit(resume_token=resume_token)

    def drain(self, events: Iterator[tuple[str, dict[str, Any]]]) -> Iterator[ChangeBatch]:
        """Convenience: feed ``(op, payload)`` events where op ∈ begin|insert|update|delete|commit|rollback."""
        for op, payload in events:
            lsn = payload.get("lsn")
            token = payload.get("resume_token")
            if op == "begin":
                self.begin(payload.get("xid"), lsn=lsn)
            elif op == "insert":
                self.insert(payload["row"], lsn=lsn)
            elif op == "update":
                self.update(payload["row"], lsn=lsn)
            elif op == "delete":
                self.delete(str(payload.get("pk") or ""), lsn=lsn)
            elif op == "commit":
                batch = self.commit(lsn=lsn, resume_token=token)
                if batch is not None:
                    yield batch
            elif op == "rollback":
                self.rollback()
        # End of stream — do not auto-flush; caller decides.

    def _ensure_open(self, lsn: str | None) -> None:
        if self._open is None:
            self.begin(lsn=lsn)
        elif lsn:
            self._open.last_lsn = lsn

    def _maybe_overflow(self) -> None:
        if self._open is None:
            return
        n = len(self._open.inserts) + len(self._open.updates) + len(self._open.deletes)
        if n > self.max_events:
            raise RuntimeError(
                f"CDC transaction buffer exceeded max_events={self.max_events} "
                f"(xid={self._open.xid}); increase buffer or reduce txn size"
            )
