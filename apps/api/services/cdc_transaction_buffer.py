"""Debezium-class transaction buffering for log CDC.

Algorithm
---------
Logical streams emit BEGIN / DML / COMMIT (and optionally ROLLBACK). Applying
DML before COMMIT breaks transactional consistency (partial transactions land
on the destination). This buffer:

1. On BEGIN — open a new in-memory transaction keyed by xid (or anonymous).
2. On DML — append to the open transaction **in event order** (never emit).
3. On COMMIT — coalesce per-PK net effect (DELETE+INSERT → insert), then flush.
4. On ROLLBACK / ABORT — discard the open transaction.

When an open transaction grows large, events **spill to a temp JSONL file**
(``DATAFLOW_CDC_TXN_SPILL_AFTER``, default half of max) so memory stays bounded.
Oversized open transactions still **raise** :class:`CdcTxnBufferOverflow` past
``DATAFLOW_CDC_TXN_BUFFER_MAX_EVENTS`` — never silently truncate or drop events.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from services.cdc_engine import ChangeBatch
from services.cdc_net_effect import CdcTxnEvent, coalesce_cdc_txn_events, infer_row_pk


def default_txn_buffer_max_events() -> int:
    raw = os.getenv("DATAFLOW_CDC_TXN_BUFFER_MAX_EVENTS", "50000")
    try:
        return max(100, int(raw))
    except (TypeError, ValueError):
        return 50_000


def default_txn_spill_after(max_events: int) -> int:
    """Spill in-memory events to disk after this many (keeps RAM bounded)."""
    raw = os.getenv("DATAFLOW_CDC_TXN_SPILL_AFTER", "").strip()
    if raw:
        try:
            return max(50, int(raw))
        except (TypeError, ValueError):
            pass
    return max(50, max_events // 2)


class CdcTxnBufferOverflow(RuntimeError):
    """Open CDC transaction exceeded the event cap — fail closed."""

    def __init__(
        self,
        message: str,
        *,
        xid: str = "",
        max_events: int = 0,
        event_count: int = 0,
    ) -> None:
        super().__init__(message)
        self.xid = xid
        self.max_events = max_events
        self.event_count = event_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": "cdc_txn_buffer_overflow",
            "message": str(self),
            "xid": self.xid,
            "max_events": self.max_events,
            "event_count": self.event_count,
        }


@dataclass
class _OpenTxn:
    xid: str
    events: list[CdcTxnEvent] = field(default_factory=list)
    last_lsn: str | None = None
    spilled_count: int = 0
    spill_path: str | None = None


class TransactionBuffer:
    """Buffer DML until COMMIT; yield one ChangeBatch per committed txn."""

    def __init__(
        self,
        *,
        max_events: int | None = None,
        spill_after: int | None = None,
        spill_dir: str | Path | None = None,
    ) -> None:
        if max_events is not None:
            self.max_events = max(1, int(max_events))
        else:
            self.max_events = max(100, default_txn_buffer_max_events())
        if spill_after is not None:
            self.spill_after = max(1, int(spill_after))
        else:
            self.spill_after = default_txn_spill_after(self.max_events)
        self.spill_dir = Path(spill_dir) if spill_dir else None
        self._open: _OpenTxn | None = None
        self._anonymous_seq = 0

    @property
    def open_xid(self) -> str | None:
        return self._open.xid if self._open else None

    @property
    def open_event_count(self) -> int:
        if self._open is None:
            return 0
        return self._open.spilled_count + len(self._open.events)

    def begin(self, xid: str | None = None, *, lsn: str | None = None) -> None:
        if self._open is not None:
            # Nested BEGIN without COMMIT — treat as continuation (PG sometimes
            # omits explicit BEGIN when using get_changes with include-xids=0).
            if lsn:
                self._open.last_lsn = lsn
            return
        self._anonymous_seq += 1
        self._open = _OpenTxn(xid=xid or f"anon-{self._anonymous_seq}", last_lsn=lsn)

    def insert(
        self,
        row: dict[str, Any],
        *,
        pk: str | None = None,
        lsn: str | None = None,
    ) -> None:
        self._ensure_open(lsn)
        if self._open is None:
            raise RuntimeError("CDC transaction buffer is not open after insert")
        self._open.events.append(
            CdcTxnEvent(op="i", pk=infer_row_pk(row, explicit=pk), row=dict(row), lsn=lsn)
        )
        self._after_dml()

    def update(
        self,
        row: dict[str, Any],
        *,
        pk: str | None = None,
        lsn: str | None = None,
    ) -> None:
        self._ensure_open(lsn)
        if self._open is None:
            raise RuntimeError("CDC transaction buffer is not open after update")
        self._open.events.append(
            CdcTxnEvent(op="u", pk=infer_row_pk(row, explicit=pk), row=dict(row), lsn=lsn)
        )
        self._after_dml()

    def delete(self, pk: str, *, lsn: str | None = None) -> None:
        if not pk:
            return
        self._ensure_open(lsn)
        if self._open is None:
            raise RuntimeError("CDC transaction buffer is not open after delete")
        self._open.events.append(CdcTxnEvent(op="d", pk=str(pk), row=None, lsn=lsn))
        self._after_dml()

    def commit(self, *, lsn: str | None = None, resume_token: Any = None) -> ChangeBatch | None:
        if self._open is None:
            # COMMIT without BEGIN — emit empty heartbeat if token provided.
            if resume_token is not None:
                return ChangeBatch(resume_token=resume_token)
            return None
        if lsn:
            self._open.last_lsn = lsn
        inserts, updates, deletes = self._materialize_open()
        spill_path = self._open.spill_path
        self._open = None
        self._cleanup_spill(spill_path)
        batch = ChangeBatch(
            inserts=inserts,
            updates=updates,
            deletes=deletes,
            resume_token=resume_token,
        )
        if batch.total_changes or resume_token is not None:
            return batch
        return None

    def rollback(self) -> None:
        if self._open is not None:
            spill_path = self._open.spill_path
            self._open = None
            self._cleanup_spill(spill_path)
        else:
            self._open = None

    def flush_open(self, *, resume_token: Any = None) -> ChangeBatch | None:
        """Deprecated for peek windows — prefer holding until COMMIT.

        Kept for callers that intentionally force-flush (tests / non-peek
        streams). PostgreSQL logical CDC uses peek + no flush so WAL is
        redelivered until a real COMMIT (Debezium-class).
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
                self.insert(payload["row"], pk=payload.get("pk"), lsn=lsn)
            elif op == "update":
                self.update(payload["row"], pk=payload.get("pk"), lsn=lsn)
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

    def _after_dml(self) -> None:
        self._maybe_overflow()
        self._maybe_spill()

    def _maybe_overflow(self) -> None:
        if self._open is None:
            return
        n = self.open_event_count
        if n > self.max_events:
            xid = self._open.xid
            raise CdcTxnBufferOverflow(
                f"CDC transaction buffer exceeded max_events={self.max_events} "
                f"(xid={xid}, events={n}); increase DATAFLOW_CDC_TXN_BUFFER_MAX_EVENTS "
                f"or reduce source txn size — refusing silent truncation",
                xid=xid,
                max_events=self.max_events,
                event_count=n,
            )

    def _memory_event_count(self) -> int:
        if self._open is None:
            return 0
        return len(self._open.events)

    def _maybe_spill(self) -> None:
        if self._open is None:
            return
        if self._memory_event_count() < self.spill_after:
            return
        self._spill_memory_to_disk()

    def _spill_memory_to_disk(self) -> None:
        if self._open is None:
            raise RuntimeError("CDC transaction buffer is not open during spill")
        if not self._open.events:
            return
        path = self._open.spill_path
        if not path:
            fd, path = tempfile.mkstemp(
                prefix="df_cdc_txn_",
                suffix=".jsonl",
                dir=str(self.spill_dir) if self.spill_dir else None,
            )
            os.close(fd)
            self._open.spill_path = path
        with open(path, "a", encoding="utf-8") as fh:
            for ev in self._open.events:
                fh.write(
                    json.dumps(
                        {"op": ev.op, "pk": ev.pk, "row": ev.row, "lsn": ev.lsn},
                        default=str,
                    )
                    + "\n"
                )
        self._open.spilled_count += len(self._open.events)
        self._open.events.clear()

    def _materialize_open(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
        if self._open is None:
            raise RuntimeError("CDC transaction buffer is not open during materialize")
        ordered: list[CdcTxnEvent] = []
        if self._open.spill_path and Path(self._open.spill_path).exists():
            with open(self._open.spill_path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    op = rec.get("op")
                    if op not in {"i", "u", "d"}:
                        continue
                    row = rec.get("row") if isinstance(rec.get("row"), dict) else None
                    pk = str(rec.get("pk") or "")
                    if not pk and row is not None:
                        pk = infer_row_pk(row)
                    ordered.append(
                        CdcTxnEvent(op=op, pk=pk, row=row, lsn=rec.get("lsn"))
                    )
        ordered.extend(self._open.events)
        return coalesce_cdc_txn_events(ordered)

    @staticmethod
    def _cleanup_spill(path: str | None) -> None:
        if not path:
            return
        try:
            Path(path).unlink(missing_ok=True)
        except OSError:
            pass
