"""Debezium-class multi-table single-reader helpers.

One logical slot (Postgres) or one binlog ``server_id`` (MySQL) decodes N tables
and demuxes into per-table :class:`ChangeBatch` values that share a resume token.
Ack only after the batch marked ``ack_barrier=True`` is applied (at-least-once).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any, Iterator

from services.cdc_engine import ChangeBatch
from services.cdc_transaction_buffer import (
    CdcTxnBufferOverflow,
    default_txn_buffer_max_events,
)


def normalize_table_list(tables: list[str] | tuple[str, ...] | str) -> list[str]:
    if isinstance(tables, str):
        parts = [p.strip() for p in tables.split(",") if p.strip()]
        return parts or [tables.strip()] if tables.strip() else []
    out: list[str] = []
    seen: set[str] = set()
    for t in tables or []:
        name = str(t or "").strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def tables_digest(tables: list[str]) -> str:
    token = ",".join(sorted(t.lower() for t in tables))
    return hashlib.sha1(token.encode("utf-8"), usedforsecurity=False).hexdigest()[:10]  # noqa: S324


def shared_route_cursor_key(
    *,
    engine: str,
    database: str,
    tables: list[str],
    job_id: str = "",
) -> str:
    """Stable cursor key for the shared log consumer (not per-table)."""
    digest = tables_digest(tables)
    jid = (job_id or "job")[:24]
    return f"cdc-shared:{engine}:{database}:{digest}:{jid}"


def can_share_log_reader(src_type: str, table_count: int) -> bool:
    """True when Debezium-style single-reader multi-table is supported."""
    if table_count < 2:
        return False
    return (src_type or "").lower() in {
        "postgresql",
        "postgres",
        "mysql",
        "sqlserver",
        "mssql",
        "oracle",
    }


@dataclass
class _TableBuf:
    inserts: list[dict[str, Any]] = field(default_factory=list)
    updates: list[dict[str, Any]] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)


@dataclass
class _OpenMultiTxn:
    xid: str
    by_table: dict[str, _TableBuf] = field(default_factory=dict)
    last_lsn: str | None = None
    event_count: int = 0
    explicit: bool = False


class MultiTableTransactionBuffer:
    """Buffer DML by table until COMMIT; yield one ChangeBatch per touched table."""

    def __init__(self, *, max_events: int | None = None) -> None:
        if max_events is not None:
            self.max_events = max(1, int(max_events))
        else:
            self.max_events = max(100, default_txn_buffer_max_events())
        self._open: _OpenMultiTxn | None = None
        self._anonymous_seq = 0

    @property
    def open_xid(self) -> str | None:
        return self._open.xid if self._open else None

    @property
    def explicit_txn(self) -> bool:
        """True when BEGIN was seen; False for autocommit/implicit row events."""
        return bool(self._open and self._open.explicit)

    @property
    def open_event_count(self) -> int:
        return int(self._open.event_count) if self._open else 0

    def begin(self, xid: str | None = None, *, lsn: str | None = None, explicit: bool = True) -> None:
        if self._open is not None:
            if lsn:
                self._open.last_lsn = lsn
            if explicit:
                self._open.explicit = True
            return
        self._anonymous_seq += 1
        self._open = _OpenMultiTxn(
            xid=xid or f"anon-{self._anonymous_seq}",
            last_lsn=lsn,
            explicit=explicit,
        )

    def _bucket(self, table: str) -> _TableBuf:
        assert self._open is not None
        key = table or ""
        if key not in self._open.by_table:
            self._open.by_table[key] = _TableBuf()
        return self._open.by_table[key]

    def _ensure_open(self, lsn: str | None) -> None:
        if self._open is None:
            # Row events without BEGIN → implicit autocommit window.
            self.begin(lsn=lsn, explicit=False)
        elif lsn:
            self._open.last_lsn = lsn

    def insert(self, table: str, row: dict[str, Any], *, lsn: str | None = None) -> None:
        self._ensure_open(lsn)
        assert self._open is not None
        self._bucket(table).inserts.append(row)
        self._open.event_count += 1
        self._maybe_overflow()

    def update(self, table: str, row: dict[str, Any], *, lsn: str | None = None) -> None:
        self._ensure_open(lsn)
        assert self._open is not None
        self._bucket(table).updates.append(row)
        self._open.event_count += 1
        self._maybe_overflow()

    def delete(self, table: str, pk: str, *, lsn: str | None = None) -> None:
        if not pk:
            return
        self._ensure_open(lsn)
        assert self._open is not None
        self._bucket(table).deletes.append(pk)
        self._open.event_count += 1
        self._maybe_overflow()

    def _maybe_overflow(self) -> None:
        if self._open and self._open.event_count > self.max_events:
            raise CdcTxnBufferOverflow(
                f"CDC multi-table txn buffer exceeded max_events={self.max_events} "
                f"(xid={self._open.xid}, events={self._open.event_count}); "
                "increase DATAFLOW_CDC_TXN_BUFFER_MAX_EVENTS or reduce txn size — "
                "refusing silent truncation (open txn held for redelivery)",
                xid=self._open.xid,
                max_events=self.max_events,
                event_count=self._open.event_count,
            )

    def commit(
        self,
        *,
        lsn: str | None = None,
        resume_token: Any = None,
        table_order: list[str] | None = None,
    ) -> list[ChangeBatch]:
        """Return demuxed batches; last non-empty (or sole) batch has ``ack_barrier``."""
        if self._open is None:
            if resume_token is not None:
                return [ChangeBatch(resume_token=resume_token, ack_barrier=True)]
            return []
        if lsn:
            self._open.last_lsn = lsn

        order = list(table_order or [])
        seen = {t.lower(): t for t in order}
        for name in self._open.by_table:
            if name.lower() not in seen:
                order.append(name)

        batches: list[ChangeBatch] = []
        for name in order:
            buf = self._open.by_table.get(name)
            if not buf:
                continue
            if not (buf.inserts or buf.updates or buf.deletes):
                continue
            batches.append(
                ChangeBatch(
                    inserts=list(buf.inserts),
                    updates=list(buf.updates),
                    deletes=list(buf.deletes),
                    resume_token=resume_token,
                    table=name,
                    ack_barrier=False,
                )
            )
        self._open = None
        if not batches:
            if resume_token is not None:
                return [ChangeBatch(resume_token=resume_token, ack_barrier=True)]
            return []
        batches[-1].ack_barrier = True
        return batches

    def rollback(self) -> None:
        self._open = None


_TEST_DECODING_TABLE_RE = re.compile(
    r"^table\s+([^\s.]+)\.([^\s:]+):\s*(INSERT|UPDATE|DELETE):",
    re.IGNORECASE,
)


def parse_test_decoding_table(line: str) -> tuple[str, str] | None:
    """Return ``(schema, table)`` from a test_decoding change line."""
    m = _TEST_DECODING_TABLE_RE.match((line or "").strip())
    if not m:
        return None
    return m.group(1), m.group(2)


def should_ack_shared_batch(change: ChangeBatch) -> bool:
    """Return True only when shared-reader apply may advance the log watermark.

    Chaos invariant: never ack after a demuxed mid-txn table batch
    (``ack_barrier=False``). Side-channel / empty heartbeats with
    ``ack_barrier=True`` may ack to release WAL when there is a resume token.
    """
    if change is None:
        return False
    if not change.ack_barrier:
        return False
    return change.resume_token is not None


@dataclass
class SharedApplyChaosReport:
    """Result of a crash/redelivery simulation for shared multi-table apply."""

    applied_tables: list[str] = field(default_factory=list)
    ack_calls: list[Any] = field(default_factory=list)
    early_ack: bool = False
    redelivered: bool = False
    final_tables: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (not self.early_ack) and bool(self.final_tables) and len(self.ack_calls) >= 1


def simulate_shared_apply_chaos(
    batches: list[ChangeBatch],
    *,
    crash_before_barrier: bool = True,
) -> SharedApplyChaosReport:
    """Simulate crash mid-demux then redelivery — prove ack stays behind barrier.

    Algorithm
    ---------
    1. Apply batches in order until the first data-bearing ``ack_barrier``
       (exclusive when ``crash_before_barrier``).
    2. Any ``should_ack_shared_batch`` on a non-barrier batch → ``early_ack``.
    3. Redeliver the full window; ack only on barrier batches.
    """
    report = SharedApplyChaosReport()
    pending: list[ChangeBatch] = list(batches)

    for batch in pending:
        if crash_before_barrier and batch.ack_barrier and batch.total_changes:
            # Crash after prior non-barrier tables, before barrier apply/ack.
            break
        if batch.table and batch.total_changes:
            report.applied_tables.append(batch.table)
        if should_ack_shared_batch(batch):
            if not batch.ack_barrier:
                report.early_ack = True
            report.ack_calls.append(batch.resume_token)

    report.redelivered = True
    for batch in pending:
        if batch.table and batch.total_changes:
            report.final_tables.append(batch.table)
        if should_ack_shared_batch(batch):
            if not batch.ack_barrier:
                report.early_ack = True
            report.ack_calls.append(batch.resume_token)
    return report


def iter_ack_ready(batches: list[ChangeBatch]) -> Iterator[ChangeBatch]:
    """Yield batches; callers ack only when ``should_ack_shared_batch`` is true."""
    yield from batches
