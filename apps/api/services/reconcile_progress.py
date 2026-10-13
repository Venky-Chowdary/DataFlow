"""Rows fingerprinted during a Gate-8 checksum, for the reconcile heartbeat.

The checksum runs on the transfer thread. The heartbeat thread only reads
this counter. Nothing here writes to the job document — a status update per
row is what made a million-row reconcile look stuck at 99% while it was
still hashing. The digest itself is unchanged.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass


@dataclass
class ReconcileScan:
    """Mutable scan counter. The heartbeat thread and the hasher share it."""

    rows_hashed: int = 0


_current: ContextVar[ReconcileScan | None] = ContextVar(
    "df_reconcile_scan", default=None
)


def bind_reconcile_scan(scan: ReconcileScan) -> Token[ReconcileScan | None]:
    return _current.set(scan)


def reset_reconcile_scan(token: Token[ReconcileScan | None]) -> None:
    _current.reset(token)


def note_hashed_rows(count: int) -> None:
    """Add ``count`` fingerprinted rows to the active scan, if one is bound.

    No active scan means a checksum outside reconcile (a unit digest, a
    sample). That path stays silent.
    """
    if count <= 0:
        return
    scan = _current.get()
    if scan is None:
        return
    scan.rows_hashed += int(count)
