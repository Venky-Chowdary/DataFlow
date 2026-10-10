"""Oracle LogMiner transaction buffer — commit-ordered emission (opt-in).

``COMMITTED_DATA_ONLY`` mining only shows a transaction once it commits, and
LogMiner reports those rows at their original DML SCNs. A reader whose cursor
has already passed those SCNs (because a *later* transaction committed first)
never sees them again — the long-transaction data-loss risk.

With ``DATAFLOW_CDC_ORACLE_TXN_BUFFER`` on, the reader mines uncommitted redo
(``DICT_FROM_ONLINE_CATALOG`` only), buffers DML per transaction id (XID) and
emits a transaction **only on COMMIT**, in commit order. Open transactions are
never persisted: the resume token carries ``low_scn`` (the oldest open start
SCN) so a restart re-mines them, and the commit cursor suppresses commits that
were already emitted.

Bounds are hard: exceeding open-transaction count, buffered bytes or an
abandoned-transaction age raises :class:`CdcTxnBufferOverflow`. This buffer
never spills to disk (unlike ``services.cdc_transaction_buffer.TransactionBuffer``)
and never drops an event.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any

from services.brand_env import getenv_brand
from services.cdc_transaction_buffer import CdcTxnBufferOverflow

_TRUTHY = frozenset({"1", "true", "yes", "on"})

#: Operations that delimit a transaction in ``V$LOGMNR_CONTENTS``.
TXN_START_OPS = frozenset({"START"})
TXN_COMMIT_OPS = frozenset({"COMMIT"})
TXN_ROLLBACK_OPS = frozenset({"ROLLBACK"})

DEFAULT_MAX_OPEN_TXNS = 1000
DEFAULT_MAX_BYTES = 256 * 1024 * 1024


def oracle_txn_buffer_enabled() -> bool:
    """True when the opt-in Oracle transaction buffer is switched on.

    Default OFF — the legacy committed-data-only path stays byte-identical.
    """
    raw = getenv_brand("CDC_ORACLE_TXN_BUFFER", "")
    return str(raw or "").strip().lower() in _TRUTHY


def _env_int(name: str, default: int) -> int:
    raw = getenv_brand(name, str(default))
    try:
        value = int(str(raw).strip() or default)
    except (TypeError, ValueError):
        return default
    return value if value >= 0 else default


def oracle_txn_max_open() -> int:
    value = _env_int("CDC_ORACLE_TXN_MAX_OPEN", DEFAULT_MAX_OPEN_TXNS)
    return value or DEFAULT_MAX_OPEN_TXNS


def oracle_txn_max_bytes() -> int:
    value = _env_int("CDC_ORACLE_TXN_MAX_BYTES", DEFAULT_MAX_BYTES)
    return value or DEFAULT_MAX_BYTES


def oracle_txn_max_age_scn() -> int:
    """SCN span after which an open transaction is treated as abandoned (0 = off)."""
    return _env_int("CDC_ORACLE_TXN_MAX_AGE_SCN", 0)


def oracle_txn_max_age_seconds() -> int:
    """Wall-clock age after which an open transaction is abandoned (0 = off)."""
    return _env_int("CDC_ORACLE_TXN_MAX_AGE_SECONDS", 0)


def rs_id_sort_key(rs_id: str) -> tuple:
    """Numeric ``(seq, blk, off)`` for a LogMiner RS_ID.

    RS_ID components are hex with site-dependent widths, so a string compare
    mis-orders ``0x9…`` against ``0x10…``.
    """
    raw = str(rs_id or "").strip()
    if not raw:
        return (0, 0, 0, 0, "")
    parts = [part.strip() for part in raw.split(".")]
    if len(parts) == 3:
        if parts[0].lower().startswith("0x"):
            parts[0] = parts[0][2:]
        if all(re.fullmatch(r"[0-9a-f]+", part, flags=re.IGNORECASE) for part in parts):
            return (1, int(parts[0], 16), int(parts[1], 16), int(parts[2], 16), "")
    raise ValueError(f"Invalid Oracle LogMiner RS_ID {rs_id!r}; refusing opaque ordering")


def mining_position(scn: int, rs_id: str, ssn: int) -> tuple:
    """LogMiner's total order ``(SCN, RS_ID, SSN)`` as a comparable key."""
    return (int(scn or 0), rs_id_sort_key(rs_id), int(ssn or 0))


@dataclass(frozen=True)
class OracleTxnRow:
    """One buffered DML record in mining order."""

    scn: int
    rs_id: str
    ssn: int
    op: str
    table: str
    row: dict[str, Any]
    rollback: bool = False
    row_id: str = ""


@dataclass
class OracleCommittedTxn:
    """A transaction released by its COMMIT record."""

    xid: str
    start_scn: int
    commit_scn: int
    commit_rs_id: str
    commit_ssn: int
    rows: list[OracleTxnRow] = field(default_factory=list)

    @property
    def position(self) -> tuple:
        return mining_position(self.commit_scn, self.commit_rs_id, self.commit_ssn)


@dataclass
class _OpenTxn:
    xid: str
    start_scn: int
    rows: list[OracleTxnRow] = field(default_factory=list)
    nbytes: int = 0
    first_seen: float = 0.0
    rowid_pks: dict[tuple[str, str], tuple[str, Any]] = field(default_factory=dict)


def _row_bytes(op: str, table: str, row: dict[str, Any], row_id: str = "") -> int:
    encoded = json.dumps(
        {"op": op, "table": table, "row": row or {}, "row_id": row_id},
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return len(encoded) + 128


class OracleTxnBuffer:
    """Buffer Oracle DML by XID and release whole transactions on COMMIT.

    Feed rows in mining order. ``feed`` returns the transactions released by
    that row (at most one), already filtered against the emitted-commit cursor
    so a restart that re-mines from ``low_scn`` does not re-emit.
    """

    def __init__(
        self,
        *,
        max_txns: int | None = None,
        max_bytes: int | None = None,
        max_age_scn: int | None = None,
        max_age_seconds: int | None = None,
        emitted_position: tuple | None = None,
    ) -> None:
        self.max_txns = int(max_txns) if max_txns else oracle_txn_max_open()
        self.max_bytes = int(max_bytes) if max_bytes else oracle_txn_max_bytes()
        self.max_age_scn = (
            int(max_age_scn) if max_age_scn is not None else oracle_txn_max_age_scn()
        )
        self.max_age_seconds = (
            int(max_age_seconds)
            if max_age_seconds is not None
            else oracle_txn_max_age_seconds()
        )
        self.emitted_position = emitted_position
        self._open: dict[str, _OpenTxn] = {}
        self._bytes = 0

    @property
    def open_txn_count(self) -> int:
        return len(self._open)

    @property
    def buffered_bytes(self) -> int:
        return self._bytes

    @property
    def low_scn(self) -> int:
        """Oldest open transaction start SCN — the restart mining point."""
        if not self._open:
            return 0
        return min(txn.start_scn for txn in self._open.values())

    def _overflow(self, message: str, *, xid: str, event_count: int) -> CdcTxnBufferOverflow:
        return CdcTxnBufferOverflow(
            message,
            xid=xid,
            max_events=self.max_txns,
            event_count=event_count,
        )

    def _ensure_open(self, xid: str, scn: int) -> _OpenTxn:
        txn = self._open.get(xid)
        if txn is None:
            if len(self._open) >= self.max_txns:
                raise self._overflow(
                    "Oracle CDC transaction buffer exceeded "
                    f"max_open_txns={self.max_txns} (open={len(self._open) + 1}, "
                    f"xid={xid}); a long-running or abandoned source transaction "
                    "is holding redo. Remedy: commit or kill the oldest source "
                    "transaction (V$TRANSACTION), or raise "
                    "DATAFLOW_CDC_ORACLE_TXN_MAX_OPEN — refusing to drop or "
                    "spill buffered changes.",
                    xid=xid,
                    event_count=len(self._open) + 1,
                )
            txn = _OpenTxn(
                xid=xid,
                start_scn=int(scn or 0),
                first_seen=time.monotonic(),
            )
            self._open[xid] = txn
        return txn

    def feed(
        self,
        *,
        xid: str,
        scn: int,
        rs_id: str = "",
        ssn: int = 0,
        operation: str,
        table: str = "",
        row: dict[str, Any] | None = None,
        rollback: bool = False,
        row_id: str = "",
        primary_key: str = "",
    ) -> list[OracleCommittedTxn]:
        key = str(xid or "").strip()
        if not key:
            key = f"scn:{int(scn or 0)}"
        op = str(operation or "").strip().upper()

        if op in TXN_START_OPS:
            self._ensure_open(key, scn)
            return []
        if op in TXN_ROLLBACK_OPS:
            # Full rollback — the transaction never happened on the source.
            dropped = self._open.pop(key, None)
            if dropped is not None:
                self._bytes -= dropped.nbytes
            return []
        if op in TXN_COMMIT_OPS:
            committed = self._open.pop(key, None)
            if committed is None:
                return []
            self._bytes -= committed.nbytes
            if not committed.rows:
                return []
            txn = OracleCommittedTxn(
                xid=key,
                start_scn=committed.start_scn,
                commit_scn=int(scn or 0),
                commit_rs_id=str(rs_id or ""),
                commit_ssn=int(ssn or 0),
                rows=list(committed.rows),
            )
            if self.emitted_position is not None and txn.position <= self.emitted_position:
                # Already emitted before the restart — re-mining must not duplicate.
                return []
            return [txn]

        open_txn = self._ensure_open(key, scn)
        row_values = dict(row or {})
        row_id_value = str(row_id or row_values.get("ROWID") or "")
        primary_key_name = str(primary_key or "")
        table_key = str(table or "").upper()
        if primary_key_name and primary_key_name not in row_values:
            cached_pk = open_txn.rowid_pks.get((table_key, row_id_value))
            if cached_pk is not None:
                row_values[cached_pk[0]] = cached_pk[1]
            else:
                row_values = {
                    "_df_rejected": {
                        "table": str(table or ""),
                        "reason": (
                            "Oracle LogMiner redo lacks the configured primary key "
                            f"{primary_key_name!r} and no earlier ROW_ID mapping exists."
                        ),
                        "row_id": row_id_value,
                        "xid": key,
                    }
                }
                op = "unparsed"
        for column in tuple(row_values):
            if column.upper() == "ROWID":
                row_values.pop(column)
        if (
            primary_key_name
            and row_id_value
            and primary_key_name in row_values
            and row_values[primary_key_name] is not None
        ):
            open_txn.rowid_pks[(table_key, row_id_value)] = (
                primary_key_name,
                row_values[primary_key_name],
            )
        record = OracleTxnRow(
            scn=int(scn or 0),
            rs_id=str(rs_id or ""),
            ssn=int(ssn or 0),
            op=op.lower(),
            table=str(table or ""),
            row=row_values,
            rollback=bool(rollback),
            row_id=row_id_value,
        )
        size = _row_bytes(record.op, record.table, record.row, record.row_id)
        if self._bytes + size > self.max_bytes:
            raise self._overflow(
                "Oracle CDC transaction buffer exceeded "
                f"max_bytes={self.max_bytes} "
                f"(buffered={self._bytes + size}, xid={key}); "
                "an oversized source transaction is open. Remedy: commit the "
                "source transaction, lower its size, or raise "
                "DATAFLOW_CDC_ORACLE_TXN_MAX_BYTES — refusing to spill to disk "
                "or drop buffered changes.",
                xid=key,
                event_count=len(open_txn.rows) + 1,
            )
        open_txn.rows.append(record)
        open_txn.nbytes += size
        self._bytes += size
        return []

    def check_abandoned(self, *, head_scn: int = 0) -> None:
        """Raise when an open transaction exceeds the configured age bounds."""
        if not self._open:
            return
        now = time.monotonic()
        for xid, txn in self._open.items():
            if self.max_age_scn and int(head_scn or 0) - txn.start_scn > self.max_age_scn:
                raise self._overflow(
                    f"Oracle CDC transaction {xid} has been open for "
                    f"{int(head_scn or 0) - txn.start_scn} SCNs (limit "
                    f"{self.max_age_scn}); it looks abandoned and is pinning "
                    "redo. Remedy: commit or kill the source session "
                    "(V$TRANSACTION / ALTER SYSTEM KILL SESSION), or raise "
                    "DATAFLOW_CDC_ORACLE_TXN_MAX_AGE_SCN — buffered changes are "
                    "never dropped.",
                    xid=xid,
                    event_count=len(txn.rows),
                )
            if self.max_age_seconds and now - txn.first_seen > self.max_age_seconds:
                raise self._overflow(
                    f"Oracle CDC transaction {xid} has been open for "
                    f"{int(now - txn.first_seen)}s (limit {self.max_age_seconds}s); "
                    "it looks abandoned and is pinning redo. Remedy: commit or "
                    "kill the source session, or raise "
                    "DATAFLOW_CDC_ORACLE_TXN_MAX_AGE_SECONDS — buffered changes "
                    "are never dropped.",
                    xid=xid,
                    event_count=len(txn.rows),
                )


def chunk_txn_rows(rows: list[OracleTxnRow], batch_size: int) -> list[list[OracleTxnRow]]:
    """Split one committed transaction into consecutive emit chunks."""
    size = max(1, int(batch_size or 1))
    if not rows:
        return []
    return [rows[i : i + size] for i in range(0, len(rows), size)]
