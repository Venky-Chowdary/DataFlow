"""PostgreSQL CDC transport selection (Phase F4).

Two transports share the same at-least-once contract (read → apply → ack):

* ``peek`` — ``pg_logical_slot_peek_*_changes`` + ``pg_replication_slot_advance``.
  Correct, but re-decodes WAL from ``confirmed_flush_lsn`` on every poll.
* ``streaming`` — ``START_REPLICATION`` on a logical replication connection; WAL is
  decoded once by the server. Confirmed-flush feedback is sent **only** for LSNs
  passed to :meth:`StreamingReplicationTransport.ack` after destination apply.

Select with ``DATAFLOW_CDC_PG_TRANSPORT``: ``auto`` (default — streaming, falling
back to peek when a replication connection cannot be opened, e.g. the role lacks
REPLICATION or pg_hba has no replication entry), ``streaming`` or ``peek``.

Streaming keeps peek semantics so the change stream does not care which one runs:

* :meth:`poll` returns *complete* transactions from the head of the unacked
  buffer and does not remove them. Re-polling without an ack redelivers them,
  exactly like a second ``peek``.
* :meth:`ack` drops the buffer prefix through the acked COMMIT. It never drops
  by comparing LSNs of individual messages: a transaction that began before an
  earlier one committed carries a smaller BEGIN/change LSN than that commit, and
  dropping ``lsn <= ack`` would silently discard it.
* On a transient connection failure the transport reconnects with
  ``start_lsn`` = last ack, so committed work is not resent and unacked work is.
"""

from __future__ import annotations

import logging
import select
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from services.brand_env import getenv_brand

_logger = logging.getLogger(__name__)

TRANSPORT_PEEK = "peek"
TRANSPORT_STREAMING = "streaming"
TRANSPORT_AUTO = "auto"


class CdcStreamingTransportError(RuntimeError):
    """The replication connection failed and cannot be recovered (fail closed)."""


def selected_pg_cdc_transport() -> str:
    raw = (getenv_brand("CDC_PG_TRANSPORT", TRANSPORT_AUTO) or TRANSPORT_AUTO).strip().lower()
    if raw in ("stream", "streaming", "replication", "start_replication"):
        return TRANSPORT_STREAMING
    if raw in ("peek", "poll", "sql"):
        return TRANSPORT_PEEK
    if raw != TRANSPORT_AUTO:
        _logger.warning("Unknown DATAFLOW_CDC_PG_TRANSPORT=%r; using auto", raw)
    return TRANSPORT_AUTO


def lsn_to_int(lsn: str | int | None) -> int:
    if lsn is None or lsn == "":
        return 0
    if isinstance(lsn, int):
        return lsn
    hi, _, lo = str(lsn).strip().partition("/")
    if not lo:
        raise ValueError(f"not a PostgreSQL LSN: {lsn!r}")
    return (int(hi, 16) << 32) | int(lo, 16)


def int_to_lsn(value: int) -> str:
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"


def _env_float(name: str, default: float) -> float:
    raw = getenv_brand(name, str(default))
    try:
        return float(raw)
    except (TypeError, ValueError):
        _logger.warning("Invalid %s=%r; using %s", name, raw, default)
        return default


def _is_commit(payload: Any) -> bool:
    # pgoutput (binary): Commit message type byte. test_decoding (text): "COMMIT ...".
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return bytes(payload[:1]) == b"C"
    return str(payload or "").lstrip().upper().startswith("COMMIT")


@dataclass
class PeekedChange:
    lsn: str
    payload: Any  # bytes for pgoutput, str for test_decoding
    is_commit: bool = False


@dataclass
class StreamingBuffer:
    """Unacked replication messages in server send order (peek semantics)."""

    items: list[PeekedChange] = field(default_factory=list)
    last_received_lsn: str = ""
    # Ack for a commit not received yet (after a reconnect): drop it on arrival.
    skip_through_lsn: str = ""
    lock: threading.RLock = field(default_factory=threading.RLock)

    def extend(self, changes: list[PeekedChange]) -> None:
        with self.lock:
            self.items.extend(changes)
            if changes:
                self.last_received_lsn = changes[-1].lsn
            if self.skip_through_lsn:
                self._drop_prefix_through(self.skip_through_lsn)

    def committed(self, limit: int | None = None) -> list[PeekedChange]:
        """Complete transactions from the head; at least one when one is buffered.

        Like ``upto_nchanges`` on a peek: stop at the first COMMIT at or past
        ``limit`` messages, never mid-transaction.
        """
        with self.lock:
            end = 0
            for idx, item in enumerate(self.items):
                if item.is_commit:
                    end = idx + 1
                    if limit is not None and end >= limit:
                        break
            return list(self.items[:end])

    def has_committed(self) -> bool:
        with self.lock:
            return any(item.is_commit for item in self.items)

    def _drop_prefix_through(self, lsn: str) -> int:
        from connectors.writer_common import compare_lsn

        cut = -1
        for idx, item in enumerate(self.items):
            if not item.is_commit:
                continue
            if compare_lsn(item.lsn, lsn) <= 0:
                cut = idx
            else:
                break
        if cut < 0:
            return 0
        self.items = self.items[cut + 1 :]
        return cut + 1

    def drop_through_commit(self, lsn: str) -> int:
        """Drop every message through the last COMMIT at or before ``lsn``.

        Commit LSNs are monotonic in decode order; BEGIN/change LSNs are not.
        """
        from connectors.writer_common import compare_lsn

        with self.lock:
            dropped = self._drop_prefix_through(lsn)
            if not self.skip_through_lsn or compare_lsn(lsn, self.skip_through_lsn) > 0:
                self.skip_through_lsn = lsn
            return dropped

    def clear(self) -> None:
        with self.lock:
            self.items = []


def _transient(exc: BaseException) -> bool:
    """Connection-class failures are retried; anything else fails closed."""
    import psycopg2

    if isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError)):
        return True
    code = str(getattr(exc, "pgcode", "") or "")
    if not code:
        return isinstance(exc, psycopg2.DatabaseError)
    # 08 connection exception, 57P admin shutdown/terminate, 55006 slot still
    # active for a just-killed walsender.
    return code.startswith("08") or code.startswith("57P") or code == "55006"


class StreamingReplicationTransport:
    """Logical replication consumer with apply-gated confirmed-flush feedback."""

    def __init__(
        self,
        *,
        dsn_kwargs: dict[str, Any],
        slot_name: str,
        publication_name: str,
        output_plugin: str = "pgoutput",
        start_lsn: str = "",
    ) -> None:
        self.dsn_kwargs = dsn_kwargs
        self.slot_name = slot_name
        self.publication_name = publication_name
        self.output_plugin = output_plugin
        self._buffer = StreamingBuffer()
        self._conn: Any = None
        self._cur: Any = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._state = threading.Lock()
        self._ack_lsn = start_lsn or ""
        self._flushed_lsn = ""
        self._server_wal_end = 0
        self._last_begin_lsn = ""
        self._idle_release_requested = False
        self._reply_requested = False
        self._started = False
        self._error: BaseException | None = None
        self.reconnects = 0
        self.last_message_at: datetime | None = None
        self.last_feedback_at: datetime | None = None
        self._max_reconnects = int(_env_float("CDC_PG_STREAM_RECONNECT_MAX", 5))
        self._backoff_cap = _env_float("CDC_PG_STREAM_RECONNECT_BACKOFF_MAX_SEC", 30.0)
        self._poll_wait = _env_float("CDC_PG_STREAM_POLL_WAIT_SEC", 0.25)

    # -- connection -------------------------------------------------------
    def _open(self) -> None:
        import psycopg2
        from psycopg2.extras import LogicalReplicationConnection

        conn = psycopg2.connect(
            connection_factory=LogicalReplicationConnection,
            **{k: v for k, v in self.dsn_kwargs.items() if v is not None},
        )
        cur = conn.cursor()
        pgoutput = self.output_plugin == "pgoutput"
        cur.start_replication(
            slot_name=self.slot_name,
            decode=not pgoutput,
            start_lsn=self._ack_lsn or 0,
            options={"proto_version": "1", "publication_names": self.publication_name}
            if pgoutput
            else {"include-xids": "1"},
            status_interval=10,
        )
        self._conn, self._cur = conn, cur

    def _close_conn(self) -> None:
        import psycopg2

        conn, self._conn, self._cur = self._conn, None, None
        if conn is None:
            return
        try:
            conn.close()
        except psycopg2.Error as exc:
            _logger.warning("CDC streaming: closing replication connection failed: %s", exc)

    def start(self) -> None:
        if self._started:
            return
        self._open()
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name=f"pg-repl-{self.slot_name}", daemon=True
        )
        self._started = True
        self._thread.start()
        _logger.info(
            "CDC streaming transport started slot=%s publication=%s start_lsn=%s",
            self.slot_name,
            self.publication_name,
            self._ack_lsn or "slot confirmed_flush_lsn",
        )

    # -- consumer thread (sole owner of the replication cursor) ------------
    def _run(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                if self._cur is None:
                    self._open()
                    self.reconnects += 1
                    _logger.warning(
                        "CDC streaming reconnected slot=%s from start_lsn=%s (attempt %s)",
                        self.slot_name,
                        self._ack_lsn or "confirmed_flush_lsn",
                        failures,
                    )
                self._pump()
                failures = 0
            except Exception as exc:  # noqa: BLE001 — classified below, never swallowed
                self._close_conn()
                # Unacked work is resent from the ack position after reconnect.
                self._buffer.clear()
                if self._stop.is_set():
                    break
                failures += 1
                if not _transient(exc) or failures > self._max_reconnects:
                    self._error = exc
                    _logger.error(
                        "CDC streaming transport failed slot=%s after %s attempt(s): %s",
                        self.slot_name,
                        failures,
                        exc,
                    )
                    break
                delay = min(self._backoff_cap, 0.5 * (2 ** (failures - 1)))
                _logger.warning(
                    "CDC streaming connection lost slot=%s (%s); retry in %.1fs",
                    self.slot_name,
                    exc,
                    delay,
                )
                self._stop.wait(delay)
        try:
            if self._cur is not None:
                self._send_feedback(force=True)
        except Exception as exc:  # noqa: BLE001 — shutdown path; logged
            _logger.warning("CDC streaming final feedback failed slot=%s: %s", self.slot_name, exc)
        self._close_conn()

    def _pump(self) -> None:
        """Read until stop; send feedback when acks advance or idle release is asked."""
        cur = self._cur
        while not self._stop.is_set():
            msg = cur.read_message()
            if msg is not None:
                self._on_message(msg)
                continue
            if cur.wal_end:
                self._server_wal_end = max(self._server_wal_end, int(cur.wal_end))
            self._maybe_idle_release()
            self._send_feedback()
            select.select([cur], [], [], 0.5)

    def _on_message(self, msg: Any) -> None:
        start = int(msg.data_start or 0)
        if start:
            lsn = int_to_lsn(start)
        else:
            # Relation/type messages carry no position; they belong to the open txn.
            lsn = self._last_begin_lsn or int_to_lsn(int(msg.wal_end or 0))
        payload = msg.payload
        is_commit = _is_commit(payload)
        head = bytes(payload[:5]) if isinstance(payload, (bytes, bytearray, memoryview)) else str(payload)[:5].encode()
        if head[:1] == b"B" or head.upper().startswith(b"BEGIN"):
            self._last_begin_lsn = lsn
        self._server_wal_end = max(self._server_wal_end, int(msg.wal_end or 0))
        self.last_message_at = datetime.now(timezone.utc)
        self._buffer.extend([PeekedChange(lsn=lsn, payload=payload, is_commit=is_commit)])
        if is_commit:
            self._wake.set()

    def _maybe_idle_release(self) -> None:
        """Confirm the server's sent position when nothing is buffered or unacked.

        Every message before ``wal_end`` has been received (in-order stream) and
        an empty buffer means all of it was acked, so nothing can be skipped.
        """
        with self._state:
            if not self._idle_release_requested:
                return
            self._idle_release_requested = False
        if self._buffer.items or not self._server_wal_end:
            return
        from connectors.writer_common import compare_lsn

        target = int_to_lsn(self._server_wal_end)
        if self._ack_lsn and compare_lsn(self._ack_lsn, target) >= 0:
            return
        with self._state:
            self._ack_lsn = target
        _logger.info(
            "CDC streaming released WAL on idle slot %s: confirmed_flush_lsn -> %s",
            self.slot_name,
            target,
        )

    def _send_feedback(self, *, force: bool = False) -> None:
        with self._state:
            ack = self._ack_lsn
            reply = self._reply_requested
            self._reply_requested = False
        if not ack and not reply:
            return
        if ack == self._flushed_lsn and not force and not reply:
            return
        value = lsn_to_int(ack) if ack else 0
        self._cur.send_feedback(
            write_lsn=value, flush_lsn=value, apply_lsn=value, reply=reply, force=True
        )
        if ack and ack != self._flushed_lsn:
            _logger.debug("CDC streaming confirmed_flush feedback slot=%s lsn=%s", self.slot_name, ack)
        self._flushed_lsn = ack
        self.last_feedback_at = datetime.now(timezone.utc)

    # -- caller API -------------------------------------------------------
    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise CdcStreamingTransportError(
                f"CDC streaming transport for slot {self.slot_name!r} failed: {self._error}"
            ) from self._error

    def poll(self, *, limit: int = 1000) -> list[PeekedChange]:
        """Complete unacked transactions from the head (not removed until ack)."""
        self._raise_if_failed()
        if not self._started:
            self.start()
        if not self._buffer.has_committed():
            self._wake.clear()
            self._wake.wait(self._poll_wait)
            self._raise_if_failed()
        return self._buffer.committed(limit)

    def committed_through(self, target_lsn: str, *, timeout: float = 5.0) -> list[PeekedChange]:
        """Every complete unacked txn, after the stream has reached ``target_lsn``.

        Raises :class:`CdcStreamingTransportError` if the server position does
        not reach ``target_lsn`` in time — callers must not treat a lagging
        buffer as "no events" (that is the stale-overwrite bug).
        """
        self._raise_if_failed()
        if not self._started:
            self.start()
        target = lsn_to_int(target_lsn)
        deadline = time.monotonic() + timeout
        while self._server_wal_end < target:
            with self._state:
                self._reply_requested = True
            if time.monotonic() >= deadline:
                raise CdcStreamingTransportError(
                    f"CDC streaming slot {self.slot_name!r} did not reach {target_lsn} "
                    f"within {timeout:.1f}s (server sent through "
                    f"{int_to_lsn(self._server_wal_end)})"
                )
            time.sleep(0.05)
            self._raise_if_failed()
        return self._buffer.committed()

    def has_pending(self, target_lsn: str, *, timeout: float = 2.0) -> bool | None:
        """True if a complete txn is unread; False when proven caught up to target."""
        if self._buffer.has_committed():
            return True
        try:
            pending = self.committed_through(target_lsn, timeout=timeout)
        except CdcStreamingTransportError as exc:
            _logger.debug("CDC streaming pending check inconclusive: %s", exc)
            return None
        return bool(pending)

    def ack(self, lsn: str) -> None:
        """Record an applied commit LSN; feedback goes out from the consumer thread."""
        if not lsn:
            return
        from connectors.writer_common import compare_lsn

        self._raise_if_failed()
        dropped = self._buffer.drop_through_commit(lsn)
        with self._state:
            if not self._ack_lsn or compare_lsn(lsn, self._ack_lsn) > 0:
                self._ack_lsn = lsn
        _logger.debug(
            "CDC streaming ack slot=%s lsn=%s dropped=%s buffered=%s",
            self.slot_name,
            lsn,
            dropped,
            len(self._buffer.items),
        )

    def request_idle_release(self) -> None:
        with self._state:
            self._idle_release_requested = True

    def stats(self) -> dict[str, Any]:
        return {
            "cdc_transport": TRANSPORT_STREAMING,
            "stream_received_lsn": self._buffer.last_received_lsn or None,
            "stream_server_wal_end": int_to_lsn(self._server_wal_end) if self._server_wal_end else None,
            "stream_acked_lsn": self._ack_lsn or None,
            "stream_flushed_lsn": self._flushed_lsn or None,
            "stream_buffered_messages": len(self._buffer.items),
            "stream_reconnects": self.reconnects,
            "stream_last_message_at": self.last_message_at.isoformat() if self.last_message_at else None,
            "stream_last_feedback_at": self.last_feedback_at.isoformat() if self.last_feedback_at else None,
            "stream_error": str(self._error)[:300] if self._error else None,
        }

    def close(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=10)
            if thread.is_alive():
                _logger.warning("CDC streaming consumer for slot %s did not stop in 10s", self.slot_name)
        if thread is None:
            self._close_conn()
        self._started = False


def open_streaming_transport(
    *,
    dsn_kwargs: dict[str, Any],
    slot_name: str,
    publication_name: str,
    output_plugin: str = "pgoutput",
    start_lsn: str = "",
) -> StreamingReplicationTransport:
    transport = StreamingReplicationTransport(
        dsn_kwargs=dsn_kwargs,
        slot_name=slot_name,
        publication_name=publication_name,
        output_plugin=output_plugin,
        start_lsn=start_lsn,
    )
    transport.start()
    return transport


def open_streaming_transport_or_none(
    *,
    dsn_kwargs: dict[str, Any],
    slot_name: str,
    publication_name: str,
    output_plugin: str = "pgoutput",
    start_lsn: str = "",
) -> StreamingReplicationTransport | None:
    """Streaming when selected; ``None`` means the caller uses peek.

    ``streaming`` and ``auto`` both fall back to peek on an open failure (peek is
    the same at-least-once contract); the reason is logged at warning.
    """
    if selected_pg_cdc_transport() == TRANSPORT_PEEK:
        return None
    try:
        return open_streaming_transport(
            dsn_kwargs=dsn_kwargs,
            slot_name=slot_name,
            publication_name=publication_name,
            output_plugin=output_plugin,
            start_lsn=start_lsn,
        )
    except Exception as exc:  # noqa: BLE001 — fallback is explicit and logged
        _logger.warning(
            "CDC streaming transport unavailable for slot %s (%s) — using peek",
            slot_name,
            exc,
        )
        return None
