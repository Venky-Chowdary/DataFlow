"""PostgreSQL CDC transport selection (Phase F4).

Two transports share the same at-least-once contract (peek/buffer → apply → ack):

* ``peek`` (default) — ``pg_logical_slot_peek_*_changes`` + ``pg_replication_slot_advance``.
  Correct, but re-decodes WAL from ``confirmed_flush_lsn`` on every poll (audit §4.4).
* ``streaming`` — ``START_REPLICATION`` on a logical replication connection; WAL is
  decoded once. Confirmed LSN feedback is sent **only** from :meth:`ack` after
  destination apply — never on receive.

Enable streaming with ``DATAFLOW_CDC_PG_TRANSPORT=streaming``. Falls back to
``peek`` when the replication connection cannot be opened.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from services.brand_env import getenv_brand

_logger = logging.getLogger(__name__)


def selected_pg_cdc_transport() -> str:
    raw = (getenv_brand("CDC_PG_TRANSPORT", "peek") or "peek").strip().lower()
    if raw in ("stream", "streaming", "replication", "start_replication"):
        return "streaming"
    return "peek"


@dataclass
class PeekedChange:
    lsn: str
    payload: Any  # bytes for pgoutput, str for test_decoding


@dataclass
class StreamingBuffer:
    """In-memory buffer of replication messages awaiting destination ack."""

    items: list[PeekedChange] = field(default_factory=list)
    last_received_lsn: str = ""
    lock: threading.RLock = field(default_factory=threading.RLock)

    def extend(self, changes: list[PeekedChange]) -> None:
        with self.lock:
            self.items.extend(changes)
            if changes:
                self.last_received_lsn = changes[-1].lsn

    def drain_upto(self, limit: int) -> list[PeekedChange]:
        with self.lock:
            out = self.items[:limit]
            self.items = self.items[limit:]
            return out

    def drop_upto_lsn(self, lsn: str) -> int:
        """Remove buffered messages with LSN <= ``lsn`` after successful apply."""
        from connectors.writer_common import compare_lsn

        with self.lock:
            keep: list[PeekedChange] = []
            dropped = 0
            for item in self.items:
                if compare_lsn(item.lsn, lsn) <= 0:
                    dropped += 1
                else:
                    keep.append(item)
            self.items = keep
            return dropped


class StreamingReplicationTransport:
    """Logical replication consumer with apply-gated feedback (experimental)."""

    def __init__(
        self,
        *,
        dsn_kwargs: dict[str, Any],
        slot_name: str,
        publication_name: str,
        output_plugin: str = "pgoutput",
    ) -> None:
        self.dsn_kwargs = dsn_kwargs
        self.slot_name = slot_name
        self.publication_name = publication_name
        self.output_plugin = output_plugin
        self._buffer = StreamingBuffer()
        self._conn: Any = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._ack_lsn = ""
        self._started = False
        self._error: BaseException | None = None

    def start(self) -> None:
        if self._started:
            return
        import psycopg2
        from psycopg2.extras import LogicalReplicationConnection

        self._conn = psycopg2.connect(
            connection_factory=LogicalReplicationConnection,
            **{k: v for k, v in self.dsn_kwargs.items() if v is not None},
        )
        cur = self._conn.cursor()
        options = {
            "proto_version": "1",
            "publication_names": self.publication_name,
        }
        # decode=False for pgoutput binary; test_decoding uses text.
        decode = self.output_plugin != "pgoutput"
        cur.start_replication(
            slot_name=self.slot_name,
            decode=decode,
            options=options if self.output_plugin == "pgoutput" else None,
            status_interval=10,
        )
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._consume_loop,
            args=(cur,),
            name=f"pg-repl-{self.slot_name}",
            daemon=True,
        )
        self._thread.start()
        self._started = True
        _logger.info(
            "CDC streaming transport started slot=%s publication=%s",
            self.slot_name,
            self.publication_name,
        )

    def _consume_loop(self, cur: Any) -> None:
        def _consume(msg: Any) -> None:
            if self._stop.is_set():
                raise StopIteration
            payload = msg.payload
            lsn = getattr(msg, "data_start", None) or getattr(msg, "wal_end", None)
            lsn_s = ""
            if lsn is not None:
                # psycopg2 may expose int LSN — convert via format if needed
                try:
                    from psycopg2.extras import LSN

                    lsn_s = str(LSN(int(lsn))) if not isinstance(lsn, str) else str(lsn)
                except Exception:
                    lsn_s = str(lsn)
            if payload is None:
                return
            self._buffer.extend([PeekedChange(lsn=lsn_s, payload=payload)])
            # Feedback only for already-acked LSN (at-least-once).
            if self._ack_lsn:
                try:
                    msg.cursor.send_feedback(flush_lsn=self._ack_lsn)
                except Exception as exc:
                    _logger.debug("replication feedback deferred: %s", exc)

        try:
            while not self._stop.is_set():
                cur.consume_stream(_consume, keepalive_interval=10)
        except StopIteration:
            return
        except Exception as exc:
            self._error = exc
            _logger.exception("CDC streaming transport failed: %s", exc)

    def poll(self, *, limit: int = 1000) -> list[PeekedChange]:
        if self._error is not None:
            raise RuntimeError(f"CDC streaming transport error: {self._error}") from self._error
        if not self._started:
            self.start()
        # Brief wait so the consumer thread can fill the buffer.
        deadline = time.time() + 0.05
        while time.time() < deadline and not self._buffer.items:
            time.sleep(0.005)
        return self._buffer.drain_upto(limit)

    def ack(self, lsn: str) -> None:
        """Record confirmed flush LSN — feedback goes out on next keepalive/message."""
        if not lsn:
            return
        self._ack_lsn = lsn
        self._buffer.drop_upto_lsn(lsn)

    def close(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
        self._started = False


def open_streaming_transport_or_none(
    *,
    dsn_kwargs: dict[str, Any],
    slot_name: str,
    publication_name: str,
    output_plugin: str = "pgoutput",
) -> StreamingReplicationTransport | None:
    """Best-effort streaming open; ``None`` means caller must use peek."""
    if selected_pg_cdc_transport() != "streaming":
        return None
    try:
        transport = StreamingReplicationTransport(
            dsn_kwargs=dsn_kwargs,
            slot_name=slot_name,
            publication_name=publication_name,
            output_plugin=output_plugin,
        )
        transport.start()
        return transport
    except Exception as exc:
        _logger.warning(
            "CDC streaming transport unavailable (%s) — using peek (at-least-once preserved)",
            exc,
        )
        return None
