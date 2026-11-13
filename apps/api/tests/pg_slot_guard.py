"""Test-session guard: no logical replication slot outlives the pytest run that made it.

An inactive logical slot keeps every WAL segment from its restart LSN on. A
CDC test whose ``finally`` never ran (the run was killed with SIGTERM, or the
slot was still held by an open replication connection at teardown, so the
test's ``WHERE NOT active`` drop skipped it) left ``df_dataflow_cdcord_*``
holding 1.76 GB of WAL on the shared server until the disk filled.

What the guard records, and why it is safe on a shared server:

* only slots this process really created: the engine's creation site
  (``postgresql_change_stream._create_logical_slot``) on a real psycopg2
  connection, confirmed in ``pg_replication_slots`` right after; a fake
  cursor records nothing. Tests that create slots with raw SQL call
  :func:`track_created_slot`.
* the slot's identity at creation (database, plugin, restart LSN). At drop
  time the slot must still have that database and plugin and a restart LSN
  no lower than recorded; a foreign slot that merely shares the name is
  left alone.
* an active slot is ended only when its holder is a connection this process
  opened (its client port is one of this process's sockets); another
  process's backend is never terminated.

SIGTERM unwinds the session (``finally`` and ``pytest_sessionfinish`` run);
a second SIGTERM while slots are being dropped is ignored. SIGKILL cannot be
intercepted.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
from typing import Any

logger = logging.getLogger(__name__)

#: slot name -> {"params": dsn parameters, "database", "plugin", "restart_lsn"}
CREATED: dict[str, dict[str, Any]] = {}
_lock = threading.Lock()


def _is_real_connection(conn: Any) -> bool:
    try:
        import psycopg2.extensions
    except ImportError:  # pragma: no cover
        return False
    return isinstance(conn, psycopg2.extensions.connection)


def _identity(cur: Any, slot_name: str) -> dict[str, Any] | None:
    cur.execute(
        "SELECT database, plugin, restart_lsn::text FROM pg_replication_slots "
        "WHERE slot_name = %s", (slot_name,))
    row = cur.fetchone()
    if not row:
        return None
    return {"database": row[0], "plugin": row[1], "restart_lsn": row[2]}


def _record_from_connection(conn: Any, slot_name: str) -> None:
    if not _is_real_connection(conn):
        return  # a fake cursor in a unit test: nothing was created
    try:
        ident = _identity(conn.cursor(), slot_name)
        params = dict(conn.get_dsn_parameters())
    except Exception as exc:  # noqa: BLE001 - recording must never break the test
        logger.debug("slot guard: could not read %s: %s", slot_name, exc)
        return
    if ident is None:
        return
    with _lock:
        CREATED[str(slot_name)] = {**ident, "params": params}


def track_created_slot(slot_name: str) -> None:
    """Record a slot a test created with raw SQL on the test server."""
    try:
        conn = _connect({})
    except Exception as exc:  # noqa: BLE001 - no server: nothing to track
        logger.debug("slot guard: cannot track %s: %s", slot_name, exc)
        return
    try:
        conn.autocommit = True
        ident = _identity(conn.cursor(), slot_name)
        if ident is not None:
            with _lock:
                CREATED[str(slot_name)] = {**ident, "params": {}}
    finally:
        conn.close()


def install() -> None:
    """Wrap the engine's slot creation and make SIGTERM unwind the session."""
    try:
        import connectors.postgresql_change_stream as stream
    except ImportError:  # pragma: no cover - API root not importable
        return
    real = stream._create_logical_slot
    if getattr(real, "_slot_guard", False):
        return

    def recording(cur: Any, slot_name: str, plugin: str):
        out = real(cur, slot_name, plugin)
        _record_from_connection(getattr(cur, "connection", None), slot_name)
        return out

    recording._slot_guard = True  # type: ignore[attr-defined]
    stream._create_logical_slot = recording
    if threading.current_thread() is threading.main_thread():
        def _term(signum, _frame):
            raise KeyboardInterrupt(f"signal {signum}: dropping this session's CDC slots")

        if signal.getsignal(signal.SIGTERM) in (signal.SIG_DFL, None):
            signal.signal(signal.SIGTERM, _term)


def _connect(params: dict[str, Any]):
    import psycopg2

    return psycopg2.connect(
        host=params.get("host") or os.environ.get("PGHOST", "127.0.0.1"),
        port=int(params.get("port") or os.environ.get("PGPORT", "5432")),
        dbname=params.get("dbname") or os.environ.get("PGDATABASE", "dataflow"),
        user=params.get("user") or os.environ.get("PGUSER", "dataflow"),
        password=os.environ.get("PGPASSWORD", ""),
        connect_timeout=5,
    )


def _own_client_ports() -> set[int]:
    """Local TCP ports of this process's sockets (Linux /proc)."""
    inodes: set[str] = set()
    try:
        for fd in os.listdir("/proc/self/fd"):
            try:
                target = os.readlink(f"/proc/self/fd/{fd}")
            except OSError:
                continue
            if target.startswith("socket:["):
                inodes.add(target[8:-1])
    except OSError:
        return set()
    ports: set[int] = set()
    for table in ("/proc/self/net/tcp", "/proc/self/net/tcp6"):
        try:
            with open(table, encoding="ascii") as fh:
                next(fh, None)
                for line in fh:
                    cols = line.split()
                    if len(cols) > 9 and cols[9] in inodes:
                        ports.add(int(cols[1].rsplit(":", 1)[1], 16))
        except OSError:
            continue
    return ports


def drop_created_slots() -> list[str]:
    """Drop every slot this process created that is still provably its own."""
    main = threading.current_thread() is threading.main_thread()
    previous = signal.getsignal(signal.SIGTERM) if main else None
    if main:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)  # a second SIGTERM must not stop this
    try:
        return _drop_all()
    finally:
        if main:
            signal.signal(signal.SIGTERM, previous)


def _drop_all() -> list[str]:
    with _lock:
        pending = dict(CREATED)
    dropped: list[str] = []
    for name, rec in pending.items():
        try:
            conn = _connect(rec.get("params") or {})
        except Exception as exc:  # noqa: BLE001 - report, the server may be down
            logger.warning("slot guard: cannot connect to drop %s: %s", name, exc)
            continue
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(
                "SELECT database, plugin, active_pid, restart_lsn >= %s::pg_lsn "
                "FROM pg_replication_slots WHERE slot_name = %s",
                (rec.get("restart_lsn") or "0/0", name))
            row = cur.fetchone()
            with _lock:
                CREATED.pop(name, None)
            if row is None:
                continue
            database, plugin, active_pid, lsn_ok = row
            if (database, plugin) != (rec.get("database"), rec.get("plugin")) or not lsn_ok:
                logger.warning("slot guard: %s is not the slot this session created; kept", name)
                continue
            if active_pid:
                cur.execute("SELECT client_port FROM pg_stat_activity WHERE pid = %s",
                            (active_pid,))
                port = (cur.fetchone() or [None])[0]
                if port is None or int(port) not in _own_client_ports():
                    logger.warning("slot guard: %s is held by another process (pid %s); kept",
                                   name, active_pid)
                    continue
                cur.execute("SELECT pg_terminate_backend(%s)", (active_pid,))
                cur.execute("SELECT pg_sleep(0.2)")
            cur.execute("SELECT pg_drop_replication_slot(%s)", (name,))
            dropped.append(name)
        except Exception as exc:  # noqa: BLE001 - one slot must not stop the rest
            logger.warning("slot guard: could not drop %s: %s", name, exc)
        finally:
            conn.close()
    if dropped:
        logger.warning("slot guard dropped leaked CDC slots: %s", ", ".join(dropped))
    return dropped


# -- pytest hooks (registered by tests/conftest.py, or ``-p tests.pg_slot_guard``) --


def pytest_configure(config):
    install()


def pytest_sessionfinish(session, exitstatus):
    drop_created_slots()
