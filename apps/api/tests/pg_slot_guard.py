"""Test-session guard: no logical replication slot outlives the pytest run that made it.

An inactive logical slot keeps every WAL segment from its restart LSN on. A
CDC test whose ``finally`` never ran (the run was killed with SIGTERM, or the
slot was still held by an open replication connection at teardown, so the
test's ``WHERE NOT active`` drop skipped it) left ``df_dataflow_cdcord_*``
holding 1.76 GB of WAL on the shared server until the disk filled.

The guard records every slot this process creates through the engine's single
creation site (``postgresql_change_stream._create_logical_slot``), turns
SIGTERM into ``KeyboardInterrupt`` so pytest still unwinds (``finally`` blocks
and ``pytest_sessionfinish`` run), and at session end drops each recorded slot
that still exists, ending the backend that holds it first. It only touches
slots this process created: other sessions' slots on the shared server are
never dropped. A SIGKILL cannot be intercepted; that is the one case left to
the operator.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
from typing import Any

logger = logging.getLogger(__name__)

#: ``(slot name, connection parameters)`` for every slot this process created.
CREATED: dict[str, dict[str, Any]] = {}
_lock = threading.Lock()


def _record(cur: Any, slot_name: str) -> None:
    try:
        params = dict(cur.connection.get_dsn_parameters())
    except Exception:  # noqa: BLE001 - fall back to the test server's env
        params = {}
    with _lock:
        CREATED[str(slot_name)] = params


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
        _record(cur, slot_name)
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


def drop_created_slots() -> list[str]:
    """Drop every slot this process created that still exists; return their names."""
    with _lock:
        pending = dict(CREATED)
    dropped: list[str] = []
    for name, params in pending.items():
        try:
            conn = _connect(params)
        except Exception as exc:  # noqa: BLE001 - report, the server may be down
            logger.warning("slot guard: cannot connect to drop %s: %s", name, exc)
            continue
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s",
                        (name,))
            row = cur.fetchone()
            if row is None:
                with _lock:
                    CREATED.pop(name, None)
                continue
            if row[0]:
                cur.execute("SELECT pg_terminate_backend(%s)", (row[0],))
                cur.execute("SELECT pg_sleep(0.2)")
            cur.execute("SELECT pg_drop_replication_slot(%s)", (name,))
            dropped.append(name)
            with _lock:
                CREATED.pop(name, None)
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
