"""End a python-oracledb connect at the connect timeout, handshake included (D354).

python-oracledb (thin) applies ``tcp_connect_timeout`` to the TCP connect
only, then clears the socket timeout before the TNS handshake. A peer that
accepts the connection and never answers (a tunnel or proxy with nothing
behind it) held the connect until the far end closed it: a connection Test
gave up only at the 120 s tool deadline, while PostgreSQL (libpq
``connect_timeout``) and SQL Server (ODBC login timeout) gave up at the
15 s connect timeout. Neither ``tcp_connect_timeout`` nor a descriptor's
``CONNECT_TIMEOUT`` bounds that read.

:func:`connect_within_timeout` runs the driver's connect with the same budget
(``tcp_connect_timeout``, which ``connectors.generic_sql`` sets from the
connector's ``connect_timeout``): when it passes, the sockets the connect
opened are shut, the driver's blocked read returns, and the caller gets a
driver ``OperationalError`` that says the server accepted the TCP connection
but did not answer the Oracle handshake.
"""

from __future__ import annotations

import contextlib
import re
import socket
import threading
from typing import Any

_DSN_ADDRESS = re.compile(r"\(HOST=([^)]+)\)\s*\(PORT=(\d+)\)", re.I)


def _budget_seconds(cparams: dict[str, Any]) -> float:
    try:
        seconds = float(cparams.get("tcp_connect_timeout") or 0)
    except (TypeError, ValueError):
        seconds = 0.0
    if seconds > 0:
        return seconds
    from connectors.generic_sql import DEFAULT_CONNECT_TIMEOUT_SECONDS

    return float(DEFAULT_CONNECT_TIMEOUT_SECONDS)


def _timeout_error(dialect: Any, cparams: dict[str, Any], seconds: float) -> Exception:
    m = _DSN_ADDRESS.search(str(cparams.get("dsn") or ""))
    host = m.group(1) if m else str(cparams.get("host") or "")
    port = m.group(2) if m else str(cparams.get("port") or "")
    where = f" ({host}:{port})" if host and port else ""
    message = (
        f"Handshake timed out{where}: the server accepted the TCP connection but did not "
        f"answer the Oracle handshake within {max(1, int(seconds))} seconds."
    )
    error = getattr(getattr(dialect, "loaded_dbapi", None), "OperationalError", None)
    try:
        return error(message) if error is not None else TimeoutError(message)
    except Exception:  # noqa: BLE001 - a driver class that takes no message
        return TimeoutError(message)


def connect_within_timeout(dialect: Any, cargs: Any, cparams: dict[str, Any]) -> Any:
    """``dialect.connect(*cargs, **cparams)``, ended at the connect timeout."""
    from services.egress_guard import observe_dials

    seconds = _budget_seconds(cparams)
    lock = threading.Lock()
    sockets: list[Any] = []
    state = {"done": False, "expired": False}

    def _track(sock: Any) -> None:
        with lock:
            sockets.append(sock)

    def _expire() -> None:
        with lock:
            # Nothing observed (thick mode dials in C): nothing to end, and a
            # slow but successful connect must not be thrown away.
            if state["done"] or not sockets:
                return
            state["expired"] = True
            for sock in sockets:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    timer = threading.Timer(seconds, _expire)
    timer.daemon = True
    with observe_dials(_track):
        timer.start()
        try:
            conn = dialect.connect(*cargs, **cparams)
        except Exception as exc:
            with lock:
                state["done"] = True
                expired = state["expired"]
            timer.cancel()
            if expired:
                raise _timeout_error(dialect, cparams, seconds) from exc
            raise
    with lock:
        state["done"] = True
        expired = state["expired"]
    timer.cancel()
    if expired:
        # The budget ran out as the handshake finished: its socket is shut.
        with contextlib.suppress(Exception):  # already broken
            conn.close()
        raise _timeout_error(dialect, cparams, seconds)
    return conn
