"""Every SQL statement sent for a tool call carries the call's remaining budget.

``run_with_deadline`` gives up on a tool call at its deadline, but the
statement it was waiting on kept running on the customer's server: PostgreSQL
reader sessions carry ``statement_timeout = 0``, pooled SQLAlchemy sessions
carry nothing, and the query route allowed 120s. The abandoned worker held its
connector slot until the server finished, which on a big table, a view over a
slow function or a lock queue is "never" (D181: abandoned calls never freed
their slots).

``install()`` adds one pool-level SQLAlchemy listener (all engines, including
pools created later): a connection checked out *inside a tool call* gets the
dialect's statement timeout set to the call's remaining budget, and gets it
reset when it is checked back in, so a pooled session used later by a
transfer job carries nothing of it. Raw driver connections are covered where
they are opened (``connectors.write_resilience.apply_postgres_session_guards``,
``connectors.postgresql_conn``).
"""

from __future__ import annotations

import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)

_installed = False
_lock = threading.Lock()
_FLAG = "df_statement_budget"


def _family(dbapi_connection: Any) -> str:
    module = type(dbapi_connection).__module__ or ""
    if module.startswith(("psycopg2", "psycopg")):
        return "postgresql"
    if module.startswith(("pymysql", "MySQLdb", "mysql")):
        return "mysql"
    if module.startswith("pyodbc"):
        return "pyodbc"
    return ""


def _execute(dbapi_connection: Any, statement: str, *, outside_transaction: bool = False) -> None:
    """Run one session statement; PostgreSQL runs it outside any transaction so
    the consumer's first statement (``SET TRANSACTION READ ONLY``) still opens it."""
    toggled = False
    if outside_transaction:
        # A pre-ping (SELECT 1) may have opened a transaction; nothing of the
        # consumer's is in it yet at checkout/checkin.
        status = getattr(dbapi_connection, "get_transaction_status", None)
        if callable(status) and status() in (2, 3):  # INTRANS / INERROR
            dbapi_connection.rollback()
    if outside_transaction and not getattr(dbapi_connection, "autocommit", True):
        dbapi_connection.autocommit = True
        toggled = True
    try:
        cur = dbapi_connection.cursor()
        try:
            cur.execute(statement)
        finally:
            cur.close()
    finally:
        if toggled:
            dbapi_connection.autocommit = False


def _on_checkout(dbapi_connection: Any, record: Any, _proxy: Any) -> None:
    from src.ai.copilot.tool_deadline import (
        register_connection,
        statement_timeout_seconds,
    )

    seconds = statement_timeout_seconds()
    if seconds is None:
        return
    # The call's deadline can end this connection's blocked I/O if the call
    # is abandoned (D281); checkin takes it back out below.
    register_connection(dbapi_connection)
    family = _family(dbapi_connection)
    millis = int(seconds * 1000)
    try:
        if family == "postgresql":
            _execute(
                dbapi_connection,
                f"SET statement_timeout = {millis}",
                outside_transaction=True,
            )
        elif family == "mysql":
            _execute(dbapi_connection, f"SET SESSION MAX_EXECUTION_TIME = {millis}")
        elif family == "pyodbc":
            # pyodbc cancels the statement client-side after this many seconds.
            dbapi_connection.timeout = max(1, int(seconds))
        else:
            return
        record.info[_FLAG] = family
    except Exception as exc:  # noqa: BLE001 - a bound is best effort
        logger.debug("statement budget not applied (%s): %s", family, exc)


def _on_connect(dbapi_connection: Any, _record: Any) -> None:
    """A new pooled connection is interruptible before its first query.

    The engine's dialect initialize (server version, settings) runs from the
    pool's ``connect`` event, before ``checkout``: a source that went silent
    right after the handshake held that worker in ``recv`` with nothing able
    to end it (D281). Class-level pool listeners run before the engine's own,
    so registering here covers those queries. A no-op outside a tool call.
    """
    from src.ai.copilot.tool_deadline import register_connection

    register_connection(dbapi_connection)


def _on_checkin(dbapi_connection: Any, record: Any) -> None:
    # The connection stays registered to the call that used it last: SQLAlchemy
    # runs ``pool_pre_ping`` *before* the checkout event, so a ping on an idle
    # pooled socket whose tunnel went silent would otherwise block with
    # nothing registered (the original D281 shape). The next checkout moves
    # ownership to its call; close/invalidate unregister it.
    family = record.info.pop(_FLAG, None) if record is not None else None
    if not family or dbapi_connection is None:
        return
    try:
        if family == "postgresql":
            if getattr(dbapi_connection, "closed", 0):
                return
            _execute(dbapi_connection, "RESET statement_timeout", outside_transaction=True)
        elif family == "mysql":
            _execute(dbapi_connection, "SET SESSION MAX_EXECUTION_TIME = 0")
        elif family == "pyodbc":
            dbapi_connection.timeout = 0
    except Exception as exc:  # noqa: BLE001 - the pool invalidates a broken one
        logger.debug("statement budget not reset (%s): %s", family, exc)


def _on_gone(dbapi_connection: Any, *_args: Any) -> None:
    """A closed or invalidated connection leaves the interrupt registry."""
    from src.ai.copilot.tool_deadline import unregister_connection

    unregister_connection(dbapi_connection)


def install() -> None:
    """Register the pool listeners once per process."""
    global _installed
    if _installed:
        return
    with _lock:
        if _installed:
            return
        try:
            from sqlalchemy import event
            from sqlalchemy.pool import Pool
        except ImportError:  # pragma: no cover - SQLAlchemy is a dependency
            return
        event.listen(Pool, "first_connect", _on_connect)
        event.listen(Pool, "connect", _on_connect)
        event.listen(Pool, "checkout", _on_checkout)
        event.listen(Pool, "checkin", _on_checkin)
        event.listen(Pool, "close", _on_gone)
        event.listen(Pool, "close_detached", _on_gone)
        event.listen(Pool, "invalidate", _on_gone)
        _installed = True
