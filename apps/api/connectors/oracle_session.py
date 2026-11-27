"""Oracle writer sessions pinned to UTC, and the read-back that proves the pin.

python-oracledb sends a ``datetime``'s clock digits and never its offset
(``encode_date`` / ``encode_timestamp`` ignore ``tzinfo``; a
``TIMESTAMP WITH TIME ZONE`` bind carries a fixed ``+00:00``). Oracle reads
those digits in the session time zone, which the thin driver sets from the
client process's local offset (or ``ORA_SDTZ``). An instant written into a
``TIMESTAMP WITH [LOCAL] TIME ZONE`` column therefore lands as the intended
instant only when the digits are UTC *and* the session zone is UTC (accuracy
review finding 8).

Every Oracle connection DataFlow opens runs ``ALTER SESSION SET TIME_ZONE =
'+00:00'`` at connect and then reads ``SESSIONTIMEZONE`` back. Only a read-back
of UTC counts as pinned; a refused statement or any other reading leaves the
connection unpinned, and the grading keeps requiring a Risk Contract
(:mod:`services.oracle_session_scope`).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

PIN_STATEMENT = "ALTER SESSION SET TIME_ZONE = '+00:00'"
READ_BACK = "SELECT SESSIONTIMEZONE FROM DUAL"
#: ``connection_record.info`` key: ``(pinned, reason)`` from the connect-time pin.
INFO_KEY = "df_oracle_utc_session"

_UTC_READINGS = frozenset({"+00:00", "-00:00", "00:00", "UTC", "GMT", "Z", "ETC/UTC", "ETC/GMT", "UCT"})


def session_zone_is_utc(reading: Any) -> bool:
    """True for a ``SESSIONTIMEZONE`` reading that names UTC itself.

    A region that is UTC only part of the year (``Europe/London``) is not UTC.
    """
    text = str(reading or "").strip().upper()
    return text in _UTC_READINGS


def read_session_zone(dbapi_conn: Any) -> tuple[bool, str]:
    """Read ``SESSIONTIMEZONE`` on a DB-API connection: ``(is_utc, reason)``."""
    cur = dbapi_conn.cursor()
    try:
        cur.execute(READ_BACK)
        row = cur.fetchone()
    except Exception as exc:  # noqa: BLE001 - an unreadable zone is an unconfirmed pin
        return False, f"SESSIONTIMEZONE could not be read back ({type(exc).__name__})"
    finally:
        try:
            cur.close()
        except Exception:  # noqa: BLE001
            pass
    reading = str(row[0] if row else "").strip()
    if session_zone_is_utc(reading):
        return True, f"session time zone read back as {reading}"
    return False, f"session time zone read back as {reading or 'nothing'}, not UTC"


def pin_utc_session(dbapi_conn: Any) -> tuple[bool, str]:
    """``ALTER SESSION`` to UTC, then read it back: ``(pinned, reason)``. Never raises."""
    cur = dbapi_conn.cursor()
    try:
        cur.execute(PIN_STATEMENT)
    except Exception as exc:  # noqa: BLE001 - a refused pin is reported, not raised
        return False, f"ALTER SESSION SET TIME_ZONE was refused ({type(exc).__name__})"
    finally:
        try:
            cur.close()
        except Exception:  # noqa: BLE001
            pass
    return read_session_zone(dbapi_conn)


def confirm_utc_session(connection: Any) -> tuple[bool, str]:
    """Re-read the zone on a live SQLAlchemy ``Connection`` (or DB-API connection)."""
    raw = getattr(connection, "connection", None)
    dbapi = getattr(raw, "dbapi_connection", None) or getattr(raw, "driver_connection", None) or raw
    return read_session_zone(dbapi if dbapi is not None else connection)


def install_utc_session_pin(engine: Any) -> None:
    """Pin every pooled connection of ``engine`` at connect; refuse unpinned checkouts.

    The connect-time result is kept on the pool record. A checkout of an
    unpinned connection raises only while the run was graded on a confirmed pin
    (no Risk Contract): writing then would move the instant the grade promised
    to keep. A run that carries the Risk Contract writes as accepted.
    """
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def _oracle_utc_session(dbapi_conn, connection_record):  # noqa: ANN001
        pinned, reason = pin_utc_session(dbapi_conn)
        connection_record.info[INFO_KEY] = (pinned, reason)
        if not pinned:
            logger.warning("Oracle session time zone pin not confirmed: %s", reason)

    @event.listens_for(engine, "checkout")
    def _oracle_utc_checkout(_dbapi_conn, connection_record, _proxy):  # noqa: ANN001
        from services.oracle_session_scope import oracle_utc_pin

        pinned, reason = connection_record.info.get(INFO_KEY, (False, "the connection was never pinned"))
        if not pinned and oracle_utc_pin()[0]:
            raise RuntimeError(
                "Oracle session time zone pin lost on a pooled connection "
                f"({reason}); Validate graded this run's instants on a UTC "
                "session, so nothing is written. Re-run Validate."
            )


def oracle_instant_bind(value: datetime) -> datetime:
    """The UTC clock digits python-oracledb should send for an instant.

    Aware values convert to UTC and drop the (ignored) ``tzinfo``. A naive
    value has no instant to keep: refused rather than read as UTC.
    """
    if value.tzinfo is None:
        raise ValueError(
            "Oracle zone column refused a naive datetime — provide an offset/Z "
            "(refuse silent UTC invent)"
        )
    return value.astimezone(timezone.utc).replace(tzinfo=None)


__all__ = [
    "INFO_KEY",
    "PIN_STATEMENT",
    "READ_BACK",
    "confirm_utc_session",
    "install_utc_session_pin",
    "oracle_instant_bind",
    "pin_utc_session",
    "read_session_zone",
    "session_zone_is_utc",
]
