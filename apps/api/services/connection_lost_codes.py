"""D427 - a dropped database connection, recognised by the driver's own code.

Error taxonomy must be evidence-based: the driver code decides before any
text rule. A SQL Server CDC read that died with DB-Library 20047 ("DBPROCESS
is dead") was reported as "CDC cursor gap (retention / failover)" because its
wrapper text says "CDC/Change Tracking", and the same drop in the capture
probe read as "the server's change log is off". Both sent operators to
re-snapshot a pipeline that only needed a re-run.

``connection_lost_code`` names the code when an exception (or anything it
wraps) is a lost or refused connection, and returns ``""`` otherwise. It is
deliberately narrow: a permission or object error that a driver happens to
raise as ``OperationalError`` is not a dropped connection.
"""

from __future__ import annotations

import re
from typing import Any

#: FreeTDS / DB-Library: connection failed (20002), timed out (20003), read
#: (20004) or write (20006) failed, unable to connect (20009), unexpected EOF
#: (20017), DBPROCESS dead (20047).
DBLIB_CONNECTION_LOST = frozenset({20002, 20003, 20004, 20006, 20009, 20017, 20047})
#: Oracle: end-of-file on the channel, not connected, connection lost contact;
#: python-oracledb's closed connection. DPY-6005 (cannot connect) counts only
#: when no server code says why: it also wraps a wrong service name (ORA-12514).
_ORACLE_CODES = re.compile(r"\b(ORA-0311[34]|ORA-03135|DPY-4011|DPI-1080)\b", re.I)
_ORACLE_CONNECT = re.compile(r"\bDPY-6005\b", re.I)
_ORACLE_ANY = re.compile(r"\bORA-\d{5}\b", re.I)
#: Text a wrapper kept, read in this order; the first match decides: Datawrap's
#: own "SQL Server error N" (the driver's main code), pymssql's tuple text
#: "(N, b'...')" (its first number is the main code; a login failure carries a
#: trailing "DB-Lib error message 20002" that must not decide), or a message
#: that starts with "DB-Lib error message N".
_TEXT_CODES = (
    re.compile(r"SQL Server error (\d+)\b"),
    re.compile(r"\((\d{3,5}),\s*b?['\"]"),
    re.compile(r"^\W*DB-Lib error message (\d+)\b", re.I),
)
_DEPTH = 6


def _code_of(err: BaseException) -> str | None:
    """The verdict of one exception: a code, ``""`` (a driver error that is not a
    lost connection: final), or ``None`` (not a driver error: keep looking)."""
    module = (type(err).__module__ or "").lower()
    number = getattr(err, "error_number", None)  # SqlServerCdcReadError
    if isinstance(number, int) and not isinstance(number, bool):
        return f"SQL Server error {number}" if number in DBLIB_CONNECTION_LOST else ""
    args = getattr(err, "args", ()) or ()
    if module.startswith("pymssql") and args and isinstance(args[0], int):
        return f"SQL Server error {args[0]}" if args[0] in DBLIB_CONNECTION_LOST else ""
    if module.startswith("pyodbc") and args and isinstance(args[0], str) and len(args[0]) == 5:
        return f"SQLSTATE {args[0]}" if args[0].startswith("08") else ""
    pgcode = getattr(err, "pgcode", None) or (
        getattr(err, "sqlstate", None) if module.startswith("psycopg") else None
    )
    if isinstance(pgcode, str) and pgcode:
        return f"SQLSTATE {pgcode}" if pgcode.startswith("08") or pgcode in {"57P01", "57P02", "57P03"} else ""
    if module.startswith(("pymysql", "mysqldb", "mysql", "mariadb")) and args and isinstance(args[0], int):
        from connectors.write_resilience import MYSQL_CONNECTION_LOST_CODES

        return f"MySQL error {args[0]}" if args[0] in MYSQL_CONNECTION_LOST_CODES else ""
    return _oracle_code(str(err)) or None


def _oracle_code(text: str) -> str:
    found = _ORACLE_CODES.search(text)
    if found:
        return found.group(1).upper()
    if _ORACLE_CONNECT.search(text) and not _ORACLE_ANY.search(text):
        return "DPY-6005"
    return ""


def _text_code(text: str) -> str:
    for pattern in _TEXT_CODES:
        found = pattern.search(text)
        if found:
            number = int(found.group(1))
            return f"SQL Server error {number}" if number in DBLIB_CONNECTION_LOST else ""
    if "dbprocess is dead" in text.lower():
        return "SQL Server error 20047"
    return _oracle_code(text)


def connection_lost_code(error: Any) -> str:
    """The driver code of a lost connection inside ``error``, or ``""``."""
    if not isinstance(error, BaseException):
        return _text_code(str(error or ""))
    seen: set[int] = set()
    frontier: list[BaseException] = [error]
    for _ in range(_DEPTH):
        nxt: list[BaseException] = []
        for err in frontier:
            if id(err) in seen:
                continue
            seen.add(id(err))
            verdict = _code_of(err)
            if verdict is not None:
                return verdict
            # Only an explicit link (``raise ... from``, SQLAlchemy ``orig``)
            # says what an error wraps. The implicit ``__context__`` is whatever
            # was being handled when it was raised: a refusal raised while a
            # dropped connection was handled is a refusal, not that drop.
            for inner in (getattr(err, "orig", None), err.__cause__):
                if isinstance(inner, BaseException):
                    nxt.append(inner)
        if not nxt:
            break
        frontier = nxt
    # No driver exception in the chain: a wrapper that kept only the text.
    return _text_code(str(error))
