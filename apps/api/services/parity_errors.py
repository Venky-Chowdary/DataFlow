"""Parity Studio: what an error may say once it leaves the server.

A fix's result is shown on the Trust Report, sealed in the signed pack,
exported to Markdown and written to the audit chain. Database and child-job
errors quote row values (``Incorrect integer value: 'alice@example.com'``,
``Key (email)=(…)``, ``Failing row contains (…)``), and no pattern list can
find them all. So this is an allowlist: an error becomes its exception class,
the engine's error code (SQLSTATE, MySQL errno, ORA-/Msg number) and a fixed
sentence for that code. The server-side record is the platform's canonical error
event (:func:`services.safe_errors.public_error`, with its correlation id); the
full text goes to the server log only, under that correlation id. Its public
``detail`` is not used: with ``DATAFLOW_EXPOSE_ERROR_DETAIL`` it would carry the
raw text, and a fix result is sealed in signed packs.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

#: Fixed sentences per engine error code. Never a value from the message.
_MESSAGES: dict[str, str] = {
    # PostgreSQL SQLSTATE
    "23505": "a unique key already holds this value (duplicate key)",
    "23502": "a NOT NULL column received NULL",
    "23503": "a foreign key has no matching row",
    "23514": "a check constraint refused a row",
    "22001": "a value is too long for its column",
    "22003": "a number is out of range for its column",
    "22007": "a date/time value has an invalid format",
    "22008": "a date/time value is out of range",
    "22P02": "a value is not valid input for its column type",
    "40P01": "a deadlock was detected",
    "55P03": "a lock could not be acquired",
    "57014": "the statement was cancelled (timeout)",
    "42501": "the destination user lacks a permission",
    # MySQL / MariaDB errno
    "MySQL 1062": "a unique key already holds this value (duplicate key)",
    "MySQL 1048": "a NOT NULL column received NULL",
    "MySQL 1452": "a foreign key has no matching row",
    "MySQL 1366": "a value is not valid for its column type",
    "MySQL 1292": "a date/time value is not valid",
    "MySQL 1265": "a value was truncated for its column",
    "MySQL 1406": "a value is too long for its column",
    "MySQL 1264": "a number is out of range for its column",
    "MySQL 1205": "a lock wait timed out",
    "MySQL 1213": "a deadlock was detected",
    "MySQL 1142": "the destination user lacks a permission",
    # SQL Server message numbers
    "Msg 2627": "a unique key already holds this value (duplicate key)",
    "Msg 2601": "a unique index already holds this value (duplicate key)",
    "Msg 515": "a NOT NULL column received NULL",
    "Msg 547": "a constraint refused a row",
    "Msg 245": "a value could not be converted to its column type",
    "Msg 8152": "a value is too long for its column",
    "Msg 2628": "a value is too long for its column",
    "Msg 1205": "a deadlock was detected",
}

#: Message shapes that identify a code when the text carries none.
_SHAPES: tuple[tuple[re.Pattern[str], str], ...] = tuple((re.compile(p, re.I), code) for p, code in (
    (r"duplicate key value violates unique constraint", "23505"),
    (r"violates not-null constraint", "23502"),
    (r"violates foreign key constraint", "23503"),
    (r"violates check constraint", "23514"),
    (r"value too long for type", "22001"),
    (r"invalid input syntax for type", "22P02"),
    (r"out of range", "22003"),
    (r"canceling statement due to statement timeout", "57014"),
    (r"deadlock detected", "40P01"),
    (r"Incorrect (integer|decimal|double|string) value", "MySQL 1366"),
    (r"Incorrect (datetime|date|time) value", "MySQL 1292"),
    (r"Data truncated for column", "MySQL 1265"),
    (r"Data too long for column", "MySQL 1406"),
    (r"Duplicate entry", "MySQL 1062"),
    (r"Violation of (PRIMARY|UNIQUE) KEY constraint", "Msg 2627"),
    (r"Conversion failed when converting", "Msg 245"),
    (r"would be truncated", "Msg 8152"),
))

_CLASS = re.compile(r"\b((?:[A-Za-z_][A-Za-z0-9_]*\.)*([A-Z][A-Za-z0-9]*(?:Error|Exception|Violation|Truncation|"
                    r"Representation|Overflow|Timeout|Refused|Unavailable)))\b")
_SQLSTATE = re.compile(r"\b(?:SQLSTATE|sqlstate|pgcode)[\s:=\[\"']*([0-9A-Z]{5})\b")
_MYSQL_ERRNO = re.compile(r"^\(?\s*(1\d{3}|2\d{3}|3\d{3})\s*,")
_ORACLE = re.compile(r"\b(ORA-\d{5})\b")
_MSSQL = re.compile(r"\b(?:Msg|SQL Server error|error)\s*\(?\s*(\d{3,5})\)?", re.I)


def error_code(text: str) -> str:
    """The engine's error code in ``text`` ("23505", "MySQL 1366", "ORA-01400", "Msg 2627"), or ""."""
    if found := _SQLSTATE.search(text):
        return found.group(1)
    if found := _MYSQL_ERRNO.search(text):
        return f"MySQL {found.group(1)}"
    if found := _ORACLE.search(text):
        return found.group(1)
    for pattern, code in _SHAPES:
        if pattern.search(text):
            return code
    if (found := _MSSQL.search(text)) and f"Msg {found.group(1)}" in _MESSAGES:
        return f"Msg {found.group(1)}"
    return ""


def safe_error(error: Any, *, what: str = "the fix", limit: int = 300, event: str = "parity.fix.failed") -> str:
    """``error`` (an exception or a child job's error text) as it may be shown, signed and audited.

    Only the exception class, the engine's error code and a fixed sentence for
    that code survive; the full text is logged on the server.
    """
    from services.safe_errors import public_error

    text = str(error or "")
    cls = type(error).__name__ if isinstance(error, BaseException) else ""
    if not cls and (found := _CLASS.search(text)):
        cls = found.group(2)
    code = error_code(text)
    _detail, cid = public_error(
        error if isinstance(error, BaseException) else RuntimeError(cls or "error"),
        event=event, public_message=what, logger=logger, error_code=code or None,
    )
    logger.warning("parity: %s failed (%s%s) [reference %s]: %s", what, cls or "error", f" {code}" if code else "",
                   cid, text[:2000])
    sentence = _MESSAGES.get(code) or (
        "the Oracle database refused the statement" if code.startswith("ORA-") else
        "see the server log for the details")
    label = " ".join(x for x in (cls, code) if x)
    return (f"{what} failed" + (f" ({label})" if label else "") + f": {sentence}.")[:limit]
