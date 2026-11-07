"""MySQL ``TIME`` as MySQL itself spells it.

MySQL ``TIME`` is a signed interval (``-838:59:59`` .. ``838:59:59``), not a
wall clock, and pymysql hands it back as ``timedelta``. A text carrier (SQLite
has no temporal storage class) holds exactly what DataFlow writes, so the only
faithful text is MySQL's own: ``CAST(t AS CHAR)`` renders ``-01:02:03.000000``
for ``TIME(6)`` and ``838:59:59`` for ``TIME``.

``timedelta`` arithmetic is exact in microseconds; ``total_seconds()`` (a
float) is not, so every split here is integral.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

#: ``TIME`` / ``TIME(n)`` as MySQL reports it (``COLUMN_TYPE`` or a DDL).
_MYSQL_TIME_TYPE_RE = re.compile(r"^\s*TIME\s*(?:\(\s*(\d)\s*\))?\s*$", re.IGNORECASE)

#: MySQL interval text: optional sign, 2-3 hour digits, minutes, seconds,
#: optional fraction (MySQL keeps at most six digits).
MYSQL_TIME_TEXT_RE = re.compile(r"^(-?)(\d{2,3}):([0-5]\d):([0-5]\d)(?:\.(\d{1,6}))?$")


def mysql_time_fsp(declared: Any) -> int | None:
    """Fractional digits of a MySQL ``TIME`` declaration, else ``None``.

    ``TIME`` without a precision is MySQL's default of 0.
    """
    match = _MYSQL_TIME_TYPE_RE.match(str(declared or ""))
    if match is None:
        return None
    return int(match.group(1)) if match.group(1) else 0


def mysql_time_text(value: timedelta, fsp: int | None) -> str:
    """``[-]HH:MM:SS[.f…]`` exactly as MySQL renders a ``TIME(fsp)`` value.

    ``fsp`` ``None`` (unknown declaration) keeps the six digits a non-zero
    fraction needs and omits a zero fraction, the shape the generic
    serializer emits.
    """
    total_us = value // timedelta(microseconds=1)
    sign = "-" if total_us < 0 else ""
    total_us = abs(total_us)
    secs, micros = divmod(total_us, 1_000_000)
    hours, rem = divmod(secs, 3600)
    minutes, seconds = divmod(rem, 60)
    clock = f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    if fsp is None:
        return f"{clock}.{micros:06d}" if micros else clock
    digits = max(0, min(int(fsp), 6))
    if not digits:
        return clock
    return f"{clock}.{f'{micros:06d}'[:digits]}"


def is_mysql_interval_text(text: str) -> bool:
    """True for MySQL ``TIME`` text outside the 0-24h wall clock.

    Those values (negative, or 24 hours and over) are valid MySQL ``TIME``
    values that no wall-clock parser accepts.
    """
    match = MYSQL_TIME_TEXT_RE.match(text or "")
    if match is None:
        return False
    return match.group(1) == "-" or int(match.group(2)) >= 24
