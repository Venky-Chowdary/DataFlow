"""PostgreSQL-style interval literals folded to one canonical fingerprint text.

Lives in its own module (Phase F8 size freeze) and is re-exported by
``services.reconciliation`` so every existing import keeps working.
"""

from __future__ import annotations

import re

_PG_INTERVAL_PART_RE = re.compile(
    r"([+-]?\d+(?:\.\d+)?)\s*(years?|mons?|months?|weeks?|days?|hours?|mins?|minutes?|secs?|seconds?)",
    re.I,
)
_PG_INTERVAL_HMS_RE = re.compile(
    r"([+-])?(\d+):(\d{1,2}):(\d{1,2}(?:\.\d+)?)"
)


def _interval_canonical_text(months: int, seconds: float) -> str:
    """Canonical interval fingerprint: months distinct, days folded into seconds.

    Matches PG comparison semantics: '1 day 02:00:00' == '26:00:00' (days are
    24h) but '1 mon' != '30 days' (months are their own field).
    """
    if abs(seconds - round(seconds, 6)) < 1e-9:
        seconds = round(seconds, 6)
    return f"{months}M+{seconds}S"


def _canonical_interval_text(value: str) -> str | None:
    """Parse a PG-style interval literal to (months, seconds) canonical text.

    Accepts verbose spans ('1 year 2 mons 3 days 04:05:06', '1 day 02:00:00'),
    bare 'HH:MM:SS[.ffffff]', and python timedelta text ('1 day, 2:00:00').
    Returns None when the value is not interval-shaped.
    """
    text = value.strip()
    if not text:
        return None
    months = 0.0
    seconds = 0.0
    matched_any = False
    for m in _PG_INTERVAL_PART_RE.finditer(text):
        n = float(m.group(1))
        unit = m.group(2).lower()
        if unit.startswith("year"):
            months += n * 12
        elif unit.startswith("mon") or unit.startswith("month"):
            months += n
        elif unit.startswith("week"):
            seconds += n * 7 * 86400
        elif unit.startswith("day"):
            seconds += n * 86400
        elif unit.startswith("hour"):
            seconds += n * 3600
        elif unit.startswith("min"):
            seconds += n * 60
        elif unit.startswith("sec"):
            seconds += n
        matched_any = True
    rest = _PG_INTERVAL_PART_RE.sub("", text).replace(",", " ").strip()
    hms = _PG_INTERVAL_HMS_RE.search(rest)
    if hms:
        sign = -1.0 if hms.group(1) == "-" else 1.0
        seconds += sign * (
            int(hms.group(2)) * 3600
            + int(hms.group(3)) * 60
            + float(hms.group(4))
        )
        matched_any = True
        rest = _PG_INTERVAL_HMS_RE.sub("", rest).strip()
    if not matched_any:
        return None
    # Leftover 'ago' / stray words => not the forms we understand; stay raw.
    leftover = re.sub(r"[\s.:-]+", "", rest)
    if leftover and leftover not in {"ago"}:
        return None
    return _interval_canonical_text(int(months), seconds)
