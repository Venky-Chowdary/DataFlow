"""One parser for an ISO-8601 instant written as text.

``datetime.fromisoformat`` does not accept the ``Z`` suffix before Python
3.11's behaviour settled across the code base, and several modules grew their
own ``replace("Z", "+00:00")`` copy. This is the shared one; callers decide
what a zone-less value means (``assume_utc``), because that differs: a stored
connector timestamp is UTC by convention, while a Gate-8 proof must tell a
zoned instant from a naive wall clock.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def parse_iso_instant(value: Any, *, assume_utc: bool = False) -> datetime | None:
    """``datetime`` for an ISO text (``Z`` accepted) or datetime; ``None`` otherwise.

    With ``assume_utc`` a zone-less result is stamped UTC; without it the
    result keeps whatever zone the text carried (none included).
    """
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if assume_utc and parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
