"""BSON-date spellings of a text key, for Mongo ``$in`` reads.

Split out of :mod:`services.target_sample` (module size budget), which
re-exports :func:`mongo_instant_key_variants` through ``mongo_query_key_variants``.
"""

from __future__ import annotations

import re
from typing import Any

_ISO_INSTANT_HEAD = re.compile(r"^\d{4}-\d{2}-\d{2}(?:$|[T ]\d{2}:\d{2})")


def mongo_instant_key_variants(key: Any) -> set[Any]:
    """BSON-date spellings of an ISO date/timestamp key.

    The Mongo writer lands a SQL ``TIMESTAMP``/``DATE`` as a BSON date (UTC,
    millisecond precision), but a watermark travels as text. ``$in`` is type
    sensitive, so ``updated_at $in ['2024-01-01T07:00:00']`` never matched the
    stored date: the incremental tie-set lookup came back empty, failed open,
    and every run re-appended the boundary row (QA r12 Mongo D7/D4).

    Returns the UTC-naive instant (pymongo's own read shape) and, when the text
    carries sub-millisecond digits, the BSON-truncated instant too. Non-ISO
    text adds nothing.
    """
    from datetime import date, datetime, timezone

    if isinstance(key, datetime):
        parsed = key
    elif isinstance(key, date):
        # Naive UTC on purpose: pymongo decodes a BSON date to naive UTC.
        parsed = datetime(key.year, key.month, key.day)  # noqa: DTZ001
    elif isinstance(key, str) and _ISO_INSTANT_HEAD.match(key.strip()):
        try:
            parsed = datetime.fromisoformat(key.strip())
        except ValueError:
            return set()
    else:
        return set()
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    out: set[Any] = {parsed}
    if parsed.microsecond % 1000:
        out.add(parsed.replace(microsecond=parsed.microsecond - parsed.microsecond % 1000))
    return out
