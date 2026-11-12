"""PR 411 review 1, M4: the fingerprint memo never aliases two offsets.

``12:00+00:00`` and ``13:00+01:00`` are the same instant, so they compare and
hash equal, yet a text column stores each as its own spelling. Keyed on the
value alone, the second one met took the first one's fingerprint and the
digest of a text-declared column depended on scan order.
"""

from __future__ import annotations

import datetime as dt

from services.fingerprint_rules import memo_key
from src.transfer.stream import row_fingerprints

_UTC = dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone.utc)
_PLUS1 = dt.datetime(2024, 1, 1, 13, tzinfo=dt.timezone(dt.timedelta(hours=1)))


def _fingerprints(rows):
    return sorted(
        fp
        for _, fp in row_fingerprints(
            rows,
            ["id", "c"],
            dest_types={"id": "INTEGER", "c": "TEXT"},
            dest_db_type="postgresql",
        )
    )


def test_text_column_digest_does_not_depend_on_scan_order():
    forward = _fingerprints([{"id": 1, "c": _UTC}, {"id": 2, "c": _PLUS1}])
    backward = _fingerprints([{"id": 2, "c": _PLUS1}, {"id": 1, "c": _UTC}])
    assert forward == backward


def test_memo_key_carries_the_offset_of_aware_values():
    assert memo_key(_UTC, "TEXT") != memo_key(_PLUS1, "TEXT")
    t_utc = dt.time(12, tzinfo=dt.timezone.utc)
    t_plus1 = dt.time(12, tzinfo=dt.timezone(dt.timedelta(hours=1)))
    assert memo_key(t_utc, "TEXT") != memo_key(t_plus1, "TEXT")
    naive = dt.datetime(2024, 1, 1, 12)  # noqa: DTZ001
    assert memo_key(naive, "TEXT") == memo_key(dt.datetime(2024, 1, 1, 12), "TEXT")  # noqa: DTZ001
