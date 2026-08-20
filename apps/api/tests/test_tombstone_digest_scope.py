"""Gate-8 source digest scope for keyed upserts that carry tombstones.

A keyed upsert applies a tombstone as a hard DELETE, so the destination
deliberately does not hold that row. Hashing the tombstoned row on the source
side compared *N* source rows against *N - deletes* destination rows and failed
Gate-8 on a correct run (live PostgreSQL -> MariaDB upsert, two hex strings and
a key-aligned sample that found no differing cell). The write path already
strips tombstones through ``partition_keyed_records``; the source digest bases
must use the same owner so both sides describe the live population.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.row_conservation import (  # noqa: E402
    live_records_for_digest,
    live_rows_for_digest,
)

_MAPPINGS = [
    {"source": "id", "target": "id"},
    {"source": "label", "target": "label"},
    {"source": "is_deleted", "target": "is_deleted"},
]


def test_tombstoned_row_leaves_the_source_digest_scope() -> None:
    """The deleted key is not in the destination, so it is not in the digest."""
    records = [
        {"id": 1, "label": "A", "is_deleted": False},
        {"id": 2, "label": "b", "is_deleted": True},
        {"id": 3, "label": "c", "is_deleted": False},
    ]
    live, excluded = live_records_for_digest(
        records, key_columns=["id"], mappings=_MAPPINGS
    )
    assert excluded == 1
    assert [r["id"] for r in live] == [1, 3]


def test_no_tombstone_returns_every_row_unchanged() -> None:
    """A digest scope narrowed without a delete would hide missing rows."""
    records = [
        {"id": 1, "label": "a", "is_deleted": False},
        {"id": 2, "label": "b", "is_deleted": False},
    ]
    live, excluded = live_records_for_digest(
        records, key_columns=["id"], mappings=_MAPPINGS
    )
    assert excluded == 0
    assert [r["id"] for r in live] == [1, 2]


def test_without_key_columns_nothing_can_be_deleted() -> None:
    """No key means no DELETE can be addressed — scope must stay whole."""
    records = [
        {"id": 1, "label": "a", "is_deleted": False},
        {"id": 2, "label": "b", "is_deleted": True},
    ]
    live, excluded = live_records_for_digest(records, key_columns=[], mappings=_MAPPINGS)
    assert excluded == 0
    assert len(live) == 2


def test_positional_rows_keep_header_order() -> None:
    """Batch readers hand positional rows; the digest scope must round-trip."""
    headers = ["id", "label", "is_deleted"]
    rows = [
        [1, "A", False],
        [2, "b", True],
        [3, "c", False],
    ]
    live, excluded = live_rows_for_digest(
        headers, rows, key_columns=["id"], mappings=_MAPPINGS
    )
    assert excluded == 1
    assert live == [[1, "A", False], [3, "c", False]]
    # The caller's batch is untouched: the keyset cursor advances on the last
    # row actually read, not on the digest scope.
    assert len(rows) == 3


def test_recreated_key_stays_live() -> None:
    """DELETE then re-INSERT of one key inside a batch is a live row."""
    records = [
        {"id": 1, "label": "old", "is_deleted": True},
        {"id": 1, "label": "new", "is_deleted": False},
        {"id": 2, "label": "b", "is_deleted": True},
    ]
    live, excluded = live_records_for_digest(
        records, key_columns=["id"], mappings=_MAPPINGS
    )
    assert excluded == 2
    assert [(r["id"], r["label"]) for r in live] == [(1, "new")]
