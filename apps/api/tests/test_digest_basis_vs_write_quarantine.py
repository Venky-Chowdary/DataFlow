"""The carrier's precision belongs to the digest, not to the write quarantine.

Two rules meet on the same fractional second and mean opposite things:

* Gate-8 must hash both sides at the precision the destination column physically
  keeps, or a load the destination stored correctly fails on two opaque hashes.
* The write-path quarantine matrix reads a *declared* precision as "refuse to
  truncate", so a column restated at the carrier's precision made every
  microsecond row a holdout.

Feeding one dict to both made the remap hold out all 20,000 rows of the live
Postgres→MySQL fixture, the re-read fingerprinted nothing, and the run then
compared the writer's own digest against the destination while still calling it
an independent source re-read. The basis for hashing is therefore kept apart
from the basis for accepting a row, and a re-read that fingerprints nothing
declines its digest instead of borrowing the writer's.
"""

from __future__ import annotations

from datetime import datetime

from connectors.writer_common import map_rows_for_fingerprint, row_fingerprints
from services.dest_physical_types import apply_physical_temporal_precision

_COLS = ["id", "event_ts"]
_MAPPINGS = [
    {"source": "id", "target": "id", "target_type": "BIGINT"},
    {"source": "event_ts", "target": "event_ts", "target_type": "DATETIME(6)"},
]
_DECLARED = {"id": "BIGINT", "event_ts": "DATETIME(6)"}
_ROWS = [["1", "2024-01-01 00:00:01.000001"], ["2", "2024-01-01 00:00:02.000002"]]


def _mapped(dest_types: dict[str, str]) -> list[tuple]:
    mapped, _rejected = map_rows_for_fingerprint(
        headers=_COLS,
        data_rows=_ROWS,
        mappings=_MAPPINGS,
        target_cols=_COLS,
        column_types=_DECLARED,
        dest_types=dest_types,
        dest_kind="mysql",
    )
    return mapped


def test_write_quarantine_keeps_rows_the_carrier_rounds() -> None:
    """A carrier that rounds is a narrowing to disclose, not rows to reject."""
    assert len(_mapped(_DECLARED)) == len(_ROWS)


def test_carrier_precision_would_hold_out_every_row_if_it_drove_the_remap() -> None:
    """Why the two bases must stay apart — this is the defect, pinned."""
    carrier = {"id": "BIGINT", "event_ts": "DATETIME(0)"}
    assert _mapped(carrier) == []


def test_digest_at_carrier_precision_matches_what_the_column_stores() -> None:
    src = _mapped(_DECLARED)
    stored = [(1, datetime(2024, 1, 1, 0, 0, 1)), (2, datetime(2024, 1, 1, 0, 0, 2))]
    carrier = {"id": "BIGINT", "event_ts": "DATETIME(0)"}

    assert row_fingerprints(
        src, _COLS, dest_db_type="mysql", dest_types=carrier
    ) == row_fingerprints(stored, _COLS, dest_db_type="mysql", dest_types=carrier)
    # Declared precision on both sides disagrees — that is the false mismatch.
    assert row_fingerprints(
        src, _COLS, dest_db_type="mysql", dest_types=_DECLARED
    ) != row_fingerprints(stored, _COLS, dest_db_type="mysql", dest_types=_DECLARED)


def test_declared_types_survive_an_unanswerable_catalog() -> None:
    """No physical answer means the declared contract stands, not a guess."""
    assert (
        apply_physical_temporal_precision(
            dict(_DECLARED), "mysql", {"host": ""}, table="nonexistent_table"
        )
        == _DECLARED
    )
