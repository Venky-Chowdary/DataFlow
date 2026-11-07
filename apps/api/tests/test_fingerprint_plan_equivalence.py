"""Hoisting column facts out of the row loop must not move a single digest.

Gate-8 compares a source digest against a destination digest, so any change to
how a cell is canonicalized is a change to whether correct transfers pass. These
tests pin the digest itself rather than the speed: the per-column resolution is
an optimization only if the bytes are identical to resolving it per cell.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from services.reconciliation import (
    _fingerprint_cell,
    _iter_fingerprints,
    checksum_rows,
)

COLUMNS = ["id", "name", "amount", "created_at", "flag"]
DEST_TYPES = {
    "id": "BIGINT",
    "name": "VARCHAR(50)",
    "amount": "NUMERIC(12,2)",
    "created_at": "TIMESTAMP",
    "flag": "BOOLEAN",
}
ROWS = [
    (1, "alpha", Decimal("150.25"), datetime(2024, 1, 5, 10, 30), True),
    (2, "beta", Decimal("20.00"), datetime(2024, 2, 11, 23, 59, 59), False),
    (3, None, None, None, None),
    (4, "", Decimal("-0.01"), datetime(2025, 3, 3, 12, 0), True),
]
ENGINES = ["postgresql", "mysql", "snowflake", "oracle", "sqlite", ""]


@pytest.mark.parametrize("engine", ENGINES)
def test_row_loop_matches_per_cell_resolution(engine: str):
    """The hoisted plan must reproduce _fingerprint_cell exactly, cell for cell."""
    for row in ROWS:
        expected = "\x1f".join(
            f"{col.lower()}="
            + _fingerprint_cell(
                row[COLUMNS.index(col)],
                column=col,
                dest_db_type=engine,
                dest_types=DEST_TYPES,
            )
            for col in sorted(COLUMNS, key=str.lower)
        )
        (_key, actual) = next(
            iter(
                _iter_fingerprints(
                    [row], COLUMNS, dest_db_type=engine, dest_types=DEST_TYPES
                )
            )
        )
        assert actual == expected


@pytest.mark.parametrize("engine", ENGINES)
def test_dict_and_tuple_rows_agree(engine: str):
    """Read-back hands dicts, the source hands tuples — both must digest alike."""
    tuples = checksum_rows(
        ROWS, COLUMNS, dest_db_type=engine, dest_types=DEST_TYPES
    )
    dicts = checksum_rows(
        [dict(zip(COLUMNS, r)) for r in ROWS],
        COLUMNS,
        dest_db_type=engine,
        dest_types=DEST_TYPES,
    )
    assert tuples == dicts


def test_short_rows_still_read_as_null():
    """A row shorter than the column list must pad with NULL, not raise."""
    short = [(1, "alpha")]
    (_key, fp) = next(
        iter(
            _iter_fingerprints(
                short, COLUMNS, dest_db_type="postgresql", dest_types=DEST_TYPES
            )
        )
    )
    assert "amount=\x00NULL\x00" in fp
    assert "flag=\x00NULL\x00" in fp


def test_sort_key_is_resolved_against_its_own_column():
    keyed = list(
        _iter_fingerprints(
            ROWS,
            COLUMNS,
            sort_key="id",
            dest_db_type="postgresql",
            dest_types=DEST_TYPES,
        )
    )
    assert [k for k, _ in keyed] == ["1", "2", "3", "4"]


def test_columns_absent_from_dest_types_still_fingerprint():
    """An unmapped column has no DDL to steer it; it must not fall over."""
    partial = {"id": "BIGINT"}
    digest = checksum_rows(
        ROWS, COLUMNS, dest_db_type="postgresql", dest_types=partial
    )
    assert len(digest) == 64
