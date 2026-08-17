"""The write quarantine matrix must name the source row it rejected.

The matrix runs the whole batch at once and only falls back to one row at a
time when something is quarantined — a clean batch cannot mislabel anything,
but a rejected row still has to carry its global source row number.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    apply_write_quarantine_matrix_keeping_numbers,
)

_COLS = ["id", "code"]
_TYPES = ["BIGINT", "VARCHAR(4)"]


def _run(rows, numbers):
    rejected: list[dict] = []
    kept, nums = apply_write_quarantine_matrix_keeping_numbers(
        rows,
        _COLS,
        _TYPES,
        rejected,
        "strict",
        dialect_label="SQLite",
        dest_db="sqlite",
        source_row_numbers=numbers,
    )
    return kept, nums, rejected


def test_clean_batch_keeps_every_row_and_its_source_number():
    rows = [(i, "ok") for i in range(1, 6)]
    numbers = [900, 901, 902, 903, 904]

    kept, nums, rejected = _run(rows, numbers)

    assert kept == rows
    assert nums == numbers
    assert rejected == []


def test_a_rejected_row_is_named_by_its_source_row_number():
    rows = [(1, "ok"), (2, "far-too-wide"), (3, "ok")]
    numbers = [900, 901, 902]

    kept, nums, rejected = _run(rows, numbers)

    assert kept == [(1, "ok"), (3, "ok")]
    assert nums == [900, 902]
    assert [d.get("row") for d in rejected] == [901]
