"""Honest transfer progress — no chunk theater / jump-to-90%."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.batch_progress import (  # noqa: E402
    compute_transfer_progress_pct,
    effective_backfill_new_fields,
)


def test_writing_progress_tracks_row_ratio_not_chunks():
    # Halfway through rows — even if this is "chunk 1 of 1" (old bug → 90%).
    pct = compute_transfer_progress_pct(
        phase="writing",
        rows_processed=50_000,
        total_rows=100_000,
        chunk=1,
        chunks=1,
    )
    assert pct is not None
    assert 40 <= pct <= 60
    assert pct != 90


def test_writing_progress_reaches_ceiling_only_when_all_rows_written():
    mid = compute_transfer_progress_pct(phase="writing", rows_processed=10, total_rows=100)
    done = compute_transfer_progress_pct(phase="writing", rows_processed=100, total_rows=100)
    assert mid is not None and done is not None
    assert mid < done
    assert done == 98  # reconcile owns 99; complete owns 100


def test_unknown_total_does_not_fake_high_percent():
    pct = compute_transfer_progress_pct(
        phase="writing",
        rows_processed=12_345,
        total_rows=0,
        chunk=5,
        chunks=5,
    )
    assert pct is None


def test_reconcile_and_complete_are_exact():
    assert compute_transfer_progress_pct(phase="reconcile") == 99
    assert compute_transfer_progress_pct(phase="complete") == 100


def test_propagate_schema_policy_implies_backfill():
    assert effective_backfill_new_fields(backfill_new_fields=False, schema_policy="propagate_columns")
    assert effective_backfill_new_fields(backfill_new_fields=False, schema_policy="propagate_all")
    assert not effective_backfill_new_fields(backfill_new_fields=False, schema_policy="manual_review")
    assert effective_backfill_new_fields(backfill_new_fields=True, schema_policy="manual_review")


def test_create_compatible_new_mapping_implies_backfill():
    assert effective_backfill_new_fields(
        backfill_new_fields=False,
        schema_policy="manual_review",
        mappings=[{
            "source": "_id",
            "target": "_id",
            "create_new": True,
            "assignment_strategy": "create_compatible_new",
        }],
    )
    assert not effective_backfill_new_fields(
        backfill_new_fields=False,
        schema_policy="manual_review",
        mappings=[{"source": "a", "target": "b", "create_new": False}],
    )


@pytest.mark.parametrize(
    "rows,total,expected_min,expected_max",
    [
        (0, 1000, 5, 5),
        (250, 1000, 25, 35),
        (500, 1000, 48, 55),
        (1000, 1000, 98, 98),
    ],
)
def test_monotonic_row_progress_band(rows, total, expected_min, expected_max):
    pct = compute_transfer_progress_pct(phase="writing", rows_processed=rows, total_rows=total)
    assert pct is not None
    assert expected_min <= pct <= expected_max
