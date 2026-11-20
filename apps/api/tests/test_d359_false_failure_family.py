"""False-failure family: this run's rows, the same filter, carrier precision.

D331 the proof re-read uses the run's source_filter.
D325 BSON dates compare at milliseconds.
D359 an append versions by cursor window, not by key lookup.
D335 the boundary row of an exclusive (cursor, key) watermark is not in the next window.
D377 incremental_append on a heap warns, it does not block a new version.
D204 rebaseline is already covered by tests/test_d204_rebaseline_keys_like_job.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from services.destination_key_collision_probe import (  # noqa: E402
    unenforced_append_warns,
)
from services.incremental_drift import RunWindow  # noqa: E402
from services.row_filter import apply_row_filter  # noqa: E402
from src.transfer.reconcile_step import (  # noqa: E402
    _truncate_instant_to_milliseconds,
    stamp_run_proof_context,
)


def test_proof_context_keeps_the_source_filter() -> None:
    summary: dict = {}

    class _Req:
        source_filter = {"column": "qty", "op": "gte", "value": 100}

    stamp_run_proof_context(summary, _Req())
    kept = apply_row_filter(
        [{"qty": 10}, {"qty": 100}, {"qty": None}],
        summary["source_filter"],
    )
    assert [r["qty"] for r in kept] == [100]


def test_bson_instant_compares_at_milliseconds() -> None:
    assert _truncate_instant_to_milliseconds("2026-10-01 00:01:00.123456") == (
        "2026-10-01 00:01:00.123"
    )
    assert _truncate_instant_to_milliseconds("2026-10-01T00:01:00.123000") == (
        "2026-10-01T00:01:00.123"
    )
    # A value that already fits the carrier is unchanged at three digits.
    assert _truncate_instant_to_milliseconds("2026-10-01 00:01:00.123") == (
        "2026-10-01 00:01:00.123"
    )


def test_append_window_excludes_the_previous_version_and_keeps_a_tie() -> None:
    window = RunWindow(
        cursor="updated_at",
        pk="id",
        seed="2026-10-01 00:30:00\x1f30",
        max=None,
        seed_inclusive=False,
    )
    boundary = {"id": "30", "updated_at": "2026-10-01 00:30:00"}
    later_tie = {"id": "31", "updated_at": "2026-10-01 00:30:00"}
    changed = {"id": "3", "updated_at": "2026-10-01 00:40:00"}
    # The stored watermark is the boundary row. It is not in the next window.
    # A peer with the same timestamp and a later key is. A bumped row is.
    assert window.position(boundary) == "before"
    assert window.position(later_tie) == "in"
    assert window.position(changed) == "in"


def test_incremental_append_on_a_heap_warns_instead_of_blocking() -> None:
    assert unenforced_append_warns(
        enforced=False,
        resume_redelivery=False,
        has_delta_scope=True,
        sync_mode="incremental_append",
    )
    # A table that enforces the key still blocks, on every run.
    assert not unenforced_append_warns(
        enforced=True,
        resume_redelivery=False,
        has_delta_scope=True,
        sync_mode="incremental_append",
    )
    # Reloading the whole table onto keys already stored still blocks.
    assert not unenforced_append_warns(
        enforced=False,
        resume_redelivery=False,
        has_delta_scope=True,
        sync_mode="full_refresh_append",
    )
