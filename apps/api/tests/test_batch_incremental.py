"""Batch (buffered) reads must be bounded by the same watermark as streaming.

Before this bound existed, an ``incremental_append`` whose destination was a
file export re-read the whole source every run and appended it again: run two of
a 200-row export left 450 rows and reported success.
"""

from __future__ import annotations

from typing import Any

import pytest

from services import sync_cursor
from services.batch_incremental import bind_batch_incremental


@pytest.fixture(autouse=True)
def _isolated_cursor_store(tmp_path, monkeypatch):
    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sync_cursor, "_cursor_collection", lambda: None, raising=False)
    yield


def _contracts(cursor: str = "updated_at", pk: str = "id") -> list[dict[str, Any]]:
    return [{
        "name": "orders",
        "sync_mode": "incremental_append",
        "cursor_field": cursor,
        "primary_key": pk,
    }]


def _bind(**over: Any):
    kwargs: dict[str, Any] = dict(
        sync_mode="incremental_append",
        stream_contracts=_contracts(),
        source_type="postgresql",
        source_database="dataflow",
        source_object="orders",
        dest_type="file_export",
        dest_database="",
        dest_object="orders.csv",
    )
    kwargs.update(over)
    return bind_batch_incremental(**kwargs)


ROWS = [
    {"id": "1", "updated_at": "2024-01-01T00:00:00"},
    {"id": "2", "updated_at": "2024-01-02T00:00:00"},
    {"id": "3", "updated_at": "2024-01-03T00:00:00"},
]


def test_full_refresh_reads_everything_and_writes_no_watermark():
    bound = _bind(sync_mode="full_refresh_append", stream_contracts=None)
    assert bound.active is False
    assert bound.bound(ROWS) == ROWS
    bound.commit()
    assert sync_cursor.get_watermark(bound.scope.cursor_key or "x") is None


def test_first_run_reads_everything_then_second_run_reads_only_the_delta():
    first = _bind()
    assert first.bound(ROWS) == ROWS
    first.commit()

    second = _bind()
    assert second.scope.watermark is not None
    delta = second.bound([*ROWS, {"id": "4", "updated_at": "2024-01-04T00:00:00"}])
    assert [r["id"] for r in delta] == ["4"]


def test_watermark_is_not_persisted_until_commit():
    run = _bind()
    run.bound(ROWS)
    assert _bind().scope.watermark is None  # nothing at rest yet
    run.commit()
    assert _bind().scope.watermark is not None


def test_row_without_a_cursor_value_is_refused_not_guessed():
    run = _bind()
    with pytest.raises(ValueError, match="carry no value for cursor"):
        run.bound([*ROWS, {"id": "4", "updated_at": None}])


def test_changed_cursor_column_is_refused_rather_than_compared_across_columns():
    first = _bind()
    first.bound(ROWS)
    first.commit()
    with pytest.raises(ValueError):
        _bind(stream_contracts=_contracts(cursor="created_at"))


def test_emptied_destination_with_a_historical_watermark_is_refused():
    first = _bind()
    first.bound(ROWS)
    first.commit()
    with pytest.raises(ValueError):
        _bind(dest_rows=0)


def test_composite_tiebreak_keeps_rows_sharing_the_cursor_instant():
    tied = [
        {"id": "1", "updated_at": "2024-01-01T00:00:00"},
        {"id": "2", "updated_at": "2024-01-01T00:00:00"},
    ]
    first = _bind()
    assert first.bound(tied) == tied
    first.commit()

    second = _bind()
    delta = second.bound([*tied, {"id": "3", "updated_at": "2024-01-01T00:00:00"}])
    assert [r["id"] for r in delta] == ["3"]


def test_the_cursor_key_follows_the_route_so_two_destinations_do_not_share_state():
    first = _bind()
    first.bound(ROWS)
    first.commit()
    other = _bind(dest_object="orders_backup.csv")
    assert other.scope.watermark is None
    assert other.bound(ROWS) == ROWS


def _scd2_contracts() -> list[dict[str, Any]]:
    return [{
        "name": "orders",
        "sync_mode": "scd2",
        "cursor_field": "updated_at",
        "primary_key": "id",
    }]


def test_scd2_reads_the_whole_snapshot_while_the_cursor_still_advances():
    """SCD2 proves itself against the destination's *current* population.

    Narrowing the read to the delta made run two carry one changed row into a
    reconcile that measured the whole 200-row current census — Validate cleared
    and Run failed its own row-count proof. The streaming SQL path snapshots the
    whole source; the buffered path must read the same scope.
    """
    first = _bind(sync_mode="scd2", stream_contracts=_scd2_contracts())
    assert first.bound(ROWS) == ROWS
    first.commit()

    second = _bind(sync_mode="scd2", stream_contracts=_scd2_contracts())
    assert second.scope.watermark is not None  # cursor state is still tracked
    changed = [*ROWS, {"id": "4", "updated_at": "2024-01-04T00:00:00"}]
    assert [r["id"] for r in second.bound(changed)] == ["1", "2", "3", "4"]
    second.commit()
    assert _bind(
        sync_mode="scd2", stream_contracts=_scd2_contracts()
    ).scope.watermark != first.scope.watermark


def test_incremental_append_still_narrows_after_the_scd2_carve_out():
    first = _bind()
    first.bound(ROWS)
    first.commit()
    second = _bind()
    assert second.bound(ROWS) == []
