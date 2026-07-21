"""Debezium DDD-3 snapshot window: stream-wins PK collision."""

from __future__ import annotations

from services.cdc_snapshot_window import SnapshotWindow, merge_snapshot_chunk_with_stream


def test_stream_wins_over_snapshot_read() -> None:
    win = SnapshotWindow(window_id="w1", primary_key="id")
    win.open_window()
    win.add_snapshot_rows([{"id": "1", "v": "snap"}, {"id": "2", "v": "snap"}])
    win.apply_stream_event(op="u", row={"id": "1", "v": "live"})
    emitted = win.close_window()
    by_id = {r["id"]: r for r in emitted if not r.get("__deleted")}
    assert by_id["1"]["v"] == "live"
    assert by_id["2"]["v"] == "snap"
    assert win.stats()["stream_overrides"] == 1


def test_stream_delete_tombstones_snapshot_row() -> None:
    win = SnapshotWindow(window_id="w2", primary_key="id")
    win.open_window()
    win.add_snapshot_rows([{"id": "9", "v": "x"}])
    win.apply_stream_event(op="d", pk="9")
    emitted = win.close_window()
    assert any(r.get("__deleted") and str(r.get("id")) == "9" for r in emitted)
    assert not any(r.get("id") == "9" and not r.get("__deleted") for r in emitted)


def test_snapshot_cannot_overwrite_stream() -> None:
    win = SnapshotWindow(window_id="w3", primary_key="id")
    win.open_window()
    win.apply_stream_event(op="c", row={"id": "1", "v": "live"})
    win.add_snapshot_rows([{"id": "1", "v": "snap"}])
    emitted = win.close_window()
    assert emitted[0]["v"] == "live"


def test_merge_helper() -> None:
    rows, stats = merge_snapshot_chunk_with_stream(
        window_id="w4",
        primary_key="id",
        snapshot_rows=[{"id": "a", "n": "1"}],
        stream_events=[{"op": "u", "row": {"id": "a", "n": "2"}}],
    )
    assert rows[0]["n"] == "2"
    assert stats["stream_overrides"] == 1
