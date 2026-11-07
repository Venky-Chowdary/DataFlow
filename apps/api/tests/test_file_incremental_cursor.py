"""Incremental sync of a file source must be bounded by its cursor.

A database reader bounds the delta in its WHERE clause; a file arrives whole, so
the same contract has to be honoured after the parse. Without it, the second run
of an "incremental append" re-appends every row the file still holds — the
duplicate load operators report as corruption.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

duckdb = pytest.importorskip(
    "duckdb", reason="requires the optional DuckDB test dependency"
)

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import sync_cursor  # noqa: E402
from services.sync_cursor import records_after_watermark  # noqa: E402
from src.transfer.file_stream import stream_file_to_database  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402


class _FakeCheckpointService:
    def __init__(self) -> None:
        self.checkpoints: dict[str, dict] = {}
        self.failed_saves = 0

    @property
    def has_failed_saves(self) -> bool:
        return self.failed_saves > 0

    def save(self, checkpoint) -> bool:
        self.checkpoints[checkpoint.job_id] = checkpoint.to_dict()
        return True

    def require_save(self, checkpoint) -> None:
        self.save(checkpoint)

    def load(self, job_id: str):
        return self.checkpoints.get(job_id)


def _csv(rows: list[tuple[int, str]]) -> bytes:
    body = "id,updated_at\n" + "\n".join(f"{i},{ts}" for i, ts in rows)
    return body.encode("utf-8")


ROWS_DAY1 = [
    (1, "2024-01-01T00:00:00"),
    (2, "2024-01-02T00:00:00"),
]
ROWS_DAY2 = ROWS_DAY1 + [
    (3, "2024-01-03T00:00:00"),
    (4, "2024-01-04T00:00:00"),
]


def _isolate_cursor_store(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)


def _contracts() -> list[dict]:
    return [
        {
            "selected": True,
            "sync_mode": "incremental_append",
            "cursor_field": "updated_at",
            "cursor_semantics": "modification_timestamp",
            "primary_key": "id",
        }
    ]


def _run(
    payload: bytes,
    dest: EndpointConfig,
    cp: _FakeCheckpointService,
    job_id: str = "000000000000000000000001",
) -> int:
    written, _, _, _ = stream_file_to_database(
        payload,
        "events.csv",
        dest,
        [],
        {},
        job_id=job_id,
        checkpoint_service=cp,
        sync_mode="incremental_append",
        stream_contracts=_contracts(),
    )
    return written


def test_file_incremental_append_writes_only_the_delta(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    dest = EndpointConfig(
        kind="database",
        format="duckdb",
        database=str(tmp_path / "events.duckdb"),
        table="events",
    )
    cp = _FakeCheckpointService()

    assert _run(_csv(ROWS_DAY1), dest, cp) == 2
    # The file still carries day 1; only the two new rows may be written.
    assert _run(_csv(ROWS_DAY2), dest, cp, job_id="000000000000000000000002") == 2

    con = duckdb.connect(str(tmp_path / "events.duckdb"))
    ids = [r[0] for r in con.execute("SELECT id FROM events ORDER BY id").fetchall()]
    con.close()
    assert ids == [1, 2, 3, 4]


def test_file_incremental_append_reruns_unchanged_file_as_no_op(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    dest = EndpointConfig(
        kind="database",
        format="duckdb",
        database=str(tmp_path / "events.duckdb"),
        table="events",
    )
    cp = _FakeCheckpointService()

    assert _run(_csv(ROWS_DAY1), dest, cp) == 2
    # Nothing new: a bounded read with an empty delta is a correct no-op, not an
    # empty file, and it must not raise or duplicate.
    assert _run(_csv(ROWS_DAY1), dest, cp, job_id="000000000000000000000002") == 0

    con = duckdb.connect(str(tmp_path / "events.duckdb"))
    count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    con.close()
    assert count == 2


def test_file_incremental_refuses_rows_without_a_cursor_value(monkeypatch, tmp_path):
    _isolate_cursor_store(monkeypatch, tmp_path)
    dest = EndpointConfig(
        kind="database",
        format="duckdb",
        database=str(tmp_path / "events.duckdb"),
        table="events",
    )
    cp = _FakeCheckpointService()

    with pytest.raises(ValueError, match="no value for cursor"):
        _run(_csv(ROWS_DAY1) + b"\n3,", dest, cp)


def test_records_after_watermark_bounds_and_reports_unbounded():
    records = [
        {"id": "1", "updated_at": "2024-01-01T00:00:00"},
        {"id": "2", "updated_at": "2024-01-03T00:00:00"},
        {"id": "3", "updated_at": ""},
    ]
    delta, unbounded = records_after_watermark(
        records, "updated_at", "2024-01-02T00:00:00"
    )
    assert [r["id"] for r in delta] == ["2"]
    assert unbounded == 1


def test_records_after_watermark_without_watermark_keeps_everything():
    records = [{"id": "1", "updated_at": "2024-01-01T00:00:00"}]
    delta, unbounded = records_after_watermark(records, "updated_at", None)
    assert delta == records
    assert unbounded == 0


def test_records_after_watermark_uses_composite_tiebreak():
    records = [
        {"id": "7", "updated_at": "2024-01-02T00:00:00"},
        {"id": "9", "updated_at": "2024-01-02T00:00:00"},
    ]
    watermark = sync_cursor.encode_keyset_bookmark(["2024-01-02T00:00:00", "7"])
    delta, unbounded = records_after_watermark(
        records, "updated_at", watermark, primary_key="id"
    )
    # The peer row sharing the timestamp is not skipped, and the row already at
    # rest is not sent again.
    assert [r["id"] for r in delta] == ["9"]
    assert unbounded == 0
