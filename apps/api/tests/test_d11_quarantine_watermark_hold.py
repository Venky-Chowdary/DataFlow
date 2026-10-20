"""D11 — a quarantined row must hold the incremental watermark at its cursor.

Production defect: a scheduled incremental run that quarantined a row still
advanced the route watermark past the row's cursor; every later run reported
"no new source rows" and the row was lost forever. The watermark now clamps
to just below the lowest quarantined cursor (a composite ``(cursor, "")``
bookmark re-reads every row at that cursor), the run summary names the held
rows, and an unknown/unexpressible hold falls back to the entry watermark —
fail-closed, never silent.
"""

import os
import sqlite3
import sys
import uuid
from pathlib import Path

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest

import services.sync_cursor as sc
from src.transfer.file_stream import stream_file_to_database
from src.transfer.models import EndpointConfig


def _csv(rows: list[tuple]) -> bytes:
    body = "id,updated_at,amt\n" + "".join(
        f"{i},{u},{a}\n" for i, u, a in rows
    )
    return body.encode()


def _contracts() -> list[dict]:
    return [{
        "selected": True,
        "sync_mode": "incremental_deduped",
        "cursor_field": "updated_at",
        "cursor_semantics": "modification_timestamp",
        "primary_key": "id",
    }]


def _mappings() -> list[dict]:
    return [
        {"source": "id", "target": "id", "target_type": "INTEGER", "confidence": 0.99},
        {"source": "updated_at", "target": "updated_at", "target_type": "TEXT", "confidence": 0.99},
        {
            "source": "amt", "target": "amt", "target_type": "INTEGER",
            "transform": "integer", "confidence": 0.99,
        },
    ]


def _dest(tmp_path: Path) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="sqlite",
        database=str(tmp_path / "out.db"), table="t",
    )


def _rows(db: Path) -> list[tuple]:
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute("SELECT id, updated_at, amt FROM t ORDER BY id").fetchall()
    finally:
        conn.close()


@pytest.fixture()
def cursors(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)


def test_d11_quarantined_row_holds_watermark_and_lands_on_repair(tmp_path, cursors):
    rows = [
        (1, "2024-01-01T00:00:00", 10),
        (2, "2024-01-02T00:00:00", "not-a-number"),  # quarantines
        (3, "2024-01-03T00:00:00", 30),
        (4, "2024-01-04T00:00:00", 40),
    ]
    written, _ddl, summary, _cols = stream_file_to_database(
        _csv(rows), "t.csv", _dest(tmp_path), _mappings(),
        {"id": "INTEGER", "updated_at": "TEXT", "amt": "INTEGER"},
        job_id="d11" + uuid.uuid4().hex[:16],
        sync_mode="incremental_deduped",
        stream_contracts=_contracts(),
        validation_mode="balanced",
        skip_preflight=True,
    )
    assert written == 3
    assert int(summary.get("rejected_rows") or 0) == 1
    # The watermark must NOT have advanced past row 2's cursor.
    key = sc.list_cursor_keys()[0]
    stored, _meta = sc.get_watermark_record(key)
    assert stored != "2024-01-04T00:00:00", stored
    assert "2024-01-02" in stored, stored  # held at the quarantined cursor
    assert summary.get("watermark_held_for_quarantine"), summary.get("watermark_held_for_quarantine")

    # Fix the source row and append a fifth.
    rows[1] = (2, "2024-01-02T00:00:00", 20)
    rows.append((5, "2024-01-05T00:00:00", 50))
    written2, _d2, summary2, _c2 = stream_file_to_database(
        _csv(rows), "t.csv", _dest(tmp_path), _mappings(),
        {"id": "INTEGER", "updated_at": "TEXT", "amt": "INTEGER"},
        job_id="d11" + uuid.uuid4().hex[:16],
        sync_mode="incremental_deduped",
        stream_contracts=_contracts(),
        validation_mode="balanced",
        skip_preflight=True,
    )
    # Final read-back has every row exactly once — no lost row, no duplicates.
    assert _rows(tmp_path / "out.db") == [
        (1, "2024-01-01T00:00:00", 10),
        (2, "2024-01-02T00:00:00", 20),
        (3, "2024-01-03T00:00:00", 30),
        (4, "2024-01-04T00:00:00", 40),
        (5, "2024-01-05T00:00:00", 50),
    ]


def test_d11_clean_run_still_advances_to_max(tmp_path, cursors):
    rows = [(i, f"2024-01-0{i}T00:00:00", i * 10) for i in range(1, 5)]
    written, _d, summary, _c = stream_file_to_database(
        _csv(rows), "t.csv", _dest(tmp_path), _mappings(),
        {"id": "INTEGER", "updated_at": "TEXT", "amt": "INTEGER"},
        job_id="d11" + uuid.uuid4().hex[:16],
        sync_mode="incremental_deduped",
        stream_contracts=_contracts(),
        validation_mode="balanced",
        skip_preflight=True,
    )
    assert written == 4
    assert not summary.get("watermark_held_for_quarantine")
    stored, _m = sc.get_watermark_record(sc.list_cursor_keys()[0])
    assert "2024-01-04" in stored


def test_d11_helper_clamps_and_falls_back():
    from services.sync_cursor import quarantine_held_watermark

    details = [{"values": {"id": 2, "updated_at": "2024-01-02T00:00:00", "amt": "x"}}]
    val, hold = quarantine_held_watermark(
        rejected_details=details, cursor_column="updated_at",
        tiebreak_column="id", entry_watermark=None,
        running_cursor="2024-01-04T00:00:00",
    )
    assert val.startswith("2024-01-02T00:00:00")
    assert hold["lowest_quarantined_cursor"] == "2024-01-02T00:00:00"
    # No details -> plain advance.
    val2, hold2 = quarantine_held_watermark(
        rejected_details=[], cursor_column="updated_at",
        tiebreak_column="id", entry_watermark=None, running_cursor="z",
    )
    assert (val2, hold2) == ("z", None)
    # Cursor not recoverable from the detail -> hold at the entry watermark.
    val3, hold3 = quarantine_held_watermark(
        rejected_details=[{"reason": "no row payload"}], cursor_column="updated_at",
        tiebreak_column="id", entry_watermark="entry-wm", running_cursor="z",
    )
    assert val3 == "entry-wm" and hold3["quarantine_cursor_unknown"]


def _append_contracts() -> list[dict]:
    return [{
        "selected": True,
        "sync_mode": "incremental_append",
        "cursor_field": "updated_at",
        "cursor_semantics": "modification_timestamp",
        "primary_key": "id",
    }]


def test_d11_append_mode_re_read_never_re_appends_landed_rows(tmp_path, cursors):
    """incremental_append: held band re-read must not duplicate landed rows."""
    rows = [
        (1, "2024-01-01T00:00:00", 10),
        (2, "2024-01-02T00:00:00", "not-a-number"),  # quarantines
        (3, "2024-01-03T00:00:00", 30),
        (4, "2024-01-04T00:00:00", 40),
    ]
    args = (
        _dest(tmp_path), _mappings(),
        {"id": "INTEGER", "updated_at": "TEXT", "amt": "INTEGER"},
    )
    written, _ddl, summary, _cols = stream_file_to_database(
        _csv(rows), "t.csv", *args,
        job_id="d11a" + uuid.uuid4().hex[:15],
        sync_mode="incremental_append",
        stream_contracts=_append_contracts(),
        validation_mode="balanced",
        skip_preflight=True,
    )
    assert written == 3
    key = sc.list_cursor_keys()[0]
    stored, meta = sc.get_watermark_record(key)
    assert "2024-01-02" in stored, stored
    holds = (meta or {}).get("quarantine_holds") or {}
    assert holds.get("rows"), holds  # identities persisted for the re-read

    rows[1] = (2, "2024-01-02T00:00:00", 20)  # repair
    rows.append((5, "2024-01-05T00:00:00", 50))
    written2, _d2, summary2, _c2 = stream_file_to_database(
        _csv(rows), "t.csv", *args,
        job_id="d11a" + uuid.uuid4().hex[:15],
        sync_mode="incremental_append",
        stream_contracts=_append_contracts(),
        validation_mode="balanced",
        skip_preflight=True,
    )
    # Rows 3+4 were re-read (inside the held band) but must NOT re-append.
    assert _rows(tmp_path / "out.db") == [
        (1, "2024-01-01T00:00:00", 10),
        (2, "2024-01-02T00:00:00", 20),
        (3, "2024-01-03T00:00:00", 30),
        (4, "2024-01-04T00:00:00", 40),
        (5, "2024-01-05T00:00:00", 50),
    ]
