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


def _db_request(src_path: Path, dst_path: Path):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite",
            database=str(src_path), table="t",
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite",
            database=str(dst_path), table="t",
        ),
        sync_mode="incremental_append",
        stream_contracts=[{
            "name": "t", "selected": True,
            "sync_mode": "incremental_append",
            "cursor_field": "updated_at",
            "cursor_semantics": "modification_timestamp",
            "primary_key": "id",
        }],
        skip_preflight=True,
        validation_mode="balanced",
    )


def _db_exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            if rows is None:
                conn.execute(sql)
            else:
                conn.executemany(sql, rows)
    finally:
        conn.close()


def _db_run(src_path: Path, dst_path: Path):
    from src.transfer.engine import UniversalTransferEngine
    from tests.test_property2_golden_path_never_blocked import _FakeMongo
    import src.transfer.engine as engine_mod

    fake = _FakeMongo()
    orig = getattr(engine_mod, "get_mongodb_service", None)
    engine_mod.get_mongodb_service = lambda: fake
    try:
        job_id = "d11db" + uuid.uuid4().hex[:14]
        fake.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(
            _db_request(src_path, dst_path), job_id
        )
    finally:
        if orig is not None:
            engine_mod.get_mongodb_service = orig


def test_d11_append_db_source_band_reread_never_re_appends(tmp_path, cursors):
    """DB-source append: held band re-read lands held+new rows only."""
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _db_exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, updated_at TEXT, amt INTEGER)")
    _db_exec(src, "INSERT INTO t VALUES (?, ?, ?)", [
        (1, "2024-01-01T00:00:00", "10"),
        (2, "2024-01-02T00:00:00", "not-a-number"),  # quarantines on INTEGER map
        (3, "2024-01-03T00:00:00", "30"),
        (4, "2024-01-04T00:00:00", "40"),
    ])
    first = _db_run(src, dst)
    assert first.success, getattr(first, "error", "")

    conn = sqlite3.connect(str(dst))
    landed = conn.execute("SELECT id, amt FROM t ORDER BY id").fetchall()
    conn.close()
    assert [r[0] for r in landed] == [1, 3, 4], landed
    key = sc.list_cursor_keys()[0]
    stored, meta = sc.get_watermark_record(key)
    assert "2024-01-02" in (stored or ""), stored
    assert (meta or {}).get("quarantine_holds", {}).get("rows"), meta

    # repair row 2 and append row 5
    _db_exec(src, "UPDATE t SET amt='20' WHERE id=2")
    _db_exec(src, "INSERT INTO t VALUES (5, '2024-01-05T00:00:00', '50')")
    second = _db_run(src, dst)
    assert second.success, getattr(second, "error", "")

    conn = sqlite3.connect(str(dst))
    landed = conn.execute("SELECT id, amt FROM t ORDER BY id").fetchall()
    conn.close()
    ids = [r[0] for r in landed]
    assert sorted(ids) == [1, 2, 3, 4, 5], landed
    assert len(ids) == len(set(ids)) == 5, f"duplicate rows appended: {landed}"


def test_d11_append_db_source_malformed_hold_fails_closed(tmp_path, cursors):
    """A hold record without band bounds refuses rather than duplicating."""
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _db_exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, updated_at TEXT, amt INTEGER)")
    _db_exec(src, "INSERT INTO t VALUES (?, ?, ?)", [
        (1, "2024-01-01T00:00:00", "10"),
        (2, "2024-01-02T00:00:00", "not-a-number"),  # quarantines → real hold
        (3, "2024-01-03T00:00:00", "30"),
    ])
    first = _db_run(src, dst)
    assert first.success, getattr(first, "error", "")

    key = sc.list_cursor_keys()[0]
    stored, meta = sc.get_watermark_record(key)
    assert (meta or {}).get("quarantine_holds"), meta
    # Corrupt the hold: band bounds missing → the band re-read cannot prove
    # which rows already landed, so the run must refuse, not duplicate.
    sc.set_watermark(key, stored or "", metadata={
        **(meta or {}),
        "quarantine_holds": {"band_high": "", "rows": []},
    })

    _db_exec(src, "UPDATE t SET amt=20 WHERE id=2")
    second = _db_run(src, dst)
    assert not second.success, "malformed hold must refuse, not land"
    err = str(getattr(second, "error", "") or "")
    assert "fails closed" in err or "quarantine hold" in err.lower(), err


def test_d11c_held_bookmark_round_trip_byte_exact():
    """A held ``cursor␟pk`` bookmark persists and presents byte-identical.

    Root cause pinned by the D11c fix: Python ``str.strip()`` eats \\x1c-\\x1f
    as whitespace, so a held composite bookmark ``"2024-01-02T00:00:00\\x1f"``
    silently degraded to ``"2024-01-02T00:00:00"`` — a cursor-only watermark
    that re-reads rows strictly past it and skips the quarantined row
    forever. The whole point of the composite bookmark is "cursor AND every
    row AT that cursor", so the byte sequence must survive the round trip.
    """
    from services.keyset_pagination import (
        KEYSET_SEP,
        decode_keyset_bookmark,
        encode_keyset_bookmark,
        is_cursor_only_bookmark,
        present_cursor_bookmark,
    )
    from services.sync_cursor import quarantine_held_watermark

    # 1) The hold produces a composite (cursor, "") bookmark.
    val, hold = quarantine_held_watermark(
        rejected_details=[{"values": {"id": 2, "updated_at": "2024-01-02T00:00:00"}}],
        cursor_column="updated_at", tiebreak_column="id",
        entry_watermark=None, running_cursor="2024-01-04T00:00:00",
    )
    assert hold is not None
    assert val.endswith(KEYSET_SEP), f"expected composite bookmark, got {val!r}"
    assert not is_cursor_only_bookmark(val)

    # 2) Byte-exact through the presentation path (the strip() regression).
    presented = present_cursor_bookmark(val)
    assert presented == val, f"bookmark degraded: {val!r} -> {presented!r}"

    # 3) Byte-exact through a store->load round trip.
    encoded = encode_keyset_bookmark(["2024-01-02T00:00:00", ""])
    assert encoded == "2024-01-02T00:00:00" + KEYSET_SEP
    assert present_cursor_bookmark(encoded) == encoded
    assert decode_keyset_bookmark(encoded, expected_parts=2) == [
        "2024-01-02T00:00:00", "",
    ]

    # 4) A composite pk part survives too (cursor␟pk, not just cursor␟).
    full = encode_keyset_bookmark(["2024-01-02T00:00:00", "42"])
    assert present_cursor_bookmark(full) == full
    assert decode_keyset_bookmark(full, expected_parts=2) == [
        "2024-01-02T00:00:00", "42",
    ]
