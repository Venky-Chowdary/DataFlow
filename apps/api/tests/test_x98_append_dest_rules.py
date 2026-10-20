"""X-98 — one rule for append history tables on every engine, and the
duplicate-key message names the actual colliding keys.

X-98(a): an append destination must not re-declare the source PRIMARY KEY /
UNIQUE — append writes repeated PK values by design (run 2 appends id2/id3
versions alongside the originals). The create scope now honors
``carry_keys=False`` for appending sync modes on every engine.

X-98(b): ``duplicate key (id) — first row id=8`` was wrong — id 8 was a
brand-new key; the colliding keys were 2 and 3 (already landed). The message
now names batch-internal repeats + destination-held keys.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest


def _mk_src(tmp: Path) -> Path:
    src = tmp / "src.db"
    conn = sqlite3.connect(src)
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, amt TEXT, updated_at TEXT)"
    )
    conn.executemany(
        "INSERT INTO t VALUES (?,?,?)",
        [
            (1, "10", "2024-01-01T00:00:00"),
            (2, "20", "2024-01-02T00:00:00"),
            (3, "30", "2024-01-03T00:00:00"),
        ],
    )
    conn.commit()
    conn.close()
    return src


def _req(src: Path, dst: Path):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="out"
        ),
        sync_mode="incremental_append",
        skip_preflight=True,
        stream_contracts=[
            {
                "name": "t",
                "selected": True,
                "sync_mode": "incremental_append",
                "primary_key": "id",
                "cursor_field": "updated_at",
            }
        ],
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "amt", "target": "amt", "target_type": "TEXT"},
            {
                "source": "updated_at",
                "target": "updated_at",
                "target_type": "TEXT",
            },
        ],
    )


def _run(src: Path, dst: Path):
    from src.transfer.engine import UniversalTransferEngine

    return UniversalTransferEngine().execute_tracked(
        _req(src, dst), uuid.uuid4().hex[:24]
    )


def _dest_ddl(dst: Path) -> str:
    conn = sqlite3.connect(dst)
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='out'"
        ).fetchone()
        return (row or ("",))[0]
    finally:
        conn.close()


def test_append_fast_path_dest_has_no_unique_pk(tmp_path, monkeypatch):
    """The staging fast path's CREATE must not re-declare the source PK —
    run 2 re-appends the same pk by design and must succeed on every engine."""
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")

    src = _mk_src(tmp_path)
    dst = tmp_path / "dst.db"
    r1 = _run(src, dst)
    assert r1.success, r1.error
    upper = _dest_ddl(dst).upper()
    assert "PRIMARY KEY" not in upper, upper
    assert "UNIQUE" not in upper, upper

    conn = sqlite3.connect(src)
    conn.execute(
        "UPDATE t SET amt='20.5', updated_at='2024-02-01T00:00:00' WHERE id=2"
    )
    conn.execute(
        "UPDATE t SET amt='30.5', updated_at='2024-02-02T00:00:00' WHERE id=3"
    )
    conn.commit()
    conn.close()
    r2 = _run(src, dst)
    assert r2.success, r2.error
    conn = sqlite3.connect(dst)
    ids = [r[0] for r in conn.execute("SELECT id FROM out ORDER BY rowid")]
    conn.close()
    assert ids.count(2) == 2 and ids.count(3) == 2, ids


def test_append_carrying_source_key_would_fail(tmp_path, monkeypatch):
    """Negative cell: if append still carried the source PK (the pre-fix bug),
    run 2 dies on UNIQUE — this proves the create scope's carry_keys is what
    makes the append leg pass."""
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(
        "services.destination_key_collision_probe."
        "sync_mode_appends_without_key_resolution",
        lambda *_a, **_k: False,
    )

    src = _mk_src(tmp_path)
    dst = tmp_path / "dst.db"
    r1 = _run(src, dst)
    assert r1.success, r1.error
    upper = _dest_ddl(dst).upper()
    assert "PRIMARY KEY" in upper or "UNIQUE" in upper, upper

    conn = sqlite3.connect(src)
    conn.execute(
        "UPDATE t SET amt='20.5', updated_at='2024-02-01T00:00:00' WHERE id=2"
    )
    conn.commit()
    conn.close()
    r2 = _run(src, dst)
    assert not r2.success
    assert "UNIQUE" in str(r2.error or "").upper(), r2.error


def test_duplicate_key_message_names_colliding_keys(tmp_path):
    """The message names the destination-held / repeated keys, not batch[0]."""
    from connectors.sqlite_writer import _sqlite_duplicate_key_message

    db = tmp_path / "d.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE out (id INTEGER PRIMARY KEY, amt TEXT)")
    conn.executemany("INSERT INTO out VALUES (?,?)", [(2, "a"), (3, "b")])
    conn.commit()
    batch = [
        (8, "new8"),  # batch[0] — brand-new key, NOT the culprit
        (2, "dup2"),
        (3, "dup3"),
        (9, "new9"),
    ]
    exc = sqlite3.IntegrityError("UNIQUE constraint failed: out.id")
    msg = _sqlite_duplicate_key_message(
        exc,
        ["id"],
        ["id", "amt"],
        batch,
        cursor=conn.cursor(),
        table_quoted='"out"',
    )
    conn.close()
    assert "id=2" in msg and "id=3" in msg, msg
    assert "first row" not in msg, msg


def test_duplicate_key_message_names_in_batch_repeats(tmp_path):
    """Batch-internal repeats are named even with no destination probe."""
    from connectors.sqlite_writer import _sqlite_duplicate_key_message

    exc = sqlite3.IntegrityError("UNIQUE constraint failed: out.id")
    batch = [(8, "a"), (2, "x"), (2, "y"), (9, "z")]
    msg = _sqlite_duplicate_key_message(
        exc, ["id"], ["id", "amt"], batch
    )
    assert "id=2" in msg, msg
    assert "id=8" not in msg, msg
