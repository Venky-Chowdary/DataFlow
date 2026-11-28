"""Finding 8 review: the writer re-checks the Oracle session on every batch.

In-process engine run through the generic SQL writer the Oracle destination
uses (a SQLite file behind a ``generic_sql`` endpoint stands in for Oracle,
the session read-back faked): the destination session reads back UTC for
batch 1 and something else from batch 2. Batch 1 stays committed, batch 2 is
not written, the run is not green, the error counts the rows batch 1
committed, and the stored watermark does not move past batch 1. (The engine's
own failure handling then removes what this run wrote; the error says so.)
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

ROWS, BATCH = 30, 10


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.fixture()
def oracle_like_sqlite(monkeypatch):
    """SQLite plays the Oracle destination for the session checks only."""
    import connectors.oracle_session as osess
    import services.oracle_session_scope as scope
    import src.transfer.stream as stream

    monkeypatch.setenv("DATAFLOW_SQLITE_SQLITE_COPY", "0")  # the row writer, batch by batch
    monkeypatch.setattr(stream, "adaptive_chunk_size", lambda *a, **k: BATCH)
    monkeypatch.setattr(scope, "is_oracle", lambda t: str(t or "").strip().lower() in {"oracle", "generic_sql"})
    monkeypatch.setattr(scope, "probe_oracle_session",
                        lambda cfg: (True, "session time zone read back as +00:00", "+00:00"))
    reads = {"n": 0}

    def confirm(_conn):
        reads["n"] += 1
        if reads["n"] == 1:
            return True, "session time zone read back as +00:00"
        return False, "session time zone read back as Asia/Kolkata, not UTC"

    monkeypatch.setattr(osess, "confirm_utc_session", confirm)
    return reads


def _seed(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?, ?)", [(i, i, f"v{i}") for i in range(1, ROWS + 1)])
    conn.close()


def _count(path: Path) -> int:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def test_session_lost_on_batch_two_keeps_batch_one_and_fails_the_run(tmp_path, isolated, oracle_like_sqlite):
    from services import sync_cursor as sc

    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _seed(src)
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="generic_sql", connection_string=f"sqlite:///{dst}",
                                   table="t"),
        sync_mode="incremental_append",
        stream_contracts=[{"name": "t", "cursor_field": "seq", "primary_key": "id",
                           "sync_mode": "incremental_append", "selected": True}],
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id = "f8lost" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)

    assert oracle_like_sqlite["n"] >= 2, "the writer never re-read the session"
    assert not result.success
    error = str(result.error)
    assert f"had committed {BATCH} row(s)" in error, error  # batch 1 was committed
    assert "this batch is not written" in error
    assert _count(dst) <= BATCH  # never anything past batch 1
    keys = sc.list_cursor_keys()
    if keys:
        record = sc.get_watermark_record(keys[0])
        watermark = record[0] if record else None
        assert watermark is None or int(str(watermark).split(sc.KEYSET_SEP)[0]) <= BATCH, watermark
    assert isolated.get_job(job_id)["oracle_session"]["utc_pinned"] is True
