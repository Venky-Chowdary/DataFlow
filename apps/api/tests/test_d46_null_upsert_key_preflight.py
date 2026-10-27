"""D46 — a NULL upsert key passed preflight and failed at write, misleadingly.

The source uniqueness probe is a GROUP BY ... HAVING COUNT(*) > 1, so a single
NULL key is not a duplicate and the Validate sample (capped) never saw a row
deep in the table. Execute then wrote the first batches and died on the NULL
with "Write referenced a column that is not on the destination".

The probe now also counts NULL-key rows on the population and G9 refuses the
keyed write before any row moves, naming the count and the key.

Real SQLite → SQLite through the engine with preflight ON.
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


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            if rows is None:
                conn.execute(sql)
            else:
                conn.executemany(sql, rows)
    finally:
        conn.close()


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _upsert(src: Path, dst: Path, mode: str) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="upsert",
        validation_mode=mode,
        duplicate_policy="upsert",
        stream_contracts=[
            {"name": "t", "selected": True, "sync_mode": "upsert", "primary_key": "code"}
        ],
    )


@pytest.mark.parametrize("mode", ["strict", "balanced"])
def test_null_key_beyond_the_sample_is_refused_at_preflight(tmp_path, isolated, mode):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE t (code TEXT, name TEXT)")
    rows = [(f"k{i}", "v") for i in range(20_000)] + [(None, "late")]
    _exec(src, "INSERT INTO t VALUES (?, ?)", rows)

    job_id = "d46" + uuid.uuid4().hex[:16]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(_upsert(src, dst, mode), job_id)

    assert not result.success
    assert "1 source row(s) have a NULL upsert key" in str(result.error), result.error
    assert "not on the destination" not in str(result.error)
    assert "fidelity" not in str(result.error).lower()
    conn = sqlite3.connect(str(dst))
    try:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    finally:
        conn.close()
    assert "t" not in tables  # refused before any row moved
