"""D99 — rows with a NULL cursor are left out of every incremental run after the first.

``cursor > watermark`` is never true for NULL. Strict Validate blocks with the
live count (tests/test_d99_null_cursor_rows.py); under balanced the run goes
ahead and must say how many rows it could not see. The count was only
stamped for the cursor-pushdown family, so a SQLite route — COPY or row path,
both of which bound the read in SQL — reported a clean run while the row
never arrived.

Real SQLite → SQLite runs through the engine with preflight ON.
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


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _setup(tmp_path: Path, validation_mode: str, row_path: bool):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    with sqlite3.connect(src) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER, v TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?, ?)", [(i, i, f"v{i}") for i in range(1, 4)])
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        stream_contracts=[{
            "name": "t", "cursor_field": "seq", "cursor_semantics": "monotonic_sequence",
            "primary_key": ["id"], "sync_mode": "incremental_append", "selected": True,
        }],
        validation_mode=validation_mode,
        # A pass-through filter declines COPY: exercises the row path.
        source_filter={"column": "id", "op": "gt", "value": 0} if row_path else {},
    )
    return src, dst, request


def _run(fake, request):
    job_id = "d99" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


@pytest.mark.parametrize("row_path", [False, True], ids=["copy", "row_path"])
def test_balanced_run_reports_the_null_cursor_rows_it_cannot_see(tmp_path, isolated, row_path):
    src, dst, request = _setup(tmp_path, "balanced", row_path)
    assert _run(isolated, request).success
    with sqlite3.connect(src) as conn:
        conn.executemany("INSERT INTO t VALUES (?, ?, ?)", [(4, None, "null4"), (5, 5, "v5")])

    second = _run(isolated, request)
    assert second.success, second.error
    summary = second.destination_summary or {}
    assert summary.get("skipped_null_cursor_rows") == 1, summary
    assert "'seq'" in str(summary.get("null_cursor_note"))
    with sqlite3.connect(dst) as conn:
        assert [r[0] for r in conn.execute("SELECT id FROM t ORDER BY id")] == [1, 2, 3, 5]


def test_strict_run_refuses_a_null_cursor_gap(tmp_path, isolated):
    src, _dst, request = _setup(tmp_path, "strict", False)
    assert _run(isolated, request).success
    with sqlite3.connect(src) as conn:
        conn.execute("INSERT INTO t VALUES (4, NULL, 'null4')")
    second = _run(isolated, request)
    assert not second.success
    assert "NULL value in the incremental cursor column 'seq'" in str(second.error)
