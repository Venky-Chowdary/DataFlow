"""Buffered resume must reconcile against the whole table, not the resumed tail.

The buffered path slices the in-memory source past the rows a killed attempt
already committed, and then handed that slice to Gate-8. The destination
read-back is always full-table, so a correct resume reported a source/target
row and checksum mismatch — a healthy migration marked corrupt.

Priority ordering forces the buffered (non-streaming) path, which is the one
that slices.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.checkpoint_service import Checkpoint  # noqa: E402
import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

ROWS = 6
COMMITTED = 3


def _sqlite(path: Path, rows: list[tuple[int, int]]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            conn.executemany("INSERT INTO t VALUES (?, ?)", rows)
    finally:
        conn.close()


def test_buffered_resume_reconciles_the_whole_population(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)

    src = tmp_path / "src.sqlite"
    dst = tmp_path / "dst.sqlite"
    all_rows = [(i, i * 10) for i in range(1, ROWS + 1)]
    _sqlite(src, all_rows)
    _sqlite(dst, all_rows[:COMMITTED])

    job_id = "bufres" + uuid.uuid4().hex[:18]
    fake.update_job_status(
        job_id,
        "running",
        checkpoint=Checkpoint(
            job_id=job_id,
            chunk_index=1,
            offset=COMMITTED,
            rows_processed=COMMITTED,
            cursor_column="id",
            cursor_value=COMMITTED,
        ).to_dict(),
        transfer_request={},
    )

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="t"
        ),
        sync_mode="upsert",
        # Staging writes are excluded from the streaming reader, so the engine
        # takes the buffered path — the one that slices on resume.
        write_via_staging=True,
        stream_contracts=[
            {
                "name": "t",
                "primary_key": "id",
                "sync_mode": "upsert",
                "selected": True,
            }
        ],
        skip_preflight=True,
        validation_mode="strict",
    )

    result = UniversalTransferEngine().execute_tracked(request, job_id, resume=True)
    assert result.success, result.error

    recon = result.reconciliation or {}
    assert recon.get("passed") is True, recon
    # The ledger and both digests must span the population, not the resumed tail.
    assert recon.get("source_rows") == ROWS, recon
    assert recon.get("target_rows") == ROWS, recon
    assert recon.get("checksum_match") is True, recon

    conn = sqlite3.connect(str(dst))
    try:
        assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == ROWS
        assert conn.execute("SELECT count(DISTINCT id) FROM t").fetchone()[0] == ROWS
    finally:
        conn.close()
