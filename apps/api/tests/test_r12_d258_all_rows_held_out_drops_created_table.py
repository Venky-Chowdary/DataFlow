"""D258(a): a create-new run that wrote 0 rows left its new table behind.

Every row was held out (quarantined), so the row path raised "nothing was
written" after the batch loop — outside the handler that runs
``undo_failed_batch_if_dest_was_empty``. The table this run created survived,
and the next run bound to it as an existing table it never approved.
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
def fake_mongo(monkeypatch):
    from tests.test_currency_to_sqlite import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _tables(path: Path) -> set[str]:
    if not path.exists():
        return set()
    conn = sqlite3.connect(str(path))
    try:
        return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()


def test_run_that_holds_out_every_row_drops_the_table_it_created(
    tmp_path: Path, fake_mongo
) -> None:
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, qty TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"oops{i}") for i in range(1, 6)])
    conn.close()
    sqlite3.connect(str(dst)).close()

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t_new"),
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "qty", "target": "qty", "target_type": "INTEGER", "transform": "integer"},
        ],
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="balanced",
    )
    job_id = "d258a" + uuid.uuid4().hex[:16]
    fake_mongo.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)

    assert result.success is False
    assert "t_new" not in _tables(dst), result.error
