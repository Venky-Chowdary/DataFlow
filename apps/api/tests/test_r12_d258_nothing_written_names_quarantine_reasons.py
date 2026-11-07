"""D258: a run that held out every row named data-quality warnings, not why.

The "nothing was written" failure listed the run's warnings ("Column 'name'
has a sensitive name but no PII transform") whenever any existed, so the
operator never saw that every row was quarantined, or the reason. The
"column '" wording then classified it as a missing destination column.
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


def _run_all_rows_held_out(tmp_path: Path, fake_mongo):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, qty TEXT)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?, ?)", [(i, f"person {i}", f"oops{i}") for i in range(1, 6)]
        )
    conn.close()
    sqlite3.connect(str(dst)).close()
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t_new"),
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "name", "target": "name", "target_type": "TEXT"},
            {"source": "qty", "target": "qty", "target_type": "INTEGER", "transform": "integer"},
        ],
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="balanced",
    )
    job_id = "d258m" + uuid.uuid4().hex[:16]
    fake_mongo.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def test_failure_names_the_quarantine_not_the_warnings(tmp_path: Path, fake_mongo) -> None:
    result = _run_all_rows_held_out(tmp_path, fake_mongo)

    assert result.success is False
    error = str(result.error or "")
    assert "were quarantined, so nothing was written" in error, error
    assert "Invalid integer" in error, error
    assert "sensitive name" not in error, error
