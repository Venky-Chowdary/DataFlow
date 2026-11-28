"""Found while reproducing D414: the read-back sample applied a trim the writer skipped.

The mapper can propose ``trim`` / ``trim_id`` from a column's name. The writer
applies such a trim only when the operator asked for it (``user_override`` /
``transform_override``; ``writer_common._convert_mapped_cell``), so ``' N1 '``
lands as ``' N1 '``. The keyed read-back sample re-applied the trim to the
source cell and failed a correct overwrite: "Read-back sample verification
failed: row 1 column name→name: source='N1' dest=' N1 '". The sample now
follows the writer's rule; an operator-requested trim is still applied and
verified.

Real engine (``execute_tracked``), SQLite → SQLite on the buffered route
(``priority_column``), which hands Gate-8 the in-memory rows it samples.
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
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(store, tmp_path: Path, name_mapping: dict) -> tuple:
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"  N{i} ") for i in range(1, 31)])
    conn.close()
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name", **name_mapping}],
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": "t", "primary_key": ["id"], "sync_mode": "full_refresh_overwrite",
                           "selected": True}],
        skip_preflight=True, validation_mode="strict", priority_column="id",
    )
    job_id = "t2c" + uuid.uuid4().hex[:16]
    store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    conn = sqlite3.connect(str(dst))
    try:
        rows = dict(conn.execute("SELECT id, name FROM t").fetchall())
    finally:
        conn.close()
    return result, rows, (store.get_job(job_id) or {}).get("reconciliation") or {}


def test_a_proposed_trim_the_writer_skipped_is_not_a_sample_mismatch(store, tmp_path):
    result, rows, recon = _run(store, tmp_path, {"transform": "trim"})
    assert result.success, result.error
    assert rows[1] == "  N1 ", "the writer keeps a name-triggered trim off"
    sample = recon.get("sample_compare") or {}
    assert sample.get("passed") is True and int(sample.get("rows_compared") or 0) > 0, sample


def test_an_operator_trim_is_applied_and_verified(store, tmp_path):
    result, rows, recon = _run(store, tmp_path, {"transform": "trim", "transform_override": True})
    assert result.success, result.error
    assert rows[1] == "N1"
    assert (recon.get("sample_compare") or {}).get("passed") is True
