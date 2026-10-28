"""The Gate-8 proof's scratch databases must not outlive the run.

``df_pinmap_*.db`` / ``df_scope_*.db`` grow to hundreds of MB on large runs.
They were mkstemp()ed into the shared temp dir and removed only by ``__del__``,
so an exception, a cancel or a killed run left them behind until the disk
filled and unrelated runs failed with ENOSPC.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.reconcile_step as rs  # noqa: E402

_PREFIXES = ("df_", "dataflow_")


def _leftovers(root: Path) -> set[str]:
    return {p.name for p in root.iterdir() if p.name.startswith(_PREFIXES)}


@pytest.fixture()
def private_tmp(tmp_path: Path, monkeypatch):
    root = tmp_path / "tmp"
    root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(root))
    return root


@pytest.mark.parametrize("outcome", ["returns", "raises"])
def test_proof_stores_are_removed_however_the_proof_ends(private_tmp, monkeypatch, outcome):
    seen: dict = {}

    def fake_impl(**_kwargs):
        pin, scope = rs._PinStore(), rs._DiskKeySet()
        seen["paths"] = (pin.path, scope.path)
        assert all(os.path.exists(p) for p in seen["paths"])
        # Strong refs held: __del__ cannot be what cleans these up.
        seen["keep"] = (pin, scope)
        if outcome == "raises":
            raise RuntimeError("cancelled mid-proof")
        return {"status": "passed"}

    monkeypatch.setattr(rs, "_transform_applied_proof_impl", fake_impl)
    if outcome == "raises":
        with pytest.raises(RuntimeError):
            rs._transform_applied_proof()
    else:
        assert rs._transform_applied_proof() == {"status": "passed"}
    assert not any(os.path.exists(p) for p in seen["paths"])
    assert _leftovers(private_tmp) == set()


def test_engine_run_leaves_no_scratch_files(private_tmp, tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    store = MemoryMongoDBService()
    import services.mongodb_service as mongo_mod

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    table = f"t_{uuid.uuid4().hex[:6]}"
    conn = sqlite3.connect(src)
    with conn:
        conn.execute(f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, v TEXT)')
        conn.executemany(
            f'INSERT INTO "{table}" VALUES (?, ?)', [(i, f"v{i}") for i in range(1, 201)]
        )
    conn.close()

    before = _leftovers(private_tmp)
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table=table),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table=table),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    assert _leftovers(private_tmp) - before == set()


def test_csv_run_leaves_no_scratch_files(private_tmp, tmp_path: Path, monkeypatch):
    import services.mongodb_service as mongo_mod
    import src.transfer.engine as engine_mod
    from services.mongodb_service import MemoryMongoDBService
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)

    body = "id,v\n" + "\n".join(f"{i},v{i}" for i in range(1, 101)) + "\n"
    table = f"c_{uuid.uuid4().hex[:6]}"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(tmp_path / "d.db"), table=table
        ),
        source_filename="in.csv",
        source_content=body.encode(),
        sync_mode="full_refresh_overwrite",
    )
    before = _leftovers(private_tmp)
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    assert _leftovers(private_tmp) - before == set()
