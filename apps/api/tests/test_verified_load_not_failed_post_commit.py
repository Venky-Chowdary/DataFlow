"""QA MX2-15 — a job marked *failed* after every row was written and verified.

A 120,000-row PostgreSQL → SQL Server run copied every row with matching
checksums, and the job read failed. Bookkeeping after Gate-8 (terminal status
write under a slow control plane, lineage emit, contract finalize) shared the
``try`` whose handler fails the job and — for rename-aside overwrites — restores
the previous destination table over the verified rows.

These runs are real SQLite → SQLite transfers through the engine.
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

ROWS = 250


def _sqlite(path: Path, rows: list[tuple[int, int]]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            conn.executemany("INSERT INTO t VALUES (?, ?)", rows)
    finally:
        conn.close()


def _count(path: Path) -> int:
    conn = sqlite3.connect(str(path))
    try:
        return int(conn.execute("SELECT count(*) FROM t").fetchone()[0])
    finally:
        conn.close()


def _request(src: Path, dst: Path, *, buffered: bool) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="upsert",
        write_via_staging=buffered,
        stream_contracts=[
            {"name": "t", "primary_key": "id", "sync_mode": "upsert", "selected": True}
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


@pytest.mark.parametrize("buffered", [False, True], ids=["streaming", "buffered"])
def test_lineage_failure_after_gate8_does_not_fail_a_verified_load(
    tmp_path: Path, monkeypatch, buffered: bool
):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _sqlite(src, [(i, i * 10) for i in range(1, ROWS + 1)])

    def _lineage_timeout(*_a, **_k):
        raise TimeoutError("lineage endpoint timed out after 600s")

    monkeypatch.setattr(engine_mod.lineage, "emit_run_completed", _lineage_timeout)

    original = getattr(engine_mod, "finalize_contract")

    def _finalize(contract_id, success=True, **kw):
        if success:
            raise ConnectionError("contract store unreachable")
        return original(contract_id, success=success, **kw)

    monkeypatch.setattr(engine_mod, "finalize_contract", _finalize)

    job_id = "mx215" + uuid.uuid4().hex[:18]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(
        _request(src, dst, buffered=buffered), job_id
    )

    assert _count(dst) == ROWS
    assert result.success, result.error
    job = fake.get_job(job_id) or {}
    assert job.get("status") in {"completed", "completed_with_warnings"}, job.get("status")
    warnings = (result.destination_summary or {}).get("post_commit_warnings") or []
    assert warnings and "lineage endpoint timed out" in warnings[0]["error"]
    assert job.get("post_commit_warnings")


def test_contract_finalize_failure_after_gate8_keeps_the_load(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _sqlite(src, [(i, i * 10) for i in range(1, ROWS + 1)])
    original = engine_mod.finalize_contract

    def _finalize(contract_id, success=True, **kw):
        if success:
            raise ConnectionError("contract store unreachable")
        return original(contract_id, success=success, **kw)

    monkeypatch.setattr(engine_mod, "finalize_contract", _finalize)
    job_id = "mx215c" + uuid.uuid4().hex[:18]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(_request(src, dst, buffered=False), job_id)
    assert result.success, result.error
    assert _count(dst) == ROWS
    assert (fake.get_job(job_id) or {}).get("status") in {"completed", "completed_with_warnings"}


def test_failure_before_gate8_still_fails_the_job(tmp_path: Path, monkeypatch):
    """The commit point is Gate-8 — earlier failures must stay failures."""
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _sqlite(src, [(i, i) for i in range(1, 20)])

    def _recon_boom(**_k):
        raise RuntimeError("reconcile read-back failed")

    monkeypatch.setattr(engine_mod, "run_reconciliation", _recon_boom)
    job_id = "mx215b" + uuid.uuid4().hex[:18]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(_request(src, dst, buffered=False), job_id)
    assert not result.success
    assert (fake.get_job(job_id) or {}).get("status") == "failed"
