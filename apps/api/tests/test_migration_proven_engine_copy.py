"""A verified, fully digested engine-copy run must classify as migration_proven.

The COPY fast path first stamps a count-only reconciliation ("post-write proof
pending"). When the verify-time ladder digest re-reads both populations and
they match, that digest is promoted to full_checksum / verified — but the
pending stamp stayed, so ``classify_post_write_assurance`` read the job as
``pre_write_only`` and the proof reported migration_proven=false on a run whose
own verdict said verified.
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
from services.signed_proof_pack import classify_post_write_assurance  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def fake(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    store = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    # The verify-time population digest is what makes the run "verified"; it is
    # opt-in in the product (DATAFLOW_ENGINE_DIGEST), so the test says so.
    monkeypatch.setenv("DATAFLOW_ENGINE_DIGEST", "1")
    return store


@pytest.mark.parametrize("row_path", [False, True], ids=["engine_copy", "row_path"])
def test_verified_full_refresh_is_migration_proven(tmp_path: Path, fake, row_path):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    with sqlite3.connect(src) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"v{i}") for i in range(1, 6)])
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        source_filter={"column": "id", "op": "gt", "value": 0} if row_path else {},
    )
    job_id = uuid.uuid4().hex[:24]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error

    recon = fake.get_job(job_id)["reconciliation"]
    assert recon["verification_verdict"] == "verified"
    assert recon["post_write_pending"] is False
    claim = classify_post_write_assurance(recon)
    assert claim["claim_level"] == "full_checksum", claim
    assert claim["migration_proven"] is True
