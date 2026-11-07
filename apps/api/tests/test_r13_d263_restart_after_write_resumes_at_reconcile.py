"""D263 — a deploy restart after the write committed re-ran the append from zero.

Prod: PG ``r8_q`` (10 rows, no PK, duplicate id 3, bad qty on ids 5 and 7) ->
existing MySQL ``q8_q_my``, ``full_refresh_append``, signed CAST_AND_CONTINUE.
Job 6ae98b47… wrote 8 rows, quarantined 2 and was reconciling when the deploy
restarted it. The resumed run repeated preflight, found its own 8 committed
keys at rest ("Destination already stores these keys…"), and ended FAILED
with 0 processed, so its quarantine could never be replayed.

Every batch was committed, so the resumed run writes nothing and only
reconciles: it keeps the write pass's sample and findings and ends
``completed_with_quarantine`` with the destination's 8 rows.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_IDS = [1, 2, 3, 3, 4, 5, 6, 7, 8, 9]
_BAD = {5: "oops", 7: ""}


def _qty_contract() -> dict:
    from services.migration_risk_contract import create_migration_risk_contract

    return create_migration_risk_contract(
        column="qty",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="qa@dataflow.app",
        reason="D263: quarantine the bad qty rows, load the rest",
        execution_policy="CAST_AND_CONTINUE",
    ).to_dict()


def _request(src: Path, dst: Path) -> TransferRequest:
    ident = {"confidence": 0.99}
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="r8_q"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="q8_q"),
        sync_mode="full_refresh_append",
        validation_mode="strict",
        mappings=[
            {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER", **ident},
            {"source": "name", "target": "name", "source_type": "TEXT", "target_type": "TEXT", **ident},
            {
                "source": "qty", "target": "qty", "source_type": "TEXT", "target_type": "INTEGER",
                "risk_contract": _qty_contract(), "risk_acknowledged": True, **ident,
            },
        ],
    )


def _dest_rows(dst: Path) -> list[tuple]:
    with sqlite3.connect(str(dst)) as conn:
        return sorted(conn.execute("SELECT id, qty FROM q8_q").fetchall())


def test_restart_after_committed_write_reconciles_instead_of_failing(tmp_path: Path) -> None:
    from services.checkpoint_service import CheckpointService
    from services.mongodb_service import get_mongodb_service

    src, dst = tmp_path / "s.db", tmp_path / "d.db"
    with sqlite3.connect(str(src)) as conn:
        conn.execute("CREATE TABLE r8_q (id INTEGER, name TEXT, qty TEXT)")
        conn.executemany(
            "INSERT INTO r8_q VALUES (?, ?, ?)",
            [(i, f"n{i}", _BAD.get(i, str(i * 3))) for i in _IDS],
        )
    with sqlite3.connect(str(dst)) as conn:
        conn.execute("CREATE TABLE q8_q (id INTEGER, name TEXT, qty INTEGER)")

    request = _request(src, dst)
    job_id = "d263" + uuid.uuid4().hex[:12]
    store = get_mongodb_service()
    store.update_job_status(job_id, "pending", transfer_request={})
    UniversalTransferEngine().execute_tracked(request, job_id)
    first = store.get_job(job_id) or {}
    assert first.get("status") == "completed_with_quarantine", first.get("error")
    committed = _dest_rows(dst)
    assert len(committed) == 8

    # The worker stopped mid-reconcile: the saved checkpoint is what the
    # reclaim sees, and the job is running again.
    checkpoint = CheckpointService(store).load(job_id)
    assert checkpoint is not None and int(checkpoint.rows_processed or 0) == 8
    store.update_job_status(
        job_id, "running", phase="reconcile", message="Reconciling", allow_terminal_exit=True
    )
    UniversalTransferEngine().execute_tracked(_request(src, dst), job_id, resume=True)

    job = store.get_job(job_id) or {}
    assert job.get("status") == "completed_with_quarantine", job.get("error") or job.get("message")
    assert int(job.get("records_processed") or 0) == 8
    assert int(job.get("rejected_rows") or 0) == 2
    assert _dest_rows(dst) == committed
