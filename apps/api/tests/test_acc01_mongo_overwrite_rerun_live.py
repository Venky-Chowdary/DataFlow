"""QA ACC-01 — re-running an overwrite into MongoDB set aside every row.

Run 1 creates the collection. Run 2 finds it existing, binds to the types the
live documents report, and must land the same rows again — zero quarantine,
Gate-8 consistent with what actually landed. Live against local MongoDB.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")


def _mongo_up() -> bool:
    try:
        socket.create_connection(("localhost", 27017), timeout=1).close()
        import pymongo

        pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).server_info()
        return True
    except Exception:
        return False


live = pytest.mark.skipif(not _mongo_up(), reason="no MongoDB on :27017")

ROWS = 40


def _source(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount NUMERIC(10,2), "
            "placed_at TIMESTAMP, placed_tz TEXT, note TEXT)"
        )
        conn.executemany(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
            [
                (
                    i,
                    f"{i}.50",
                    f"2024-03-{(i % 28) + 1:02d} 10:{i % 60:02d}:00",
                    f"2024-03-{(i % 28) + 1:02d}T10:{i % 60:02d}:00+02:00",
                    f"n{i}",
                )
                for i in range(1, ROWS + 1)
            ],
        )
    conn.close()


#: What Validate stamps after the operator accepts UTC-normalize on the one
#: wall-clock column (BSON date is an instant). Run 2 re-sends the same plan.
ACKED_MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0},
    {"source": "amount", "target": "amount", "confidence": 1.0},
    {"source": "placed_at", "target": "placed_at", "confidence": 1.0,
     "risk_acknowledged": True},
    {"source": "placed_tz", "target": "placed_tz", "confidence": 1.0},
    {"source": "note", "target": "note", "confidence": 1.0},
]


def _acked_mappings() -> list[dict]:
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="placed_at",
        source_type="TIMESTAMP",
        destination_type="date",
        approved_by="qa-operator@example.com",
        reason="Source wall clock is UTC; store as BSON date (UTC).",
        execution_policy="CAST_AND_CONTINUE",
        expected_precision_loss=False,
    ).to_dict()
    out = [dict(m) for m in ACKED_MAPPINGS]
    for m in out:
        if m["source"] == "placed_at":
            m["risk_contract"] = contract
    return out


def _request(src: Path, db: str, coll: str) -> TransferRequest:
    return TransferRequest(
        mappings=_acked_mappings(),
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="orders"),
        destination=EndpointConfig(
            kind="database",
            format="mongodb",
            connection_string=MONGO_URI,
            database=db,
            collection=coll,
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="balanced",
    )


@live
def test_acc01_second_overwrite_into_mongo_lands_every_row(tmp_path: Path, monkeypatch):
    import pymongo

    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Jobs(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            return self.update_job_status(job_id, (self.get_job(job_id) or {}).get("status", "running"), **fields)

    fake = _Jobs()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    src = tmp_path / "src.sqlite"
    _source(src)
    db, coll = "df_acc01_test", f"orders_{uuid.uuid4().hex[:8]}"
    client = pymongo.MongoClient(MONGO_URI)
    try:
        outcomes = []
        for run in (1, 2):
            job_id = f"acc01r{run}" + uuid.uuid4().hex[:12]
            fake.update_job_status(job_id, "pending", transfer_request={})
            result = UniversalTransferEngine().execute_tracked(_request(src, db, coll), job_id)
            summary = result.destination_summary or {}
            outcomes.append(
                {
                    "run": run,
                    "success": result.success,
                    "error": result.error,
                    "rejected": int(summary.get("rejected_rows") or 0),
                    "landed": client[db][coll].count_documents({}),
                    "gate8": (result.reconciliation or {}).get("passed"),
                    "first_reject": (summary.get("rejected_details") or [None])[0],
                }
            )
        for o in outcomes:
            assert o["rejected"] == 0, outcomes
            assert o["landed"] == ROWS, outcomes
            assert o["success"], outcomes
            assert o["gate8"] is True, outcomes
    finally:
        client.drop_database(db)
