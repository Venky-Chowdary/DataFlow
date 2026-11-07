"""QA MX3-19 — get_job must carry the approval/governance audit and zoned timestamps.

QA: get_job on a contract-violation job confirmed with reason "QA_E2E_RT retest"
had no approver, confirm reason, acknowledgement or contract fields, and
``created_at="2026-10-10 11:27:40.535000"`` had no offset.
"""

from __future__ import annotations

import socket
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))


def _zoned(value: str) -> bool:
    return bool(value) and datetime.fromisoformat(value.replace("Z", "+00:00")).tzinfo is not None


def test_get_job_exposes_governance_and_zoned_timestamps():
    from services.mongodb_service import get_mongodb_service
    from src.ai.copilot.tools import DataPilotTools
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    signed = _sign_required_risk_contracts(
        [{"source": "flt", "target": "flt", "source_type": "TEXT",
          "target_type": "DOUBLE PRECISION", "confidence": 0.99}],
        {"approved_by": "risk-owner", "reason": "accepted loss",
         "execution_policy": "QUARANTINE_ROW"},
        table="t",
    )
    mongo = get_mongodb_service()
    job_id = f"mx319-{uuid.uuid4().hex[:8]}"
    mongo.create_transfer_job({
        "_id": job_id,
        "status": "completed",
        "transfer_request": {
            "mappings": signed,
            "compliance_acknowledged": True,
            "acknowledgment_actor": "dpo@example.com",
            "acknowledgment_reason": "synthetic data",
            "contract_id": "c-1",
            "require_signed_contract": True,
        },
        "confirmation": {"approved_by": "qa-approver", "reason": "QA_E2E_RT retest",
                         "ack_id": "ack-1", "confirmed_at": "2026-10-10T11:27:40Z"},
    })
    # Mongo returns naive UTC datetimes — mirror that.
    mongo._jobs[job_id]["created_at"] = datetime(2026, 10, 10, 11, 27, 40, 535000)
    mongo._jobs[job_id]["completed_at"] = datetime(2026, 10, 10, 11, 28, 1)

    out = DataPilotTools().execute("get_job", {"job_id": job_id}).output
    assert out["created_at"] == "2026-10-10T11:27:40.535000Z", out["created_at"]
    assert out["completed_at"] == "2026-10-10T11:28:01Z", out["completed_at"]
    gov = out["governance"]
    assert gov["approval"]["approved_by"] == "qa-approver"
    assert gov["approval"]["reason"] == "QA_E2E_RT retest"
    assert gov["acknowledgments"] == {
        "compliance": True, "schema_drift": False, "fk_risk": False,
        "actor": "dpo@example.com", "reason": "synthetic data",
    }
    assert gov["contract_id"] == "c-1" and gov["require_signed_contract"] is True
    [contract] = gov["risk_contracts"]
    assert contract["approved_by"] == "risk-owner"
    assert contract["reason"] == "accepted loss"
    assert contract["execution_policy"] == "QUARANTINE_ROW"
    assert contract["verified"] is True

    # list_jobs rows go through the same summary (a naive Mongo document here).
    from src.ai.copilot.job_reads import summarize_listed_job

    row = summarize_listed_job(dict(mongo._jobs[job_id]))
    assert row["created_at"] == "2026-10-10T11:27:40.535000Z", row
    assert row["completed_at"] == "2026-10-10T11:28:01Z", row


def _pg_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1.5):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _pg_reachable(), reason="PostgreSQL 5432 not reachable")
def test_live_confirm_reason_lands_on_the_job():
    import asyncio

    import psycopg2
    from fastapi import BackgroundTasks

    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import DataPilotTools
    from src.routers.copilot_router import ConfirmActionRequest, copilot_confirm

    sfx = uuid.uuid4().hex[:6]
    src, dst = f"mx319s_{sfx}", f"mx319d_{sfx}"
    conn = psycopg2.connect(host="localhost", port=5432, dbname="dataflow",
                            user="dataflow", password="dataflow")
    conn.autocommit = True
    saved = create_connector({
        "name": f"MX319{sfx}", "type": "postgresql", "host": "localhost", "port": 5432,
        "database": "dataflow", "username": "dataflow", "password": "dataflow",
        "schema": "public",
    })
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, label TEXT)")
            cur.executemany(f"INSERT INTO {src} VALUES (%s,%s)", [(1, "a"), (2, "b")])
        staged = DataPilotTools().execute("start_transfer", {
            "source_connector_name": saved.name, "source_table": src,
            "dest_connector_name": saved.name, "dest_table": dst,
            "sync_mode": "full_refresh_overwrite",
        })
        assert staged.success, staged.error

        class _State:
            pass

        class _Req:
            state = _State()
            headers: dict[str, str] = {}

        confirmed = asyncio.run(copilot_confirm(
            ConfirmActionRequest(ack_id=staged.output["ack_id"], actor="qa-approver",
                                 reason="QA_E2E_RT retest"),
            _Req(), BackgroundTasks(),
        ))
        assert confirmed["ok"] is True, confirmed
        deadline = time.time() + 120
        job: dict = {}
        while time.time() < deadline:
            job = dict(DataPilotTools().execute("get_job", {"job_id": confirmed["job_id"]}).output or {})
            if str(job.get("status") or "") not in {"", "pending", "queued", "running", "starting"}:
                break
            time.sleep(1.0)
        assert str(job.get("status") or "").startswith("completed"), job
        approval = (job.get("governance") or {}).get("approval") or {}
        assert approval.get("approved_by") == "qa-approver", job.get("governance")
        assert approval.get("reason") == "QA_E2E_RT retest"
        assert approval.get("ack_id") == staged.output["ack_id"]
        assert _zoned(approval.get("confirmed_at") or "")
        assert _zoned(job["created_at"])
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
            cur.execute(f"DROP TABLE IF EXISTS {dst}")
        conn.close()
        delete_connector(saved.id)
