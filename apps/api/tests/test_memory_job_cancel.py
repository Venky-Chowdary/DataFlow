"""Memory job store cancel — this host's DATAFLOW_JOB_STORE=memory path.

MongoDBService already had request_job_cancel. MemoryMongoDBService did not,
so POST /jobs/{id}/cancel raised AttributeError → 500. A late completed write
also overwrote cancelled because the memory path had no terminal fence.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_memory_store_cancel_methods_and_completed_fence() -> None:
    from services.mongodb_service import MemoryMongoDBService

    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job(
        {"_id": "mem-cancel-1", "status": "running", "workspace_id": ""}
    )
    assert mongo.request_job_cancel(job_id) is True
    assert mongo.is_cancel_requested(job_id) is True
    assert mongo.update_job_status(
        job_id, "cancelled", phase="cancelled", message="Transfer cancelled by user"
    )
    assert mongo.update_job_status(
        job_id, "completed", records_processed=800, message="Done"
    ) is False
    job = mongo.get_job(job_id)
    assert job is not None
    assert job["status"] == "cancelled"
    assert mongo.clear_job_cancel(job_id) is True
    assert mongo.update_job_status(
        job_id, "pending", message="Resume", allow_terminal_exit=True
    )
    assert mongo.get_job(job_id)["status"] == "pending"


def test_memory_cancel_requested_blocks_completed_before_status_flips() -> None:
    from services.mongodb_service import MemoryMongoDBService

    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job(
        {"_id": "mem-cancel-2", "status": "running", "workspace_id": ""}
    )
    assert mongo.request_job_cancel(job_id) is True
    assert mongo.update_job_status(job_id, "completed", records_processed=12) is False
    assert mongo.get_job(job_id)["status"] == "running"
    assert mongo.update_job_status(job_id, "cancelled") is True
    assert mongo.get_job(job_id)["status"] == "cancelled"


def test_cancel_api_does_not_500_on_memory_store(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from services.mongodb_service import MemoryMongoDBService
    from src.main import app

    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job(
        {
            "_id": "mem-cancel-api",
            "status": "running",
            "workspace_id": "",
            "progress_pct": 40,
        }
    )
    monkeypatch.setattr(
        "src.routers.connectors_router.get_mongodb_service", lambda: mongo
    )
    monkeypatch.setattr(
        "src.middleware.auth_middleware._auth_service.auth_required",
        lambda: False,
    )
    with TestClient(app) as client:
        res = client.post(f"/api/v1/connectors/jobs/{job_id}/cancel")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["success"] is True
    assert body["status"] == "cancelled"
    job = mongo.get_job(job_id)
    assert job is not None
    assert job["status"] == "cancelled"
    assert job.get("cancel_requested") is True
    assert mongo.update_job_status(job_id, "completed") is False
    assert mongo.get_job(job_id)["status"] == "cancelled"
