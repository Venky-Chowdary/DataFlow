"""D176: Resume of a CDC job clears the stale error and fails fast if the stream cannot attach.

A job that failed on MySQL error 1227 kept that error through ``resume_job``
and then sat "running" at 5% with a frozen ``updated_at``. Resume now asks the
same log-capture check Validate uses, under a bounded deadline, before it
re-queues anything; on success the stale failure is cleared.
"""

from __future__ import annotations

import asyncio
import importlib
import time
from types import SimpleNamespace

import pytest
from fastapi import BackgroundTasks, HTTPException

import services.checkpoint_service as checkpoint_service
import services.cdc_log_capture_probe as probe_mod
from services.cdc_capability import CAUSE_PRIVILEGE
from services.mongodb_service import MemoryMongoDBService
from src.transfer.models import EndpointConfig, TransferRequest, transfer_request_to_dict

# The package re-exports the APIRouter under the module's name; import the module.
router = importlib.import_module("src.routers.connectors_router")

STALE = "(1227, 'Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s)')"


def _job(mongo: MemoryMongoDBService) -> str:
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql", host="127.0.0.1", port=3306,
            database="d", username="u", password="generated-not-used", table="t",
        ),
        destination=EndpointConfig(kind="database", format="sqlite", database=":memory:", table="t"),
        sync_mode="cdc_incremental",
        stream_contracts=[
            {"name": "t", "primary_key": ["id"], "sync_mode": "cdc_incremental", "selected": True}
        ],
    )
    job_id = "d176resume0001"
    mongo.create_transfer_job({"_id": job_id, "status": "failed"})
    mongo.update_job_status(
        job_id, "failed", error=STALE, message=STALE, phase="failed",
        transfer_request=transfer_request_to_dict(request), checkpoint={"rows": 1},
    )
    return job_id


@pytest.fixture()
def resume_env(monkeypatch):
    mongo = MemoryMongoDBService()
    monkeypatch.setattr(router, "get_mongodb_service", lambda: mongo)
    monkeypatch.setattr(router, "_can_access_job", lambda *_a: True)
    monkeypatch.setattr(router, "_can_mutate_job", lambda *_a: True)
    monkeypatch.setattr(
        checkpoint_service, "evaluate_resume_safety",
        lambda *_a, **_k: {"ok": True, "reasons": [], "warnings": [], "age_hours": 0.0},
    )
    return mongo


def _resume(job_id: str):
    tasks = BackgroundTasks()
    out = asyncio.run(router.resume_transfer_job(job_id, tasks, SimpleNamespace()))
    return out, tasks


def test_resume_refuses_when_the_binlog_cannot_be_attached(resume_env, monkeypatch) -> None:
    job_id = _job(resume_env)
    monkeypatch.setattr(
        probe_mod, "probe_log_capture",
        lambda *a, **k: probe_mod.LogCaptureProbe(
            "mysql", False, "binlog", CAUSE_PRIVILEGE,
            "the MySQL user needs REPLICATION SLAVE and REPLICATION CLIENT",
        ),
    )
    with pytest.raises(HTTPException) as caught:
        _resume(job_id)
    assert caught.value.status_code == 409
    assert "REPLICATION SLAVE and REPLICATION CLIENT" in str(caught.value.detail)
    job = resume_env.get_job(job_id)
    assert job["status"] == "failed", "a refused resume must leave the job failed, not queued"


def test_resume_gives_up_at_the_deadline_instead_of_hanging(resume_env, monkeypatch) -> None:
    job_id = _job(resume_env)
    monkeypatch.setattr(router, "RESUME_ATTACH_DEADLINE_SECONDS", 0.3)
    monkeypatch.setattr(probe_mod, "probe_log_capture", lambda *a, **k: time.sleep(5))
    elapsed: list[float] = []

    async def attempt() -> None:
        started = time.monotonic()
        try:
            await router.resume_transfer_job(job_id, BackgroundTasks(), SimpleNamespace())
        finally:
            # asyncio.run() joins the abandoned probe thread on exit; the server's loop does not.
            elapsed.append(time.monotonic() - started)

    with pytest.raises(HTTPException) as caught:
        asyncio.run(attempt())
    assert elapsed[0] < 2
    assert caught.value.status_code == 409
    assert "did not attach" in str(caught.value.detail)
    assert resume_env.get_job(job_id)["status"] == "failed"


def test_resume_clears_the_stale_error_when_the_stream_attaches(resume_env, monkeypatch) -> None:
    job_id = _job(resume_env)
    monkeypatch.setattr(
        probe_mod, "probe_log_capture",
        lambda *a, **k: probe_mod.LogCaptureProbe("mysql", True, "binlog"),
    )
    out, tasks = _resume(job_id)
    assert out["success"] is True and len(tasks.tasks) == 1
    job = resume_env.get_job(job_id)
    assert job["status"] == "pending"
    assert not job.get("error"), job.get("error")
    assert "1227" not in str(job.get("message") or "")


def test_live_resume_by_a_user_without_replication_grants_is_refused(resume_env) -> None:
    """Real MySQL user with no REPLICATION grants (see test_d176_mysql_cdc_privilege_preflight)."""
    import os

    user = os.environ.get("DATAFLOW_TEST_MYSQL_NOREPL_USER", "")
    password = os.environ.get("DATAFLOW_TEST_MYSQL_NOREPL_PASSWORD", "")
    database = os.environ.get("DATAFLOW_TEST_MYSQL_NOREPL_DB", "")
    if not (user and password and database):
        pytest.skip("set DATAFLOW_TEST_MYSQL_NOREPL_USER/_PASSWORD/_DB")
    job_id = _job(resume_env)
    payload = resume_env.get_job(job_id)["transfer_request"]
    payload["source"].update(
        host=os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306")),
        database=database, username=user, password=password,
    )
    resume_env.update_job_fields(job_id, {"transfer_request": payload})
    with pytest.raises(HTTPException) as caught:
        _resume(job_id)
    assert caught.value.status_code == 409
    assert "REPLICATION SLAVE and REPLICATION CLIENT" in str(caught.value.detail)
    assert resume_env.get_job(job_id)["status"] == "failed"
