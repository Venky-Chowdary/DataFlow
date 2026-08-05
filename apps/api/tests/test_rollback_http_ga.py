"""GA Module F — HTTP rollback execute (DISCARD_STAGING only)."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from services.migration_rollback import plan_rollback
from src.routers.transfer_router import RollbackExecuteBody, execute_job_rollback


def test_rollback_execute_discard_staging():
    plan = plan_rollback(
        job_id="j1",
        sync_mode="append",
        destination_table="t",
        staging_table="t_df_staging",
    ).to_dict()
    job = {
        "_id": "j1",
        "status": "completed",
        "destination_summary": {"rollback_plan": plan},
        "request": {
            "destination": {
                "kind": "database",
                "format": "postgresql",
                "table": "t",
                "host": "localhost",
                "database": "db",
            }
        },
    }
    mongo = MagicMock()
    mongo.get_job.return_value = job
    mongo.update_job_status.return_value = None
    req = MagicMock()

    with patch(
        "src.services.mongodb_service.get_mongodb_service", return_value=mongo
    ), patch(
        "src.routers.transfer_router._can_access_job", return_value=True
    ), patch(
        "services.migration_rollback.discard_staging_table", return_value=True
    ), patch(
        "services.audit_log.append_audit_event"
    ), patch(
        "services.audit_log.actor_from_request", return_value="ops@example.com"
    ):
        result = asyncio.run(
            execute_job_rollback(
                "j1",
                RollbackExecuteBody(
                    approved_by="ops@example.com",
                    reason="discard staging after blocked promote",
                ),
                req,
            )
        )
    assert result["ok"] is True
    assert result["strategy"] == "DISCARD_STAGING"
    assert result["population_undo_claimed"] is False


def test_rollback_execute_refuses_document_only():
    plan = plan_rollback(
        job_id="j2",
        sync_mode="append",
        destination_table="t",
        staging_table=None,
    ).to_dict()
    job = {
        "_id": "j2",
        "status": "completed",
        "destination_summary": {"rollback_plan": plan},
        "request": {"destination": {"kind": "database", "format": "postgresql"}},
    }
    mongo = MagicMock()
    mongo.get_job.return_value = job
    req = MagicMock()

    with patch(
        "src.services.mongodb_service.get_mongodb_service", return_value=mongo
    ), patch(
        "src.routers.transfer_router._can_access_job", return_value=True
    ), patch(
        "services.audit_log.actor_from_request", return_value="ops@example.com"
    ):
        with pytest.raises(HTTPException) as ei:
            asyncio.run(
                execute_job_rollback(
                    "j2",
                    RollbackExecuteBody(
                        approved_by="ops@example.com", reason="try undo"
                    ),
                    req,
                )
            )
    assert ei.value.status_code == 409
