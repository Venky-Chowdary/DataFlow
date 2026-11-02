"""D133 — replay_quarantine on a parent that never committed its good rows.

A failed parent is rolled back (its table dropped or emptied), yet replay was
allowed: it built a table holding only the quarantined rows, reported it
verified and closed the ledger. Replay now refuses unless the parent
completed, and says to re-run the parent first — through the REST route and
the Pilot tool alike.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient


def _quarantining_parent(tmp_path: Path) -> tuple[str, Path]:
    """A parent job record that quarantined one row and landed two."""
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dest = tmp_path / "d133.db"
    with sqlite3.connect(dest) as db:
        db.execute('CREATE TABLE "users" (id TEXT, age INTEGER)')
        db.executemany('INSERT INTO "users" VALUES (?, ?)', [("1", 30), ("3", 40)])
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database", format="sqlite", table="users",
            connection_string=f"sqlite:///{dest}", database=str(dest),
        ),
        source_filename="users.csv",
        source_content=b"id,age\n1,30\n2,not-a-number\n3,40\n",
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"},
        ],
        column_types={"id": "string", "age": "string"},
    )
    job_id = UniversalTransferEngine()._create_pending_job(request)
    get_mongodb_service().update_job_status(
        job_id,
        "completed_with_quarantine",
        rejected_rows=1,
        rejected_details=[{
            "row": {"id": "2", "age": "not-a-number"},
            "column": "age",
            "value": "not-a-number",
            "values": {"id": "2", "age": "not-a-number"},
            "reason": "cannot cast to integer",
            "source_row_index": 2,
        }],
    )
    return job_id, dest


def _fail_and_roll_back(job_id: str, dest: Path) -> None:
    """The parent's state after a failed post-write proof undid its batch."""
    from services.mongodb_service import get_mongodb_service

    with sqlite3.connect(dest) as db:
        db.execute('DROP TABLE "users"')
    get_mongodb_service().update_job_status(
        job_id, "failed", message="Gate-8 failed; batch removed", allow_terminal_exit=True
    )


def test_replay_refused_on_failed_parent(tmp_path: Path):
    from src.main import app

    job_id, dest = _quarantining_parent(tmp_path)
    _fail_and_roll_back(job_id, dest)

    resp = TestClient(app).post(
        f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={"rows": []}
    )
    assert resp.status_code == 409, resp.text
    detail = str(resp.json()["detail"])
    assert "failed" in detail and "Re-run the parent job first" in detail
    with sqlite3.connect(dest) as db:
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "users" not in tables, "replay must not build a partial table"


def test_pilot_replay_staging_refuses_failed_parent(tmp_path: Path):
    from src.ai.copilot import lifecycle_tools

    job_id, dest = _quarantining_parent(tmp_path)
    _fail_and_roll_back(job_id, dest)

    staged = lifecycle_tools._job_tool("replay_quarantine", job_id, "")
    assert staged.success is False
    assert "Re-run the parent job first" in str(staged.error)


def test_replay_parent_guard_allows_completed_statuses():
    from services.quarantine_dlq import replay_parent_not_committed

    for status in ("completed", "completed_with_quarantine", "completed_with_warnings"):
        assert replay_parent_not_committed({"status": status}) is None
    for status in ("failed", "cancelled", "running", ""):
        assert replay_parent_not_committed({"status": status, "job_id": "j1"})
