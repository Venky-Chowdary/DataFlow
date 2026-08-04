"""Rank 56: execute_tracked with skip_preflight=False (gates ON) for a clean path."""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import src.transfer.engine as engine_mod
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


class _FakeMongo:
    def __init__(self):
        self.jobs: dict = {}

    def get_job(self, job_id: str):
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True


@pytest.fixture(autouse=True)
def _patch_mongo(monkeypatch):
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _FakeMongo())


def test_clean_csv_to_sqlite_with_preflight_on():
    """Clean integer/text rows — G1–G9 must pass without skip_preflight."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "clean.db"
        job_id = uuid.uuid4().hex
        csv = b"id,name\n1,alpha\n2,beta\n"
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="clean.csv",
            source_content=csv,
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(db_path),
                table="people",
            ),
            sync_mode="full_refresh_overwrite",
            stream_contracts=[{
                "name": "people",
                "sync_mode": "full_refresh_overwrite",
                "selected": True,
            }],
            skip_preflight=False,
            # create-new path — approve mappings via user_override when engine stamps review
            mappings=[
                {"source": "id", "target": "id", "confidence": 1.0, "user_override": True, "transform": "none"},
                {"source": "name", "target": "name", "confidence": 1.0, "user_override": True, "transform": "none"},
            ],
        )
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success is True, result.error
        assert result.records_transferred == 2
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT id, name FROM people ORDER BY id").fetchall()
        conn.close()
        assert rows == [(1, "alpha"), (2, "beta")] or rows == [("1", "alpha"), ("2", "beta")]
