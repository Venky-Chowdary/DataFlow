"""End-to-end upsert for CSV → SQLite using the non-streaming engine path."""

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
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    fake_mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake_mongo)


def _csv_content(rows: list[dict]) -> bytes:
    cols = list(rows[0].keys())
    lines = [",".join(cols)]
    for r in rows:
        lines.append(",".join(str(r[c]) for c in cols))
    return "\n".join(lines).encode("utf-8")


def test_csv_to_sqlite_upsert_updates_existing_rows():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "upsert.db"
        job_id = uuid.uuid4().hex

        request = TransferRequest(
            source=EndpointConfig(
                kind="file",
                format="csv",
            ),
            source_filename="payments.csv",
            source_content=_csv_content([
                {"id": "1", "amount": "1000.00"},
                {"id": "2", "amount": "2000.50"},
            ]),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(db_path),
                table="payments",
            ),
            sync_mode="upsert",
            stream_contracts=[{
                "name": "payments",
                "sync_mode": "upsert",
                "primary_key": "id",
                "selected": True,
            }],
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result1 = engine.execute_tracked(request, job_id)
        assert result1.success is True, result1.error
        assert result1.records_transferred == 2

        # Run a second transfer with an updated row and a new row.
        request.source_content = _csv_content([
            {"id": "1", "amount": "1111.00"},
            {"id": "3", "amount": "3000.00"},
        ])
        result2 = engine.execute_tracked(request, job_id)
        assert result2.success is True, result2.error
        assert result2.records_transferred == 2

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT id, amount FROM payments ORDER BY id").fetchall()
        conn.close()
        assert rows == [(1, "1111.00"), (2, "2000.50"), (3, "3000.00")]
