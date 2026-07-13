"""Verify currency / locale values are not lost when writing to SQLite."""

from __future__ import annotations

import sqlite3
import sys
import tempfile
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


def test_currency_csv_to_sqlite_preserves_value():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        db_path = tmp_path / "currency.db"

        content = (
            'id,amount,note\n'
            '1,"$1,000.00",US payment\n'
            '2,"€2.000,50",EU payment\n'
            '3,"USD 1000000.89",Rich customer\n'
        ).encode("utf-8")
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(db_path),
                table="payments",
            ),
            source_content=content,
            source_filename="payments.csv",
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )
        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, "0" * 24)
        assert result.success is True, result.error
        assert result.records_transferred == 3

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT id, amount FROM payments ORDER BY id").fetchall()
        conn.close()
        assert rows[0] == (1, pytest.approx(1000.00))
        assert rows[1] == (2, pytest.approx(2000.50))
        assert rows[2] == (3, pytest.approx(1000000.89))
