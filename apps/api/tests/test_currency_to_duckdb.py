"""Verify currency / locale-formatted values are not corrupted during transfer."""

from __future__ import annotations

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


duckdb = pytest.importorskip("duckdb")


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


def test_currency_csv_to_duckdb_preserves_value():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        duckdb_path = tmp_path / "currency.duckdb"

        content = 'id,amount,note\n1,"$1,000.00",US payment\n2,"€2.000,50",EU payment\n3,"USD 1000000.89",Rich customer\n'.encode("utf-8")
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(kind="database", format="duckdb", database=str(duckdb_path), table="payments"),
            source_content=content,
            source_filename="payments.csv",
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )
        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, "0" * 24)
        assert result.success is True, result.error
        assert result.records_transferred == 3

        con = duckdb.connect(str(duckdb_path))
        rows = con.execute("SELECT id, amount FROM payments ORDER BY id").fetchall()
        con.close()
        assert rows[0] == (1, 1000.00)
        assert rows[1] == (2, 2000.50)
        assert rows[2] == (3, 1000000.89)
