"""Integration tests for generic SQL / DuckDB routes."""

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

    def list_jobs(self, limit: int = 50) -> list[dict]:
        return list(self.jobs.values())

    def create_transfer_job(self, job_data: dict) -> str:
        job_id = "0" * 24
        self.jobs[job_id] = job_data
        return job_id


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    fake_mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake_mongo)


def _make_csv_bytes() -> bytes:
    return b"id,amount,active\n1,123.45,true\n2,678.90,false\n"


def test_csv_to_duckdb_and_duckdb_to_sqlite():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        duckdb_path = tmp_path / "test.duckdb"
        sqlite_path = tmp_path / "dst.db"

        # CSV -> DuckDB
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(kind="database", format="duckdb", database=str(duckdb_path), table="orders_out"),
            source_content=_make_csv_bytes(),
            source_filename="orders.csv",
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )
        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, "0" * 24)
        assert result.success is True
        assert result.records_transferred == 2

        con = duckdb.connect(str(duckdb_path))
        assert con.execute("SELECT count(*) FROM orders_out").fetchone() == (2,)
        con.close()

        # DuckDB -> SQLite
        request2 = TransferRequest(
            source=EndpointConfig(kind="database", format="duckdb", database=str(duckdb_path), table="orders_out"),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(sqlite_path), table="orders_out"),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )
        result2 = engine.execute_tracked(request2, "0" * 24)
        assert result2.success is True
        assert result2.records_transferred == 2

        conn = sqlite3.connect(str(sqlite_path))
        assert conn.execute("SELECT count(*) FROM orders_out").fetchone() == (2,)
        conn.close()
