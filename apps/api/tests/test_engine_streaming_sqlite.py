"""Integration test for UniversalTransferEngine DB→DB streaming and resume."""

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

from services.checkpoint_service import Checkpoint  # noqa: E402
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

    def list_jobs(self, limit: int = 50) -> list[dict]:
        return list(self.jobs.values())

    def create_transfer_job(self, job_data: dict) -> str:
        job_id = "0" * 24
        self.jobs[job_id] = job_data
        return job_id


def _make_source(rows: int, db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount TEXT, active TEXT)")
    for i in range(1, rows + 1):
        conn.execute(
            "INSERT INTO orders VALUES (?, ?, ?)",
            (i, f"{i * 1.5:.2f}", "true" if i % 2 == 1 else "false"),
        )
    conn.commit()
    conn.close()


def _populate_destination(rows: int, db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE orders_out (id INTEGER, amount TEXT, active TEXT)")
    for i in range(1, rows + 1):
        conn.execute(
            "INSERT INTO orders_out VALUES (?, ?, ?)",
            (i, f"{i * 1.5:.2f}", "true" if i % 2 == 1 else "false"),
        )
    conn.commit()
    conn.close()


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    fake_mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake_mongo)


def test_engine_stream_sqlite_to_sqlite():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = tmp_path / "src.db"
        dst = tmp_path / "dst.db"
        _make_source(250, src)

        source = EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="orders"
        )
        destination = EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="orders_out"
        )
        request = TransferRequest(
            source=source,
            destination=destination,
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, "000000000000000000000000")
        assert result.success is True

        conn = sqlite3.connect(dst)
        count = conn.execute("SELECT count(*) FROM orders_out").fetchone()[0]
        conn.close()
        assert count == 250


def test_engine_stream_sqlite_to_sqlite_resume_from_checkpoint():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = tmp_path / "src.db"
        dst = tmp_path / "dst.db"
        _make_source(500, src)
        _populate_destination(250, dst)

        # Pre-seed a checkpoint that says the first 250 rows were already committed.
        fake_mongo = engine_mod.get_mongodb_service()
        checkpoint = Checkpoint(
            job_id="000000000000000000000000",
            chunk_index=1,
            offset=250,
            rows_processed=250,
        )
        fake_mongo.update_job_status(
            "000000000000000000000000",
            "running",
            checkpoint=checkpoint.to_dict(),
            transfer_request={},
        )

        source = EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="orders"
        )
        destination = EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="orders_out"
        )
        request = TransferRequest(
            source=source,
            destination=destination,
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, "000000000000000000000000", resume=True)
        assert result.success is True

        conn = sqlite3.connect(dst)
        count = conn.execute("SELECT count(*) FROM orders_out").fetchone()[0]
        conn.close()
        assert count == 500
