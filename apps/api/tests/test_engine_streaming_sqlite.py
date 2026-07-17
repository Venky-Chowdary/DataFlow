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

import src.transfer.engine as engine_mod  # noqa: E402
from services import sync_cursor as sync_cursor_mod  # noqa: E402
from services.checkpoint_service import Checkpoint  # noqa: E402
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


def test_engine_stream_sqlite_to_sqlite_incremental_deduped():
    """Incremental-deduped transfers should update existing keys and insert new ones."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        sync_cursor_mod.STORE_PATH = tmp_path / "sync_cursors.json"
        if sync_cursor_mod.STORE_PATH.exists():
            sync_cursor_mod.STORE_PATH.unlink()

        src = tmp_path / "src.db"
        dst = tmp_path / "dst.db"
        _make_source(500, src)

        source = EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="orders"
        )
        destination = EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="orders_out"
        )
        request = TransferRequest(
            source=source,
            destination=destination,
            sync_mode="incremental_deduped",
            stream_contracts=[
                {
                    "selected": True,
                    "sync_mode": "incremental_deduped",
                    "primary_key": "id",
                    "cursor_field": "id",
                }
            ],
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, "000000000000000000000000")
        assert result.success is True

        conn = sqlite3.connect(src)
        conn.execute("UPDATE orders SET amount = 999999.99 WHERE id = 1")
        conn.execute("INSERT INTO orders (id, amount) VALUES (501, 123.45)")
        conn.commit()
        conn.close()

        result = engine.execute_tracked(request, "000000000000000000000000")
        assert result.success is True

        conn = sqlite3.connect(dst)
        count = conn.execute("SELECT count(*) FROM orders_out").fetchone()[0]
        updated = conn.execute(
            "SELECT amount FROM orders_out WHERE id = 1"
        ).fetchone()[0]
        new_id = conn.execute(
            "SELECT id FROM orders_out WHERE id = 501"
        ).fetchone()
        conn.close()
        assert count == 501
        assert updated == "999999.99"
        assert new_id == (501,)


def test_engine_stream_sqlite_to_sqlite_incremental_cursor_rollover():
    """Incremental cursor must advance numerically, not lexicographically."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = tmp_path / "src.db"
        dst = tmp_path / "dst.db"

        conn = sqlite3.connect(src)
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount TEXT)")
        for i in range(1, 6):
            conn.execute("INSERT INTO orders VALUES (?, ?)", (i, str(i * 1.5)))
        conn.execute("INSERT INTO orders VALUES (?, ?)", (1000, "1500.0"))
        conn.commit()
        conn.close()

        source = EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="orders"
        )
        destination = EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="orders_out"
        )
        request = TransferRequest(
            source=source,
            destination=destination,
            sync_mode="incremental_deduped",
            stream_contracts=[
                {
                    "name": "orders",
                    "sync_mode": "incremental_deduped",
                    "selected": True,
                    "primary_key": "id",
                    "cursor_field": "id",
                }
            ],
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, "000000000000000000000000")
        assert result.success is True

        conn = sqlite3.connect(src)
        conn.execute("INSERT INTO orders (id, amount) VALUES (?, ?)", (2000, "3000.0"))
        conn.execute("UPDATE orders SET amount = '9999.0' WHERE id = 1")
        conn.commit()
        conn.close()

        result = engine.execute_tracked(request, "000000000000000000000000")
        assert result.success is True

        conn = sqlite3.connect(dst)
        count = conn.execute("SELECT count(*) FROM orders_out").fetchone()[0]
        updated = conn.execute("SELECT amount FROM orders_out WHERE id = 1").fetchone()[0]
        new_id = conn.execute("SELECT id FROM orders_out WHERE id = 2000").fetchone()
        conn.close()
        assert count == 7
        assert updated == "9999.0"
        assert new_id == (2000,)


def test_engine_stream_sqlite_resume_no_checkpoint_drops_partial_destination():
    """If a resume has no checkpoint, the destination must be dropped so no partial
    data from a previous failed run is duplicated.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = tmp_path / "src.db"
        dst = tmp_path / "dst.db"
        _make_source(500, src)
        _populate_destination(300, dst)

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
