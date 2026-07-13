"""Streaming DB→DB integration test: SQLite source → SQLite destination."""

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

from services.checkpoint_service import CheckpointService  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402
from src.transfer.stream import stream_database_transfer  # noqa: E402


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


def _make_source(rows: int, tmp_path: Path) -> Path:
    db = tmp_path / "src.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount TEXT, active TEXT)")
    for i in range(rows):
        conn.execute(
            "INSERT INTO orders VALUES (?, ?, ?)",
            (i + 1, f"{i * 1.5:.2f}", "true" if i % 2 == 0 else "false"),
        )
    conn.commit()
    conn.close()
    return db


def test_stream_sqlite_to_sqlite_basic():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = _make_source(250, tmp_path)
        dst = tmp_path / "dst.db"

        source = EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="orders"
        )
        destination = EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="orders_out"
        )
        mappings = [
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
            {"source": "active", "target": "active"},
        ]
        schema = {"id": "integer", "amount": "decimal", "active": "boolean"}

        fake_mongo = _FakeMongo()
        rows_written, _ddl, _summary, columns = stream_database_transfer(
            source,
            destination,
            mappings,
            schema,
            job_id="000000000000000000000000",
            checkpoint_service=CheckpointService(fake_mongo),
        )

        assert rows_written == 250
        assert "id" in columns

        conn = sqlite3.connect(dst)
        count = conn.execute("SELECT count(*) FROM orders_out").fetchone()[0]
        conn.close()
        assert count == 250


def test_stream_sqlite_to_sqlite_resume_from_checkpoint():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = _make_source(500, tmp_path)
        dst = tmp_path / "dst.db"

        # Pre-populate the destination as if a previous run wrote the first 250 rows.
        conn = sqlite3.connect(dst)
        conn.execute(
            "CREATE TABLE orders_out (id INTEGER, amount TEXT, active TEXT)"
        )
        for i in range(1, 251):
            conn.execute(
                "INSERT INTO orders_out VALUES (?, ?, ?)",
                (i, f"{i * 1.5:.2f}", "true" if (i - 1) % 2 == 0 else "false"),
            )
        conn.commit()
        conn.close()

        source = EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="orders"
        )
        destination = EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="orders_out"
        )
        mappings = [
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
            {"source": "active", "target": "active"},
        ]
        schema = {"id": "integer", "amount": "decimal", "active": "boolean"}

        fake_mongo = _FakeMongo()
        # Simulate a prior run that committed the first 250 rows.
        from services.checkpoint_service import Checkpoint

        checkpoint = Checkpoint(
            job_id="000000000000000000000000",
            chunk_index=1,
            offset=250,
            rows_processed=250,
        )

        rows_written, _ddl, _summary, _columns = stream_database_transfer(
            source,
            destination,
            mappings,
            schema,
            job_id="000000000000000000000000",
            checkpoint=checkpoint,
            checkpoint_service=CheckpointService(fake_mongo),
        )

        # The first 250 rows should be skipped, then the remaining 250 written.
        # rows_written is cumulative (250 already + 250 new = 500).
        assert rows_written == 500
        conn = sqlite3.connect(dst)
        count = conn.execute("SELECT count(*) FROM orders_out").fetchone()[0]
        conn.close()
        assert count == 500


def test_stream_sqlite_includes_ddl_log_and_summary():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = _make_source(5, tmp_path)
        dst = tmp_path / "dst.db"

        source = EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="orders"
        )
        destination = EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="orders_out"
        )
        mappings = [
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
            {"source": "active", "target": "active"},
        ]
        schema = {"id": "integer", "amount": "decimal", "active": "boolean"}

        fake_mongo = _FakeMongo()
        rows_written, ddl_log, summary, _columns = stream_database_transfer(
            source,
            destination,
            mappings,
            schema,
            job_id="000000000000000000000000",
            checkpoint_service=CheckpointService(fake_mongo),
        )

        assert rows_written == 5
        assert any("STREAM" in line for line in ddl_log)
        assert summary["type"] == "sqlite"
        assert summary["table"] == "orders_out"
