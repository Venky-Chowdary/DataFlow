"""Business-rule guard: append keeps existing rows, overwrite replaces them."""

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


def test_append_preserves_existing_rows():
    """Append should add new rows without deleting previously loaded data."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "append.db"

        # Pre-populate destination with existing products.
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE products (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO products VALUES (1, 'existing-1'), (2, 'existing-2')")
        conn.commit()
        conn.close()

        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="products.csv",
            source_content=_csv_content([
                {"id": "3", "name": "new-1"},
                {"id": "4", "name": "new-2"},
            ]),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(db_path),
                table="products",
            ),
            sync_mode="append",
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, uuid.uuid4().hex)
        assert result.success is True, result.error
        assert result.records_transferred == 2

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT id, name FROM products ORDER BY id").fetchall()
        conn.close()
        assert rows == [(1, "existing-1"), (2, "existing-2"), (3, "new-1"), (4, "new-2")]


def test_full_refresh_overwrite_replaces_existing_rows():
    """Full-refresh overwrite should replace the entire table, not merge."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "overwrite.db"

        # Pre-populate destination with stale rows.
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE products (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO products VALUES (1, 'stale'), (99, 'orphan')")
        conn.commit()
        conn.close()

        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="products.csv",
            source_content=_csv_content([
                {"id": "1", "name": "fresh-1"},
                {"id": "2", "name": "fresh-2"},
            ]),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(db_path),
                table="products",
            ),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, uuid.uuid4().hex)
        assert result.success is True, result.error
        assert result.records_transferred == 2

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT id, name FROM products ORDER BY id").fetchall()
        conn.close()
        assert rows == [(1, "fresh-1"), (2, "fresh-2")]
