"""Upsert keys on the destination's declared primary key when no contract sets one.

The catalog key is evidence, not inference: refusing the run while the table
itself declares the key made every upsert route unusable unless the operator had
also typed the key into the stream contract. An unmapped key column still
refuses — keying on a column the write never supplies would insert duplicates.
"""

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
    def __init__(self) -> None:
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
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _FakeMongo())


def _csv(rows: list[dict]) -> bytes:
    cols = list(rows[0].keys())
    lines = [",".join(cols)]
    lines += [",".join(str(r[c]) for c in cols) for r in rows]
    return "\n".join(lines).encode("utf-8")


def _keyed_sqlite(tmp: str) -> Path:
    db_path = Path(tmp) / "declared_key.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE payments (id INTEGER PRIMARY KEY, amount TEXT)")
    conn.commit()
    conn.close()
    return db_path


def _request(db_path: Path, rows: list[dict], mappings: list[dict]) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="payments.csv",
        source_content=_csv(rows),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(db_path),
            table="payments",
        ),
        sync_mode="incremental_deduped",
        mappings=mappings,
        skip_preflight=True,
    )


def test_upsert_without_contract_key_uses_the_declared_destination_key():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _keyed_sqlite(tmp)
        maps = [
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "amount", "target": "amount", "confidence": 0.99},
        ]
        engine = UniversalTransferEngine()

        first = engine.execute_tracked(
            _request(db_path, [{"id": "1", "amount": "10"}, {"id": "2", "amount": "20"}], maps),
            uuid.uuid4().hex,
        )
        assert first.success is True, first.error

        second = engine.execute_tracked(
            _request(db_path, [{"id": "1", "amount": "99"}, {"id": "3", "amount": "30"}], maps),
            uuid.uuid4().hex,
        )
        assert second.success is True, second.error

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT id, amount FROM payments ORDER BY id").fetchall()
        conn.close()
        # Converged on the declared key — no duplicate id 1.
        assert rows == [(1, "99"), (2, "20"), (3, "30")]


def test_upsert_still_refuses_when_the_declared_key_is_not_written():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = _keyed_sqlite(tmp)
        result = UniversalTransferEngine().execute_tracked(
            _request(
                db_path,
                [{"amount": "10"}, {"amount": "20"}],
                [{"source": "amount", "target": "amount", "confidence": 0.99}],
            ),
            uuid.uuid4().hex,
        )
        assert result.success is False
        assert "primary_key" in (result.error or "")
