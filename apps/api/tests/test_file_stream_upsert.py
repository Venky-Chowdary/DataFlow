"""File-stream upsert semantics for incremental/deduped transfers."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb", reason="requires the optional DuckDB test dependency")

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.file_stream import stream_file_to_database  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402


class _FakeCheckpointService:
    """In-memory checkpoint service that never touches MongoDB."""

    def __init__(self):
        self.checkpoints = {}

    def save(self, checkpoint) -> bool:
        self.checkpoints[checkpoint.job_id] = checkpoint.to_dict()
        return True

    def load(self, job_id: str):
        return self.checkpoints.get(job_id)


def _make_csv(rows: list[tuple]) -> bytes:
    lines = "id,amount\n" + "\n".join(f"{i},{a}" for i, a in rows)
    return lines.encode("utf-8")


def test_csv_file_stream_to_duckdb_upsert():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        duckdb_path = tmp_path / "payments.duckdb"
        dest = EndpointConfig(
            kind="database",
            format="duckdb",
            database=str(duckdb_path),
            table="payments",
        )
        cp = _FakeCheckpointService()
        job_id = "000000000000000000000000"

        rows1, _, _, _ = stream_file_to_database(
            _make_csv([(1, 1000.00), (2, 2000.50), (3, 3000.00)]),
            "payments.csv",
            dest,
            [],
            {},
            job_id=job_id,
            checkpoint_service=cp,
            sync_mode="incremental_deduped",
            stream_contracts=[
                {"selected": True, "sync_mode": "incremental_deduped", "primary_key": "id"}
            ],
        )
        assert rows1 == 3

        rows2, _, _, _ = stream_file_to_database(
            _make_csv([(1, 1111.00), (4, 4000.00)]),
            "payments.csv",
            dest,
            [],
            {},
            job_id=job_id,
            checkpoint_service=cp,
            sync_mode="incremental_deduped",
            stream_contracts=[
                {"selected": True, "sync_mode": "incremental_deduped", "primary_key": "id"}
            ],
        )
        assert rows2 == 2

        con = duckdb.connect(str(duckdb_path))
        result = con.execute("SELECT id, amount FROM payments ORDER BY id").fetchall()
        con.close()
        assert result == [(1, 1111.0), (2, 2000.5), (3, 3000.0), (4, 4000.0)]
