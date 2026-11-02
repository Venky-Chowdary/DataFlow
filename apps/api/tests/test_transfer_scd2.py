"""Integration tests for SCD2 sync mode through the universal transfer engine."""

from __future__ import annotations

import os
from services.brand_env import getenv_brand
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _sqlite_endpoint(path: Path, table: str):
    return EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=f"sqlite:///{path}",
        database=str(path),
        table=table,
    )


def _csv_path(rows: list[dict], path: Path):
    import csv

    columns = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)


@pytest.mark.skipif(
    getenv_brand("SKIP_SQLITE") == "1",
    reason="SQLite tests disabled",
)
def test_transfer_scd2_csv_to_sqlite():
    """SCD2 history merge with Validate ON — clean text path (no DECIMAL invent)."""
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    csv_fd, csv_path = tempfile.mkstemp(suffix=".csv")
    os.close(csv_fd)
    try:
        # Integer/text only — DECIMAL invent fidelity is covered by fidelity matrices.
        _csv_path(
            [
                {"id": "1", "name": "A"},
                {"id": "2", "name": "B"},
            ],
            Path(csv_path),
        )
        source = EndpointConfig(
            kind="file",
            format="csv",
            connection_string=csv_path,
            database=csv_path,
        )
        dest = _sqlite_endpoint(Path(db_path), "products")
        request = TransferRequest(
            source=source,
            destination=dest,
            sync_mode="scd2",
            validation_mode="strict",
            stream_contracts=[
                {
                    "selected": True,
                    "primary_key": "id",
                    "sync_mode": "scd2",
                }
            ],
            mappings=[
                {
                    "source": "id",
                    "target": "id",
                    "confidence": 1.0,
                    "user_override": True,
                    "transform": "none",
                },
                {
                    "source": "name",
                    "target": "name",
                    "confidence": 1.0,
                    "user_override": True,
                    "transform": "none",
                },
            ],
            source_filename="products.csv",
            source_content=Path(csv_path).read_bytes(),
        )
        engine = UniversalTransferEngine()
        result = engine.execute(request)
        assert result.success, result.error
        assert result.records_transferred == 2

        # Re-run with one changed row.
        _csv_path(
            [
                {"id": "1", "name": "A-updated"},
                {"id": "2", "name": "B"},
            ],
            Path(csv_path),
        )
        request.source_content = Path(csv_path).read_bytes()
        result = engine.execute(request)
        assert result.success, result.error
        # Only the changed row creates a new current version.
        assert result.records_transferred == 1

        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT COUNT(*) FROM products")
        total = cur.fetchone()[0]
        assert total == 3
        cur = conn.execute("SELECT COUNT(*) FROM products WHERE is_current = 1")
        active = cur.fetchone()[0]
        assert active == 2
        conn.close()
    finally:
        try:
            Path(db_path).unlink(missing_ok=True)
        except PermissionError:
            pass
        try:
            Path(csv_path).unlink(missing_ok=True)
        except PermissionError:
            pass
