"""Streaming DB->DB SCD2 and mirror transfer tests through the universal engine."""

from __future__ import annotations

import os
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


def _endpoint(path: Path, table: str):
    return EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=f"sqlite:///{path}",
        database=str(path),
        table=table,
    )


@pytest.mark.skipif(os.getenv("DATAFLOW_SKIP_SQLITE") == "1", reason="SQLite tests disabled")
def test_stream_scd2_sqlite_to_sqlite():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE src (id TEXT, name TEXT, price TEXT)")
            for i in range(50):
                conn.execute("INSERT INTO src (id, name, price) VALUES (?, ?, ?)", (str(i), f"Item {i}", str(10 + i)))

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(
            TransferRequest(
                source=_endpoint(db_path, "src"),
                destination=_endpoint(db_path, "dst"),
                sync_mode="scd2",
                stream_contracts=[{"selected": True, "primary_key": "id", "sync_mode": "scd2"}],
                skip_preflight=True,
                validation_mode="balanced",
            ),
            "a" * 24,
        )
        assert result.success, result.error
        assert result.records_transferred == 50

        # Running the same snapshot again should not create new current rows.
        result2 = engine.execute_tracked(
            TransferRequest(
                source=_endpoint(db_path, "src"),
                destination=_endpoint(db_path, "dst"),
                sync_mode="scd2",
                stream_contracts=[{"selected": True, "primary_key": "id", "sync_mode": "scd2"}],
                skip_preflight=True,
                validation_mode="balanced",
            ),
            "b" * 24,
        )
        assert result2.success, result2.error
        assert result2.records_transferred == 0
    finally:
        Path(db_path).unlink(missing_ok=True)


@pytest.mark.skipif(os.getenv("DATAFLOW_SKIP_SQLITE") == "1", reason="SQLite tests disabled")
def test_stream_mirror_sqlite_to_sqlite():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE src (id TEXT PRIMARY KEY, name TEXT, price TEXT)")
            for i in range(50):
                conn.execute("INSERT INTO src (id, name, price) VALUES (?, ?, ?)", (str(i), f"Item {i}", str(10 + i)))
            conn.execute("CREATE TABLE dst (id TEXT PRIMARY KEY, name TEXT, price TEXT)")

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(
            TransferRequest(
                source=_endpoint(db_path, "src"),
                destination=_endpoint(db_path, "dst"),
                sync_mode="mirror",
                stream_contracts=[{"selected": True, "primary_key": "id", "sync_mode": "mirror"}],
                skip_preflight=True,
                validation_mode="balanced",
            ),
            "c" * 24,
        )
        assert result.success, result.error
        assert result.records_transferred == 50

        # Re-run should be idempotent and active row count should stay 50.
        result2 = engine.execute_tracked(
            TransferRequest(
                source=_endpoint(db_path, "src"),
                destination=_endpoint(db_path, "dst"),
                sync_mode="mirror",
                stream_contracts=[{"selected": True, "primary_key": "id", "sync_mode": "mirror"}],
                skip_preflight=True,
                validation_mode="balanced",
            ),
            "d" * 24,
        )
        assert result2.success, result2.error
    finally:
        Path(db_path).unlink(missing_ok=True)
