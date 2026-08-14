"""Streaming DB->DB SCD2 and mirror transfer tests through the universal engine."""

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


def _endpoint(path: Path, table: str):
    return EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=f"sqlite:///{path}",
        database=str(path),
        table=table,
    )


def _safe_unlink(path: str | Path) -> None:
    try:
        Path(path).unlink(missing_ok=True)
    except PermissionError:
        pass


@pytest.mark.skipif(getenv_brand("SKIP_SQLITE") == "1", reason="SQLite tests disabled")
def test_stream_scd2_sqlite_to_sqlite():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE src (id TEXT, name TEXT)")
            for i in range(50):
                conn.execute(
                    "INSERT INTO src (id, name) VALUES (?, ?)",
                    (str(i), f"Item {i}"),
                )

        engine = UniversalTransferEngine()
        mappings = [
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
        ]
        result = engine.execute_tracked(
            TransferRequest(
                source=_endpoint(db_path, "src"),
                destination=_endpoint(db_path, "dst"),
                sync_mode="scd2",
                stream_contracts=[
                    {"selected": True, "primary_key": "id", "sync_mode": "scd2"}
                ],
                mappings=mappings,
                # Balanced: stream SCD2 proves history merge; FAIL_JOB/strict
                # map abort is covered by unit tests + writer FAIL_JOB proof.
                validation_mode="balanced",
                skip_preflight=False,
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
                stream_contracts=[
                    {"selected": True, "primary_key": "id", "sync_mode": "scd2"}
                ],
                mappings=mappings,
                validation_mode="balanced",
                skip_preflight=False,
            ),
            "b" * 24,
        )
        assert result2.success, result2.error
        assert result2.records_transferred == 0
    finally:
        _safe_unlink(db_path)


@pytest.mark.skipif(getenv_brand("SKIP_SQLITE") == "1", reason="SQLite tests disabled")
def test_stream_mirror_sqlite_to_sqlite():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE src (id TEXT PRIMARY KEY, name TEXT)")
            for i in range(50):
                conn.execute(
                    "INSERT INTO src (id, name) VALUES (?, ?)",
                    (str(i), f"Item {i}"),
                )
            conn.execute("CREATE TABLE dst (id TEXT PRIMARY KEY, name TEXT)")

        engine = UniversalTransferEngine()
        mappings = [
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
        ]
        result = engine.execute_tracked(
            TransferRequest(
                source=_endpoint(db_path, "src"),
                destination=_endpoint(db_path, "dst"),
                sync_mode="mirror",
                stream_contracts=[
                    {"selected": True, "primary_key": "id", "sync_mode": "mirror"}
                ],
                mappings=mappings,
                validation_mode="strict",
                skip_preflight=False,
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
                stream_contracts=[
                    {"selected": True, "primary_key": "id", "sync_mode": "mirror"}
                ],
                mappings=mappings,
                validation_mode="strict",
                skip_preflight=False,
            ),
            "d" * 24,
        )
        assert result2.success, result2.error
        ledger2 = result2.row_accounting or {}
        assert ledger2.get("reactivated") == 0, ledger2
        assert ledger2.get("inferred_deletes") == 0, ledger2

        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM src WHERE id IN ('0','1','2','3','4')")
            conn.commit()

        result3 = engine.execute_tracked(
            TransferRequest(
                source=_endpoint(db_path, "src"),
                destination=_endpoint(db_path, "dst"),
                sync_mode="mirror",
                stream_contracts=[
                    {"selected": True, "primary_key": "id", "sync_mode": "mirror"}
                ],
                mappings=mappings,
                validation_mode="strict",
                skip_preflight=False,
            ),
            "e" * 24,
        )
        assert result3.success, result3.error
        ledger3 = result3.row_accounting or {}
        assert ledger3.get("conservation_kind") == "mirror", ledger3
        assert ledger3.get("inferred_deletes") == 5, ledger3
        assert ledger3.get("reactivated") == 0, ledger3
        assert ledger3.get("active_count") == 45, ledger3
        assert ledger3.get("dest_count") == 50, ledger3

        with sqlite3.connect(db_path) as conn:
            for i in range(5):
                conn.execute(
                    "INSERT INTO src (id, name) VALUES (?, ?)",
                    (str(i), f"Item {i}"),
                )
            conn.commit()

        result4 = engine.execute_tracked(
            TransferRequest(
                source=_endpoint(db_path, "src"),
                destination=_endpoint(db_path, "dst"),
                sync_mode="mirror",
                stream_contracts=[
                    {"selected": True, "primary_key": "id", "sync_mode": "mirror"}
                ],
                mappings=mappings,
                validation_mode="strict",
                skip_preflight=False,
            ),
            "f" * 24,
        )
        assert result4.success, result4.error
        ledger4 = result4.row_accounting or {}
        assert ledger4.get("inferred_deletes") == 0, ledger4
        assert ledger4.get("reactivated") == 5, ledger4
        assert ledger4.get("active_count") == 50, ledger4
        assert ledger4.get("dest_count") == 50, ledger4
    finally:
        _safe_unlink(db_path)
