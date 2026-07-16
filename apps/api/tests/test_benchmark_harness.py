"""Verify the cloud scale harness can drive a local SQLite transfer end-to-end."""

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

from benchmarks.cloud_scale import generate_csv
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def test_generate_csv_is_detinistic_and_parsable():
    content = generate_csv(1000, seed=7)
    assert b"id,amount,status,created_at" in content
    rows = content.decode("utf-8").strip().splitlines()
    assert len(rows) == 1001
    assert rows[1].startswith("7,")


def test_local_sqlite_scale_transfer(tmp_path: Path):
    rows = 10_000
    db_path = tmp_path / "scale.db"
    content = generate_csv(rows, seed=1)

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=content,
        source_filename="scale.csv",
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=str(db_path),
            table="scale_payments",
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )

    job_id = f"bench_sqlite_{os.getpid():06d}"
    result = UniversalTransferEngine().execute_tracked(request, job_id)

    assert result.success, result.error
    assert result.records_transferred == rows
    assert result.records_per_second > 0
    assert result.reconciliation.get("passed") is True

    conn = sqlite3.connect(str(db_path))
    try:
        count = conn.execute("SELECT COUNT(*) FROM scale_payments").fetchone()[0]
        assert count == rows
    finally:
        conn.close()
