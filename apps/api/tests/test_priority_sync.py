"""Priority-first sync and limit tests."""

from __future__ import annotations

import csv
import io
import sqlite3
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import get_transfer_engine
from src.transfer.models import EndpointConfig, TransferRequest


def _csv_bytes(rows: list[dict]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def test_priority_sync_sorts_and_limits(tmp_path):
    db_path = tmp_path / "priority.db"
    csv_path = tmp_path / "priority.csv"
    rows = [
        {"id": "1", "priority": "10", "name": "low"},
        {"id": "2", "priority": "100", "name": "high"},
        {"id": "3", "priority": "50", "name": "mid"},
    ]
    csv_path.write_bytes(_csv_bytes(rows))

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(db_path),
            table="leads",
        ),
        source_filename="priority.csv",
        source_content=csv_path.read_bytes(),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        priority_column="priority",
        priority_direction="desc",
        limit=2,
    )
    engine = get_transfer_engine()
    result = engine.execute(request)
    assert result.success, result.error
    assert result.records_transferred == 2

    conn = sqlite3.connect(str(db_path))
    cur = conn.execute("SELECT id FROM leads ORDER BY priority DESC")
    ids = [r[0] for r in cur.fetchall()]
    conn.close()
    assert ids == [2, 3]


def test_priority_sync_ascending(tmp_path):
    db_path = tmp_path / "priority_asc.db"
    csv_path = tmp_path / "priority_asc.csv"
    rows = [
        {"id": "1", "score": "9"},
        {"id": "2", "score": "3"},
        {"id": "3", "score": "6"},
    ]
    csv_path.write_bytes(_csv_bytes(rows))

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(db_path),
            table="scores",
        ),
        source_filename="priority_asc.csv",
        source_content=csv_path.read_bytes(),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        priority_column="score",
        priority_direction="asc",
    )
    engine = get_transfer_engine()
    result = engine.execute(request)
    assert result.success, result.error
    assert result.records_transferred == 3

    conn = sqlite3.connect(str(db_path))
    cur = conn.execute("SELECT id FROM scores ORDER BY score ASC")
    ids = [r[0] for r in cur.fetchall()]
    conn.close()
    assert ids == [2, 3, 1]
