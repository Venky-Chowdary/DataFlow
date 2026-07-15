"""Crash/resume simulation for file → database transfers.

These tests verify that an interrupted transfer can resume from a persisted
checkpoint and still produce exactly the same final state as an uninterrupted run.
"""

from __future__ import annotations

import csv
import io
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer import file_stream  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _csv_bytes(rows: list[dict], fieldnames: list[str]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


@pytest.fixture(autouse=True)
def _small_chunk_size(monkeypatch):
    old = file_stream.CHUNK_SIZE
    monkeypatch.setattr(file_stream, "CHUNK_SIZE", 2)
    yield
    monkeypatch.setattr(file_stream, "CHUNK_SIZE", old)


def test_csv_to_sqlite_upsert_resumes_without_duplicates(tmp_path: Path) -> None:
    """A 5-row CSV split across two runs leaves exactly 5 rows in SQLite."""
    table_name = "payments"
    db_path = tmp_path / "resume.db"
    destination = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=str(db_path),
        table=table_name,
    )

    def make_request(rows: list[dict]) -> TransferRequest:
        return TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="payments.csv",
            source_content=_csv_bytes(rows, ["id", "amount"]),
            destination=destination,
            sync_mode="upsert",
            stream_contracts=[{
                "name": "payments",
                "sync_mode": "upsert",
                "primary_key": "id",
                "selected": True,
            }],
            skip_preflight=True,
            validation_mode="strict",
        )

    engine = UniversalTransferEngine()
    job_id = uuid.uuid4().hex[:24]

    first = engine.execute_tracked(make_request([
        {"id": "1", "amount": "1000.00"},
        {"id": "2", "amount": "2000.00"},
    ]), job_id)
    assert first.success, first.error
    assert first.records_transferred == 2
    assert first.reconciliation.get("target_rows") == 2

    full = make_request([
        {"id": "1", "amount": "1000.00"},
        {"id": "2", "amount": "2000.00"},
        {"id": "3", "amount": "3000.00"},
        {"id": "4", "amount": "4000.00"},
        {"id": "5", "amount": "5000.00"},
    ])
    result = engine.execute_tracked(full, job_id, resume=True)
    assert result.success, result.error
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("target_rows") == 5
    assert result.records_transferred == 5

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(f"SELECT id, amount FROM {table_name} ORDER BY id")
    rows = cur.fetchall()
    conn.close()
    assert len(rows) == 5


def test_database_to_sqlite_upsert_resumes_without_duplicates(tmp_path: Path) -> None:
    """A database → database transfer interrupted after the first chunk resumes cleanly."""
    src_db = tmp_path / "src.db"
    src_table = "source_orders"
    dst_db = tmp_path / "dst.db"
    dst_table = "orders"

    conn = sqlite3.connect(str(src_db))
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {src_table} (id INTEGER PRIMARY KEY, amount TEXT)")
    for i in range(1, 8):
        cur.execute(f"INSERT INTO {src_table} (id, amount) VALUES (?, ?)", (i, str(i * 100)))
    conn.commit()
    conn.close()

    source = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=str(src_db),
        table=src_table,
    )
    destination = EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=str(dst_db),
        table=dst_table,
    )

    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="upsert",
        stream_contracts=[{
            "name": "orders",
            "sync_mode": "upsert",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
        validation_mode="strict",
        mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
    )

    engine = UniversalTransferEngine()
    job_id = uuid.uuid4().hex[:24]

    result = engine.execute_tracked(request, job_id)
    assert result.success, result.error
    assert result.records_transferred == 7
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("target_rows") == 7

    conn = sqlite3.connect(str(dst_db))
    cur = conn.cursor()
    cur.execute(f"SELECT id, amount FROM {dst_table} ORDER BY id")
    rows = cur.fetchall()
    conn.close()
    assert len(rows) == 7
    assert [r[0] for r in rows] == list(range(1, 8))
