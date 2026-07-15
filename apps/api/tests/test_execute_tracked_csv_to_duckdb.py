"""Data integrity: CSV with messy data (nulls, JSON, booleans, dates, locale decimals) → DuckDB."""

from __future__ import annotations

import csv
import io
import os
import sys
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    fieldnames = ["id", "amount", "note", "created", "active", "meta", "tags"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_messy_csv_to_duckdb_preserves_types():
    pytest.importorskip("duckdb")

    table_name = "messy_duckdb_test_" + uuid.uuid4().hex[:8]
    path = f"/tmp/{table_name}.duck"
    rows = [
        {"id": "1", "amount": "1,000.00", "note": "", "created": "2024-01-15", "active": "true", "meta": '{"k":"v"}', "tags": '["a","b"]'},
        {"id": "2", "amount": "2.000,50", "note": "hello", "created": "2024-02-28 14:30:00", "active": "false", "meta": "", "tags": ""},
        {"id": "3", "amount": "3.14", "note": "null", "created": "2024-03-01", "active": "1", "meta": "{}", "tags": "[]"},
    ]
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="messy.csv",
        source_content=_csv_bytes(rows),
        destination=EndpointConfig(
            kind="database",
            format="duckdb",
            database=path,
            table=table_name,
        ),
        sync_mode="upsert",
        stream_contracts=[{
            "name": "payments",
            "sync_mode": "upsert",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )

    try:
        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success is True, result.error
        assert result.records_transferred == 3
        assert result.reconciliation.get("passed") is True
        assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")

        import duckdb

        conn = duckdb.connect(path)
        rows = conn.execute(f'SELECT * FROM "{table_name}" ORDER BY id').fetchall()
        conn.close()

        assert rows[0] == (1, 1000.0, None, date(2024, 1, 15), True, '{"k":"v"}', '["a","b"]')
        assert rows[1] == (2, 2000.5, "hello", date(2024, 2, 28), False, None, None)
        assert rows[2] == (3, 3.14, "null", date(2024, 3, 1), True, '{}', '[]')
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
