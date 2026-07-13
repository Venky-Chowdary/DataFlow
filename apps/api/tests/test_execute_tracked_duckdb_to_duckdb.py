"""DuckDB → DuckDB end-to-end data integrity (generic_sql round trip)."""

from __future__ import annotations

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


def test_duckdb_to_duckdb_preserves_types():
    pytest.importorskip("duckdb")

    import duckdb

    source_path = f"/tmp/duckdb_src_{uuid.uuid4().hex[:8]}.duck"
    target_path = f"/tmp/duckdb_dst_{uuid.uuid4().hex[:8]}.duck"
    table_name = "round_trip_" + uuid.uuid4().hex[:8]

    conn = duckdb.connect(source_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, amount DECIMAL(10,2), note VARCHAR, created DATE, active BOOLEAN, meta JSON, tags JSON)")
    conn.executemany(
        "INSERT INTO users VALUES (?,?,?,?,?,?,?)",
        [
            (1, Decimal("1000.00"), None, date(2024, 1, 15), True, '{"k":"v"}', '["a","b"]'),
            (2, Decimal("2000.50"), "hello", date(2024, 2, 28), False, None, None),
            (3, Decimal("3.14"), "null", date(2024, 3, 1), True, '{}', '[]'),
        ],
    )
    conn.close()

    try:
        request = TransferRequest(
            source=EndpointConfig(
                kind="database", format="duckdb",
                database=source_path, table="users",
            ),
            destination=EndpointConfig(
                kind="database", format="duckdb",
                database=target_path, table=table_name,
            ),
            sync_mode="full_refresh_overwrite",
            stream_contracts=[{
                "name": "users",
                "sync_mode": "full_refresh_overwrite",
                "primary_key": "id",
                "selected": True,
            }],
            skip_preflight=True,
        )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success is True, result.error
        assert result.records_transferred == 3
        assert result.reconciliation.get("passed") is True
        assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")

        conn = duckdb.connect(target_path)
        rows = conn.execute(f'SELECT * FROM "{table_name}" ORDER BY id').fetchall()
        conn.close()

        assert rows[0] == (1, Decimal("1000.00"), None, date(2024, 1, 15), True, '{"k":"v"}', '["a","b"]')
        assert rows[1] == (2, Decimal("2000.50"), "hello", date(2024, 2, 28), False, None, None)
        assert rows[2] == (3, 3.14, "null", date(2024, 3, 1), True, '{}', '[]')
    finally:
        for p in (source_path, target_path):
            try:
                os.unlink(p)
            except OSError:
                pass
