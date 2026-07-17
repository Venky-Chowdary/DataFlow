"""Parquet file → PostgreSQL end-to-end."""

from __future__ import annotations

import io
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _parquet_bytes(rows: list[dict]) -> bytes:
    try:
        import pandas as pd
    except ImportError as exc:
        pytest.skip(f"pandas not installed: {exc}")
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.Table.from_pandas(pd.DataFrame(rows))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def test_parquet_to_postgresql():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"PostgreSQL emulator not reachable: {exc}")

    table_name = "pq_to_pg_" + uuid.uuid4().hex[:8]
    parquet_content = _parquet_bytes([
        {"id": 1, "name": "alice", "amount": "1000.00"},
        {"id": 2, "name": "bob", "amount": "2000.50"},
    ])

    request = TransferRequest(
        source=EndpointConfig(
            kind="file", format="parquet",
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql",
            host="localhost", port=5432, database="dataflow",
            username="dataflow", password="dataflow",
            schema="public", table=table_name,
        ),
        source_filename="orders.parquet",
        source_content=parquet_content,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")
