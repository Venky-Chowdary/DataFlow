"""End-to-end execute_tracked CSV→Snowflake upsert via fakesnow."""

from __future__ import annotations

import csv
import io
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.snowflake_conn import get_connection, resolve_or_fold_snowflake_table
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "amount"])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _new_job_id() -> str:
    return uuid.uuid4().hex[:24]


def test_csv_to_snowflake_upsert_updates_and_appends():
    pytest.importorskip("fakesnow")

    table_name = "payments_snowflake_upsert_e2e_" + uuid.uuid4().hex[:8]
    destination = EndpointConfig(
        kind="database",
        format="snowflake",
        host="localhost",
        port=443,
        database="dataflow",
        username="test",
        password="test",
        schema="public",
        table=table_name,
    )
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="payments.csv",
        source_content=_csv_bytes([
            {"id": "1", "amount": "1000.00"},
            {"id": "2", "amount": "2000.50"},
        ]),
        destination=destination,
        sync_mode="upsert",
        stream_contracts=[{
            "name": "payments",
            "sync_mode": "upsert",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result1 = engine.execute_tracked(request, _new_job_id())
    assert result1.success, result1.error
    assert result1.records_transferred == 2
    assert result1.reconciliation.get("passed") is True
    assert result1.reconciliation.get("target_rows") == 2

    request.source_content = _csv_bytes([
        {"id": "1", "amount": "1111.00"},
        {"id": "3", "amount": "3000.00"},
    ])
    result2 = engine.execute_tracked(request, _new_job_id())
    assert result2.success, result2.error
    assert result2.records_transferred == 2
    assert result2.reconciliation.get("passed") is True
    assert result2.reconciliation.get("target_rows") == 3

    conn = get_connection(
        account="localhost",
        username="test",
        password="test",
        database="dataflow",
        schema="public",
        warehouse="",
        connection_string="",
    )
    try:
        cur = conn.cursor()
        # Resolve the actual stored table name (Snowflake folds unquoted names).
        resolved = resolve_or_fold_snowflake_table(cur, "public", table_name)
        cur.execute(f"SELECT id, amount FROM {resolved} ORDER BY id")
        rows = cur.fetchall()
    finally:
        conn.close()
    assert len(rows) == 3
    by_id = {r[0]: r[1] for r in rows}
    assert by_id[1] == pytest.approx(1111.00)
    assert by_id[2] == pytest.approx(2000.50)
    assert by_id[3] == pytest.approx(3000.00)
