"""Snowflake → Snowflake schema drift: backfill_new_fields adds a new column."""

from __future__ import annotations

import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("fakesnow", reason="requires the optional Snowflake test dependency")

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.snowflake_conn import get_connection
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def test_snowflake_to_snowflake_backfill_new_fields():
    src_table = f"sf_src_backfill_{uuid.uuid4().hex[:8]}"
    dst_table = f"sf_dst_backfill_{uuid.uuid4().hex[:8]}"

    # Rely on snowflake_conn auto-patch for localhost — never nest fakesnow.patch().
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
        cur.execute(f'DROP TABLE IF EXISTS "{src_table}"')
        cur.execute(f'DROP TABLE IF EXISTS "{dst_table}"')
        cur.execute(f'CREATE TABLE "{src_table}" (id INT, amount DECIMAL(18,2))')
        cur.execute(f'CREATE TABLE "{dst_table}" (id INT, amount DECIMAL(18,2))')
        cur.execute(f'INSERT INTO "{src_table}" (id, amount) VALUES (1, 1000.00), (2, 2000.50)')
        cur.execute(f'INSERT INTO "{dst_table}" (id, amount) VALUES (1, 1000.00)')
        conn.commit()

        request = TransferRequest(
            source=EndpointConfig(
                kind="database", format="snowflake", host="localhost", port=443,
                database="dataflow", username="test", password="test",
                schema="public", table=src_table,
            ),
            destination=EndpointConfig(
                kind="database", format="snowflake", host="localhost", port=443,
                database="dataflow", username="test", password="test",
                schema="public", table=dst_table,
            ),
            sync_mode="upsert",
            stream_contracts=[{
                "name": "payments",
                "sync_mode": "upsert",
                "primary_key": "id",
                "selected": True,
            }],
            backfill_new_fields=True,
            skip_preflight=True,
        )
        engine = UniversalTransferEngine()
        result1 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
        assert result1.success is True, result1.error
        assert result1.records_transferred == 2

        cur.execute(f'ALTER TABLE "{src_table}" ADD COLUMN currency VARCHAR(10)')
        cur.execute(f"UPDATE \"{src_table}\" SET currency = 'USD' WHERE id = 1")
        cur.execute(f"UPDATE \"{src_table}\" SET currency = 'EUR' WHERE id = 2")
        conn.commit()

        result2 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
        assert result2.success is True, result2.error
        assert result2.records_transferred == 2

        cur.execute(f'SELECT id, amount, currency FROM "{dst_table}" ORDER BY id')
        rows = cur.fetchall()
        assert rows == [(1, Decimal("1000.00"), "USD"), (2, Decimal("2000.50"), "EUR")]
    finally:
        conn.close()
