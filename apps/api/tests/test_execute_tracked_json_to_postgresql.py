"""JSON and JSONL → PostgreSQL end-to-end, exercising non-streaming and streaming paths."""

from __future__ import annotations

import json
import socket
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _json_bytes(rows: list[dict]) -> bytes:
    return json.dumps(rows).encode("utf-8")


def _jsonl_bytes(rows: list[dict]) -> bytes:
    return b"\n".join(json.dumps(r).encode("utf-8") for r in rows)


def _pg_destination(table_name: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=table_name,
    )


@pytest.mark.parametrize("fmt, content_fn", [
    ("json", _json_bytes),
    ("jsonl", _jsonl_bytes),
])
def test_json_to_postgresql_preserves_types(fmt: str, content_fn):
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL not reachable on localhost:5432")

    table_name = f"json_pg_{fmt}_{uuid.uuid4().hex[:8]}"
    rows = [
        {"id": "1", "amount": "1000.00", "note": "hello", "created": "2024-01-15T00:00:00", "active": "true", "meta": '{"k":"v"}', "tags": '["a","b"]'},
        {"id": "2", "amount": "2000.50", "note": "", "created": "2024-02-28T14:30:00", "active": "false", "meta": "", "tags": ""},
        {"id": "3", "amount": "3.14", "note": "null", "created": "2024-03-01T00:00:00", "active": "1", "meta": "{}", "tags": "[]"},
    ]

    request = TransferRequest(
        source=EndpointConfig(kind="file", format=fmt),
        source_filename=f"sample.{fmt}",
        source_content=content_fn(rows),
        destination=_pg_destination(table_name),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "payments",
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

    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    cur = conn.cursor()
    cur.execute(f'SELECT id, amount, note, created, active, meta, tags FROM public."{table_name}" ORDER BY id')
    rows = cur.fetchall()
    conn.close()

    def _json(x):
        return json.dumps(x, sort_keys=True) if x is not None else None

    assert len(rows) == 3
    assert rows[0][0] == 1
    assert rows[0][2] == "hello"
    assert rows[0][3] == datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    assert rows[0][4] is True
    assert rows[1][0] == 2
    assert rows[1][2] is None
    assert rows[1][3] == datetime(2024, 2, 28, 14, 30, 0, tzinfo=timezone.utc)
    assert rows[1][4] is False
    assert rows[2][0] == 3
    assert rows[2][2] == "null"
    assert rows[2][3] == datetime(2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert rows[2][4] is True
    # Postgres returns numeric columns as Decimal; float them for comparison.
    assert float(rows[0][1]) == pytest.approx(1000.0)
    assert float(rows[1][1]) == pytest.approx(2000.5)
    assert float(rows[2][1]) == pytest.approx(3.14)
    # JSON values roundtrip via psycopg2 as native dict/list/None.
    assert _json(rows[0][5]) == '{"k": "v"}'
    assert _json(rows[1][5]) is None
    assert _json(rows[2][5]) == '{}'
    assert _json(rows[0][6]) == '["a", "b"]'
    assert _json(rows[1][6]) is None
    assert _json(rows[2][6]) == '[]'
