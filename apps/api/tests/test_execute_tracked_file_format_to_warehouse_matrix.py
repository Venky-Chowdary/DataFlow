"""File formats (JSON/JSONL/Parquet) → BigQuery and Snowflake end-to-end."""

from __future__ import annotations

import csv
import io
import json
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

COLUMNS = ["id", "amount", "note", "created", "active", "meta", "tags"]
ROWS = [
    {"id": 1, "amount": "1000.00", "note": "hello", "created": "2024-01-15T00:00:00", "active": "true", "meta": '{"k":"v"}', "tags": '["a","b"]'},
    {"id": 2, "amount": "2000.50", "note": "", "created": "2024-02-28T14:30:00", "active": "false", "meta": "", "tags": ""},
    {"id": 3, "amount": "3.14", "note": "null", "created": "2024-03-01T00:00:00", "active": "1", "meta": "{}", "tags": "[]"},
]


def _json_bytes(rows: list[dict]) -> bytes:
    return json.dumps(rows).encode("utf-8")


def _jsonl_bytes(rows: list[dict]) -> bytes:
    return b"\n".join(json.dumps(r).encode("utf-8") for r in rows)


def _parquet_bytes(rows: list[dict]) -> bytes:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df), buf)
    return buf.getvalue()


_FORMATS = {
    "json": ("sample.json", _json_bytes, 3),
    "jsonl": ("sample.jsonl", _jsonl_bytes, 3),
    "parquet": ("sample.parquet", _parquet_bytes, 3),
}


def _bigquery_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="bigquery",
        host="localhost",
        port=9050,
        connection_string="http://localhost:9050",
        database="dataflow-test",
        schema="dataflow",
        table=table,
    )


def _snowflake_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="snowflake",
        host="localhost",
        port=443,
        database="dataflow",
        username="test",
        password="test",
        schema="public",
        table=table,
    )


@pytest.mark.parametrize("fmt,destination", [
    ("json", "bigquery"),
    ("jsonl", "bigquery"),
    ("parquet", "bigquery"),
    ("json", "snowflake"),
    ("jsonl", "snowflake"),
    ("parquet", "snowflake"),
])
def test_file_format_to_warehouse(fmt: str, destination: str):
    if destination == "bigquery":
        try:
            with socket.create_connection(("localhost", 9050), timeout=1):
                pass
        except OSError as exc:
            pytest.skip(f"Emulator not reachable: {exc}")
    else:
        pytest.importorskip("fakesnow")

    filename, content_fn, expected = _FORMATS[fmt]
    if fmt == "parquet":
        content_fn(ROWS)  # trigger importorskip inside helper

    table_name = f"{fmt}_to_{destination}_{uuid.uuid4().hex[:8]}"
    dest = _bigquery_endpoint(table_name) if destination == "bigquery" else _snowflake_endpoint(table_name)

    request = TransferRequest(
        source=EndpointConfig(kind="file", format=fmt),
        source_filename=filename,
        source_content=content_fn(ROWS),
        destination=dest,
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
    assert result.records_transferred == expected
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")
