"""Data integrity: every file format CSV/JSON/JSONL/TSV/Parquet -> DuckDB (warehouse surrogate)."""

from __future__ import annotations

import csv
import io
import json
import sys
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
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


def _to_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _to_tsv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLUMNS, delimiter="\t")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _to_json(rows: list[dict]) -> bytes:
    return json.dumps(rows).encode("utf-8")


def _to_jsonl(rows: list[dict]) -> bytes:
    return b"\n".join(json.dumps(r).encode("utf-8") for r in rows)


def _to_parquet(rows: list[dict]) -> bytes:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df), buf)
    return buf.getvalue()


_FORMATS = {
    "csv": ("sample.csv", _to_csv),
    "tsv": ("sample.tsv", _to_tsv),
    "json": ("sample.json", _to_json),
    "jsonl": ("sample.jsonl", _to_jsonl),
    "parquet": ("sample.parquet", _to_parquet),
}


@pytest.mark.parametrize("fmt", ["csv", "tsv", "json", "jsonl", "parquet"])
def test_file_to_duckdb_preserves_types(fmt: str):
    pytest.importorskip("duckdb")

    filename, content_fn = _FORMATS[fmt]
    table_name = f"f2d_{fmt}_{uuid.uuid4().hex[:8]}"
    path = f"/tmp/{table_name}.duck"

    request = TransferRequest(
        source=EndpointConfig(kind="file", format=fmt),
        source_filename=filename,
        source_content=content_fn(ROWS),
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
        rows = conn.execute(f'SELECT id, amount, note, created, active, meta, tags FROM "{table_name}" ORDER BY id').fetchall()
        conn.close()

        # Normalized values: decimals as float, dates/timestamps as datetime, bools as bool, empty JSON as None
        assert len(rows) == 3
        assert rows[0] == (
            1,
            pytest.approx(1000.0),
            "hello",
            datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc),
            True,
            '{"k":"v"}',
            '["a","b"]',
        )
        assert rows[1] == (
            2,
            pytest.approx(2000.5),
            None,
            datetime(2024, 2, 28, 14, 30, 0, tzinfo=timezone.utc),
            False,
            None,
            None,
        )
        assert rows[2] == (
            3,
            pytest.approx(3.14),
            "null",
            datetime(2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc),
            True,
            '{}',
            '[]',
        )
    finally:
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass
