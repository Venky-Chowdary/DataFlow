"""An object export must carry the source type, not stringify every column.

S3/GCS/ADLS/SFTP have no DDL, so type invent used to fall through to the TEXT
default and every integer, decimal and date landed as a quoted string — in JSON
and, worse, as a string column in Parquet, which Athena/Spark then read as text.
"""

from __future__ import annotations

import csv
import io
import json
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

moto = pytest.importorskip("moto")

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _csv_bytes() -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "amount", "ordered_at", "active"])
    writer.writeheader()
    for i in range(1, 6):
        writer.writerow(
            {
                "id": i,
                "amount": f"{i * 1000}.25",
                "ordered_at": f"2024-01-0{i}",
                "active": "true",
            }
        )
    return buf.getvalue().encode()


def _run(key: str) -> None:
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="payments.csv",
        source_content=_csv_bytes(),
        destination=EndpointConfig(
            kind="database",
            format="s3",
            host="s3.amazonaws.com",
            port=443,
            database="dataflow",
            username="mock",
            password="mock",
            table=key,
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 5


def test_json_export_keeps_numbers_as_numbers():
    with moto.mock_aws():
        import boto3

        key = "typed_export.json"
        _run(key)
        s3 = boto3.client("s3", region_name="us-east-1")
        rows = json.loads(s3.get_object(Bucket="dataflow", Key=key)["Body"].read())

    assert [r["id"] for r in rows] == [1, 2, 3, 4, 5]
    assert all(isinstance(r["id"], int) and not isinstance(r["id"], bool) for r in rows)
    assert all(isinstance(r["amount"], (int, float)) for r in rows)
    assert rows[0]["amount"] == pytest.approx(1000.25)
    assert rows[0]["ordered_at"] == "2024-01-01"


def test_parquet_export_keeps_typed_columns():
    pq = pytest.importorskip("pyarrow.parquet")

    with moto.mock_aws():
        import boto3

        key = "typed_export.parquet"
        _run(key)
        s3 = boto3.client("s3", region_name="us-east-1")
        body = s3.get_object(Bucket="dataflow", Key=key)["Body"].read()

    schema = pq.read_schema(io.BytesIO(body))
    assert str(schema.field("id").type).startswith("int")
    assert str(schema.field("amount").type).startswith("decimal")
    assert str(schema.field("ordered_at").type).startswith("date")
