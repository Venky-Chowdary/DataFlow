"""CSV/SQL → S3 export safety tests using moto (no real AWS needed)."""

from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

try:
    import moto  # noqa: E402
except ImportError as exc:
    pytest.skip(f"requires a working optional moto dependency: {exc}", allow_module_level=True)

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _csv_bytes(rows: int) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "amount"])
    writer.writeheader()
    for i in range(1, rows + 1):
        writer.writerow({"id": i, "amount": f"{i}.00"})
    return buf.getvalue().encode("utf-8")


def test_csv_to_s3_large_export_does_not_overwrite_batches():
    """Regression: object-store writers used to overwrite the same key per batch."""
    with moto.mock_aws():
        key = "export_large.json"
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            source_filename="payments.csv",
            source_content=_csv_bytes(21_000),
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

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, "test-s3-large-0001")
        assert result.success is True, result.error
        assert result.records_transferred == 21_000

        import boto3

        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="dataflow", Key=key)
        data = json.loads(obj["Body"].read())
        assert len(data) == 21_000
        ids = {row["id"] for row in data}
        assert ids == set(range(1, 21_001))
