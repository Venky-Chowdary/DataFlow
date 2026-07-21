"""End-to-end CSV → Snowflake transfer with edge-case types."""

from __future__ import annotations

import base64
import csv
import io
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


class _FakeMongo:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    fake_mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake_mongo)


def test_edge_types_csv_to_snowflake():
    fakesnow = pytest.importorskip("fakesnow")

    rows = [
        {
            "row_id": "1",
            "amount": "1500.50",
            "record_uuid": "550e8400-e29b-41d4-a716-446655440000",
            "is_active": "true",
            "created_at": "2024-01-15T10:30:00Z",
            "metadata_json": '{"status":"active","tier":"gold"}',
            "txn_yyyymmdd": "2024-01-15",
            "birth_date": "1990-05-20",
            "updated_epoch_ms": "2024-01-15T09:50:00Z",
            "customer_email": "alice@example.com",
            "narrative_body": (
                "This is a long narrative description field that exceeds two hundred "
                "fifty five characters in length so the schema inference engine must "
                "classify it as TEXT rather than VARCHAR for proper warehouse DDL generation."
            ),
            "payload_b64": base64.b64encode(b"Hello World").decode("ascii"),
        },
        {
            "row_id": "2",
            "amount": "275.00",
            "record_uuid": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "is_active": "false",
            "created_at": "2024-02-01T14:22:33Z",
            "metadata_json": '{"status":"inactive"}',
            "txn_yyyymmdd": "2024-02-01",
            "birth_date": "1985-11-03",
            "updated_epoch_ms": "2024-02-01T12:00:00Z",
            "customer_email": "user@corp.io",
            "narrative_body": (
                "Another extended memo field with sufficient length to trigger TEXT detection "
                "in the inference pipeline when samples are analyzed for blob stories and long sequences."
            ),
            "payload_b64": base64.b64encode(b"Hello Base64").decode("ascii"),
        },
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

    table = f"edge_sf_{uuid.uuid4().hex[:8]}"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="edge.csv",
        source_content=buf.getvalue().encode("utf-8"),
        destination=EndpointConfig(
            kind="database",
            format="snowflake",
            host="localhost",
            database="dataflow",
            schema="public",
            username="user",
            password="pass",
            warehouse="dataflow",
            table=table,
        ),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "edge",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "row_id",
            "selected": True,
        }],
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")
