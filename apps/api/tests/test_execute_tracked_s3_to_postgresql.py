"""End-to-end S3 (MinIO) → PostgreSQL streaming transfer."""

from __future__ import annotations

import json
import socket
import sys
import uuid
from decimal import Decimal
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


def test_s3_minio_to_postgresql_upsert():
    try:
        with socket.create_connection(("localhost", 9000), timeout=1):
            pass
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("MinIO or PostgreSQL emulator not reachable")

    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="dataflow",
        aws_secret_access_key="dataflowsecret",
        region_name="us-east-1",
    )
    bucket = "dataflow"
    key = f"payments_s3_to_pg_{uuid.uuid4().hex[:8]}.json"
    body = json.dumps([
        {"id": 1, "amount": "1000.00"},
        {"id": 2, "amount": "2000.50"},
    ]).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body)

    dst_table = f"dst_s3_pg_{uuid.uuid4().hex[:8]}"
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="s3",
            host="localhost",
            port=9000,
            database=bucket,
            username="dataflow",
            password="dataflowsecret",
            table=key,
        ),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=dst_table,
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

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2

    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM public."{dst_table}"')
        assert cur.fetchone()[0] == 2
        cur.execute(f'SELECT id, amount FROM public."{dst_table}" ORDER BY id')
        rows = cur.fetchall()
    conn.close()
    assert rows == [(1, Decimal("1000.00")), (2, Decimal("2000.50"))]
