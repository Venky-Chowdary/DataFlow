"""End-to-end GCS (fake-gcs-server) → PostgreSQL streaming upsert."""

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


def test_gcs_to_postgresql_upsert():
    try:
        with socket.create_connection(("localhost", 4443), timeout=1):
            pass
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("GCS fake server or PostgreSQL emulator not reachable")

    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    client = storage.Client(
        project="dataflow-test",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint="http://localhost:4443"),
    )
    bucket = client.bucket("dataflow-test")
    key = f"payments_gcs_to_pg_{uuid.uuid4().hex[:8]}.json"
    bucket.blob(key).upload_from_string(json.dumps([
        {"id": 1, "amount": "1000.00"},
        {"id": 2, "amount": "2000.50"},
    ]))

    dst_table = f"dst_gcs_pg_{uuid.uuid4().hex[:8]}"
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="gcs",
            host="localhost",
            port=4443,
            database="dataflow-test",
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
