"""End-to-end Elasticsearch → PostgreSQL streaming upsert."""

from __future__ import annotations

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


def test_elasticsearch_to_postgresql_upsert():
    try:
        with socket.create_connection(("localhost", 9200), timeout=1):
            pass
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("Elasticsearch or PostgreSQL emulator not reachable")

    elasticsearch = pytest.importorskip("elasticsearch")
    es = elasticsearch.Elasticsearch("http://localhost:9200")
    index = f"payments_es_to_pg_{uuid.uuid4().hex[:8]}"
    if es.indices.exists(index=index):
        es.indices.delete(index=index)
    es.indices.create(index=index)
    for i in range(1, 3):
        es.index(index=index, id=str(i), body={"id": i, "amount": f"{i}00.50"})
    es.indices.refresh(index=index)

    dst_table = f"dst_es_pg_{uuid.uuid4().hex[:8]}"
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="elasticsearch",
            host="localhost",
            port=9200,
            database=index,
            table="",
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
    assert rows == [(1, Decimal("100.50")), (2, Decimal("200.50"))]
