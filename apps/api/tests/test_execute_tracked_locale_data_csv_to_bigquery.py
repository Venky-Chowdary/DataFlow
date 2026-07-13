"""End-to-end CSV → BigQuery transfer with locale/currency formats."""

from __future__ import annotations

import csv
import io
import socket
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


def test_locale_data_csv_to_bigquery():
    try:
        with socket.create_connection(("localhost", 9050), timeout=1):
            pass
    except OSError:
        pytest.skip("bigquery-emulator not reachable")

    rows = [
        {"id": "1", "amount": "$1,000.00", "currency": "USD"},
        {"id": "2", "amount": "€2.000,50", "currency": "EUR"},
        {"id": "3", "amount": "1 000 000,89", "currency": "GBP"},
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "amount", "currency"])
    writer.writeheader()
    writer.writerows(rows)

    table = f"locale_bq_{uuid.uuid4().hex[:8]}"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="locale.csv",
        source_content=buf.getvalue().encode("utf-8"),
        destination=EndpointConfig(
            kind="database",
            format="bigquery",
            host="localhost",
            port=9050,
            connection_string="http://localhost:9050",
            database="dataflow-test",
            schema="dataflow",
            table=table,
        ),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "locale",
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
