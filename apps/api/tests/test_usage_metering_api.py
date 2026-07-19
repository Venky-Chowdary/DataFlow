"""Tests for GET /api/v1/usage/summary."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest
from fastapi.testclient import TestClient

from services import usage_metering as um
from src.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(um, "STORE_PATH", tmp_path / "usage_metering.json")
    monkeypatch.setattr(um, "_mongo", lambda: None)
    with TestClient(app) as c:
        yield c


def test_usage_summary_endpoint_returns_daily_and_totals(client, monkeypatch):
    now = datetime.now(timezone.utc)
    # Inject two events on different days.
    monkeypatch.setattr(um, "_now", lambda: (now - timedelta(days=1)).isoformat())
    um.record_transfer_usage(job_id="j1", rows_written=10, bytes_processed=100)
    monkeypatch.setattr(um, "_now", lambda: now.isoformat())
    um.record_transfer_usage(job_id="j2", rows_written=5, bytes_processed=50)

    response = client.get("/api/v1/usage/summary?days=30")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["rows_written"] == 15
    assert body["bytes_processed"] == 150
    assert body["totals"]["rows_written"] == 15
    assert isinstance(body["daily"], list)
    assert len(body["daily"]) == 30
    days_with_rows = [d for d in body["daily"] if d["rows_written"] > 0]
    assert len(days_with_rows) >= 1
