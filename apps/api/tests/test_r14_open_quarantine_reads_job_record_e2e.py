"""QA browser 4: "Open quarantine" shows the rows the job record counts.

A run wrote the good rows and held 2 out; the job page said "no quarantined
rows" and "Open quarantine" did nothing. The web half (one count reader, one
scroll target) is ``apps/web/src/lib/quarantineTarget.test.ts``. This half pins
the contract the page relies on, on live PostgreSQL: the job record's count,
the row-conservation ledger and the quarantine view (the route Open quarantine
loads) agree, row for row, with a reason on each held-out row.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.test_d205_quarantine_append_into_populated_e2e import _append, pg_route  # noqa: F401
from tests.helpers.live_env import pg_up

pytestmark = pytest.mark.skipif(not pg_up(), reason="PostgreSQL not reachable")


def test_quarantine_view_returns_every_row_the_job_record_counts(pg_route):  # noqa: F811
    from services.mongodb_service import get_mongodb_service
    from src.main import app

    job_id, job = _append(pg_route["name"], pg_route["src"], pg_route["name"], pg_route["dst"])
    assert job.get("status") == "completed_with_quarantine", job
    record = get_mongodb_service().get_job(job_id) or {}
    ledger = record.get("row_accounting") or {}
    held = int(record.get("rejected_rows") or 0)
    assert held == 2
    assert int(ledger.get("rows_quarantined") or 0) == held
    assert int((record.get("destination_summary") or {}).get("rejected_rows") or 0) == held

    with TestClient(app) as client:
        response = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
    assert response.status_code == 200, response.text
    body = response.json()
    rows = body.get("quarantine") or []
    assert int(body.get("rejected_rows") or 0) == held == int(body.get("open_count") or 0), body
    assert len(rows) == held, body
    for row in rows:
        assert str(row.get("reason") or row.get("error") or "").strip(), row
