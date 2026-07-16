"""Quarantine API and rejected-details persistence tests."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient


def _client():
    from src.main import app
    return TestClient(app)


def test_quarantine_details_in_write_result():
    from connectors.writer_common import build_mapped_rows_with_details

    headers = ["id", "age"]
    data_rows = [["1", "30"], ["2", "not-a-number"]]
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.95},
        {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"},
    ]
    mapped, errors, details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=["id", "age"],
        column_types={"id": "string", "age": "string"},
        dest_types={"id": "string", "age": "integer"},
        error_policy="quarantine",
    )
    assert len(mapped) == 2
    assert any(d["value"] == "not-a-number" and "age" in (d["column"], d["target"]) for d in details)


def test_job_quarantine_endpoint():
    from services import connector_store
    from src.transfer.models import EndpointConfig, TransferRequest
    from src.transfer.engine import UniversalTransferEngine

    # Create a tiny CSV that fails integer coercion for one row.
    csv = b"id,age\n1,30\n2,not-a-number\n"
    dest_path = Path(_API_ROOT) / "exports" / "quarantine_test.db"
    try:
        connector_store.create_connector({
            "name": "Quarantine SQLite",
            "type": "sqlite",
            "role": "destination",
            "connection_string": f"sqlite:///{dest_path}",
            "workspace_id": "",
        })
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(kind="database", format="sqlite", table="users"),
            source_filename="users.csv",
            source_content=csv,
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.95},
                {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"},
            ],
        )
        engine = UniversalTransferEngine()
        result = engine.execute(request)
        job_id = result.job_id
        assert result.success is True
        assert result.destination_summary.get("rejected_rows", 0) >= 1

        client = _client()
        resp = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
        if resp.status_code != 200:
            print("QUARANTINE RESP", resp.status_code, resp.text)
        assert resp.status_code == 200
        data = resp.json()
        assert data["rejected_rows"] >= 1
        assert any("not-a-number" in str(q.get("value", "")) for q in data["quarantine"])

        export_resp = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/export")
        assert export_resp.status_code == 200
        export = export_resp.json()
        assert export["success"] is True
        assert export["row_count"] >= 1
        assert export["download_url"]
    finally:
        if dest_path.exists():
            dest_path.unlink()
