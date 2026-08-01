"""Quarantine replay must run Gate-8 and refuse incomplete scraps."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import HTTPException

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_refuse_incomplete_quarantine_replay():
    from src.routers.connectors_router import _refuse_incomplete_quarantine_replay

    mappings = [
        {"source": "id", "target": "id"},
        {"source": "age", "target": "age"},
        {"source": "name", "target": "name"},
    ]
    with pytest.raises(HTTPException) as ei:
        _refuse_incomplete_quarantine_replay([{"age": "25"}], mappings)
    assert ei.value.status_code == 400
    assert "incomplete" in ei.value.detail.lower()

    # Full mapped keys present — ok even if some values are empty strings.
    _refuse_incomplete_quarantine_replay(
        [{"id": "1", "age": "25", "name": ""}],
        mappings,
    )


def test_refuse_incomplete_accepts_target_shaped_values():
    """Write-matrix quarantine stamps target keys when source≠target."""
    from src.routers.connectors_router import (
        _canonicalize_quarantine_records_to_source,
        _refuse_incomplete_quarantine_replay,
    )

    mappings = [
        {"source": "user_id", "target": "id"},
        {"source": "years", "target": "age"},
        {"source": "full_name", "target": "name"},
    ]
    target_shaped = [{"id": "1", "age": "25", "name": "Ada"}]
    _refuse_incomplete_quarantine_replay(target_shaped, mappings)
    records, columns = _canonicalize_quarantine_records_to_source(
        target_shaped, mappings
    )
    assert records[0]["user_id"] == "1"
    assert records[0]["years"] == "25"
    assert records[0]["full_name"] == "Ada"
    assert "user_id" in columns


def test_quarantine_replay_persists_gate8_reconciliation(tmp_path: Path):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from fastapi.testclient import TestClient
    from src.main import app

    dest_path = tmp_path / "gate8_replay.db"
    conn = f"sqlite:///{dest_path}"
    csv = b"id,age\n1,30\n2,not-a-number\n"
    result = UniversalTransferEngine().execute(
        TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                table="users",
                connection_string=conn,
                database=str(dest_path),
            ),
            source_filename="users.csv",
            source_content=csv,
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.95},
                {
                    "source": "age",
                    "target": "age",
                    "confidence": 0.95,
                    "target_type": "integer",
                },
            ],
            column_types={"id": "string", "age": "string"},
        )
    )
    job_id = result.job_id
    assert int(result.destination_summary.get("rejected_rows") or 0) >= 1

    client = TestClient(app)
    q = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine").json()
    edited = []
    for detail in q["quarantine"]:
        d = dict(detail)
        if str(d.get("value")) == "not-a-number":
            d["value"] = "25"
            values = dict(d.get("values") or {})
            values["age"] = "25"
            d["values"] = values
        edited.append(d)

    with patch(
        "src.transfer.reconcile_step.run_reconciliation",
        return_value={
            "passed": True,
            "phase": "post_write_verified",
            "message": "Gate-8 verified (test)",
            "source_rows": 1,
            "target_rows": 2,
        },
    ) as mock_recon:
        replay = client.post(
            f"/api/v1/connectors/jobs/{job_id}/quarantine/replay",
            json={"rows": edited},
        )
    assert replay.status_code == 200, replay.text
    body = replay.json()
    assert body["success"] is True
    assert body.get("reconciliation", {}).get("passed") is True
    assert mock_recon.called
    # Child job stores reconciliation for Studio / trust score.
    child = client.get(f"/api/v1/connectors/jobs/{body['job_id']}").json()
    assert (child.get("reconciliation") or {}).get("passed") is True
