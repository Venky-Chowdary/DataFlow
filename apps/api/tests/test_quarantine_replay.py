"""Quarantine replay API — rewrite edited rejected rows through the destination."""

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


def test_quarantine_replay_sqlite_edits_and_rewrites(tmp_path: Path):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dest_path = tmp_path / "quarantine_replay.db"
    conn = f"sqlite:///{dest_path}"

    csv = b"id,age\n1,30\n2,not-a-number\n"
    request = TransferRequest(
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
            {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"},
        ],
        column_types={"id": "string", "age": "string"},
    )
    engine = UniversalTransferEngine()
    result = engine.execute(request)
    job_id = result.job_id
    assert result.success is True
    assert int(result.destination_summary.get("rejected_rows") or 0) >= 1

    client = _client()
    q = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
    assert q.status_code == 200
    qbody = q.json()
    quarantine = qbody["quarantine"]
    assert quarantine
    dest_dlq = qbody.get("dest_dlq") or {}
    assert dest_dlq.get("table") == "users_df_quarantine"
    assert int(dest_dlq.get("rows_written") or 0) >= 1
    assert int(dest_dlq.get("open_rows") or 0) >= 1

    import sqlite3

    with sqlite3.connect(dest_path) as db:
        open_n = db.execute(
            "SELECT COUNT(*) FROM users_df_quarantine "
            "WHERE _df_promoted_at IS NULL OR _df_promoted_at = ''"
        ).fetchone()[0]
        assert open_n >= 1

    # Fix the bad age value and replay.
    edited = []
    for detail in quarantine:
        d = dict(detail)
        if str(d.get("value")) == "not-a-number":
            d["value"] = "25"
            values = dict(d.get("values") or {})
            values["age"] = "25"
            d["values"] = values
        edited.append(d)

    replay = client.post(
        f"/api/v1/connectors/jobs/{job_id}/quarantine/replay",
        json={"rows": edited},
    )
    assert replay.status_code == 200, replay.text
    body = replay.json()
    assert body["success"] is True
    assert body["job_id"]
    assert body["parent_job_id"] == job_id
    assert body["rows_written"] >= 1
    assert body["rejected"] == 0
    promote = body.get("dest_dlq_promoted") or {}
    assert int(promote.get("updated") or 0) >= 1

    with sqlite3.connect(dest_path) as db:
        open_after = db.execute(
            "SELECT COUNT(*) FROM users_df_quarantine "
            "WHERE _df_promoted_at IS NULL OR _df_promoted_at = ''"
        ).fetchone()[0]
        assert open_after == 0

    child = client.get(f"/api/v1/connectors/jobs/{body['job_id']}")
    assert child.status_code == 200
    assert child.json()["status"] in ("completed", "completed_with_quarantine")

    q2 = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
    assert q2.status_code == 200
    assert int((q2.json().get("dest_dlq") or {}).get("open_rows") or 0) == 0


def test_quarantine_replay_empty_uses_stored_details(tmp_path: Path):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dest_path = tmp_path / "quarantine_replay_all.db"
    conn = f"sqlite:///{dest_path}"

    csv = b"id,age\n1,bad\n"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            table="ages",
            connection_string=conn,
            database=str(dest_path),
        ),
        source_filename="ages.csv",
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

    client = _client()
    # Empty body.rows → replay all stored quarantine rows (still bad — expect rejected or written with null).
    replay = client.post(
        f"/api/v1/connectors/jobs/{job_id}/quarantine/replay",
        json={},
    )
    assert replay.status_code == 200, replay.text
    body = replay.json()
    assert body["success"] is True
    assert body["rows_attempted"] >= 1
    assert body["job_id"] != job_id


def test_quarantine_replay_no_details_400(tmp_path: Path):
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dest_path = tmp_path / "empty_q.db"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            table="empty_q",
            connection_string=f"sqlite:///{dest_path}",
            database=str(dest_path),
        ),
        source_filename="ok.csv",
        source_content=b"id,age\n1,10\n",
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
    mongo = get_mongodb_service()
    job = mongo.get_job(job_id)
    if job:
        mongo.update_job_status(job_id, job.get("status", "completed"), rejected_rows=0, rejected_details=[])

    client = _client()
    resp = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={"rows": []})
    assert resp.status_code == 400