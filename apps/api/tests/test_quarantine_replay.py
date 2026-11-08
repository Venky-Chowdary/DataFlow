"""Quarantine replay API — rewrite edited rejected rows through the destination."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

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
    assert qbody["replay_block_reason"] is None, qbody
    dest_dlq = qbody.get("dest_dlq") or {}
    assert dest_dlq.get("table") == "users_df_quarantine"
    assert int(dest_dlq.get("rows_written") or 0) >= 1
    assert int(dest_dlq.get("open_rows") or 0) >= 1

    from services.mongodb_service import get_mongodb_service

    store = get_mongodb_service()
    assert store.update_job_fields(job_id, {
        "rejected_details_truncated": True,
        "rejected_details_total": len(quarantine) + 1,
    })
    incomplete = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
    assert incomplete.status_code == 200, incomplete.text
    assert "incomplete" in incomplete.json()["replay_block_reason"].lower()
    assert store.update_job_fields(job_id, {
        "rejected_details_truncated": False,
        "rejected_details_total": len(quarantine),
    })

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
    q2body = q2.json()
    assert int((q2body.get("dest_dlq") or {}).get("open_rows") or 0) == 0
    closure = q2body.get("quarantine_closure") or {}
    assert closure.get("verdict") == "closed"
    assert closure.get("open_count") == 0
    assert closure.get("migration_proven") is False
    assert q2body.get("open_count") == 0

    again = client.post(
        f"/api/v1/connectors/jobs/{job_id}/quarantine/replay",
        json={"rows": edited},
    )
    assert again.status_code == 400
    assert "closed" in again.json()["detail"].lower()


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
    # Empty body replays the stored quarantine row. The value is still not an
    # integer, so nothing lands. A replay that writes 0 rows is not a success
    # (QA R2-061) — the attempt is still recorded.
    assert body["rows_attempted"] >= 1
    assert body["job_id"] != job_id
    assert body["success"] is False
    assert int(body.get("rows_written") or 0) == 0


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


@pytest.mark.parametrize(
    "replay_proof,keep_reject",
    [
        pytest.param(None, False, id="verified"),
        pytest.param({}, False, id="missing_proof"),
        pytest.param({"passed": False}, False, id="failed_proof"),
        pytest.param({"passed": 1}, False, id="numeric_truthy_proof"),
        pytest.param({"passed": "false"}, False, id="string_truthy_proof"),
        pytest.param({}, True, id="missing_proof_with_reject"),
        pytest.param({"passed": False}, True, id="failed_proof_with_reject"),
    ],
)
def test_quarantine_corrected_replay_sqlite_preserves_committed_rows_and_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replay_proof, keep_reject: bool,
):
    import copy
    import sqlite3

    from services.migration_risk_contract import sign_risk_contract
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dest_path = tmp_path / "corrected_replay_lifecycle.db"
    contract = {
        "risk_id": "cast_failure",
        "severity": "high",
        "root_cause": "cast_failure",
        "column": "qty",
        "target": "qty",
        "source_type": "string",
        "destination_type": "INTEGER",
        "transform": None,
        "rows_sampled": 5,
        "estimated_rows": 5,
        "expected_failure_pct": 0.4,
        "expected_precision_loss": False,
        "expected_truncation": False,
        "expected_nulls": True,
        "execution_policy": "QUARANTINE_ROW",
        "quarantine_policy": "DLQ",
        "retry_policy": "NONE",
        "rollback_strategy": "DOCUMENT_ONLY",
        "approved_by": "admin@dataflow.app",
        "approved_at": "2026-11-08T00:00:00Z",
        "reason": "Hold invalid integer quantities for corrected replay",
    }
    contract["signature"] = sign_risk_contract(contract)
    mappings = [
        {"source": "id", "target": "id", "target_type": "INTEGER", "confidence": 0.95},
        {
            "source": "qty", "target": "qty", "target_type": "INTEGER",
            "source_type": "string", "destination_type": "INTEGER",
            "confidence": 0.95, "risk_contract": contract,
        },
    ]
    with sqlite3.connect(dest_path) as db:
        db.execute("CREATE TABLE flights (id INTEGER, qty INTEGER)")
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database", format="sqlite", table="flights",
            connection_string=f"sqlite:///{dest_path}", database=str(dest_path),
        ),
        source_filename="flights.csv",
        source_content=b"id,qty\n1,15\n2,invalid-qty\n3,25\n4,not-an-integer\n5,35\n",
        sync_mode="full_refresh_overwrite",
        skip_preflight=False,
        validation_mode="strict",
        stream_contracts=[{"primary_key": "id"}],
        mappings=mappings,
        column_types={"id": "INTEGER", "qty": "string"},
    )
    result = UniversalTransferEngine().execute(request)
    assert result.success is True, (result.error, result.destination_summary)
    assert result.records_transferred == 3, result.destination_summary
    assert result.destination_summary.get("rejected_rows") == 2, result.destination_summary

    store = get_mongodb_service()
    parent = copy.deepcopy(store.get_job(result.job_id))
    assert parent["status"] == "completed_with_quarantine", parent
    assert parent["transfer_request"]["skip_preflight"] is False
    assert parent.get("reconciliation"), parent
    assert parent.get("message"), parent
    assert parent["destination_summary"].get("checksum"), parent["destination_summary"]
    history = {
        key: copy.deepcopy(parent.get(key))
        for key in ("status", "message", "reconciliation", "transfer_request", "records_processed", "rejected_rows")
    }
    checksum = parent["destination_summary"]["checksum"]
    good_rows = [(1, 15), (3, 25), (5, 35)]
    with sqlite3.connect(dest_path) as db:
        assert db.execute("SELECT id, qty FROM flights ORDER BY id").fetchall() == good_rows

    client = _client()
    quarantine_response = client.get(f"/api/v1/connectors/jobs/{result.job_id}/quarantine")
    assert quarantine_response.status_code == 200, quarantine_response.text
    quarantine_body = quarantine_response.json()
    assert quarantine_body["quarantine_closure"]["verdict"] == "open", quarantine_body
    assert quarantine_body["quarantine_closure"]["open_count"] == 2, quarantine_body
    details = quarantine_body["quarantine"]
    assert len(details) == 2, details
    original_payloads = {
        str(detail["source_values"]["id"]): copy.deepcopy(detail["source_values"])
        for detail in details
    }
    assert original_payloads == {
        "2": {"id": "2", "qty": "invalid-qty"},
        "4": {"id": "4", "qty": "not-an-integer"},
    }
    corrections = {"2": "20", "4": "30"}
    if keep_reject:
        corrections["4"] = "not-an-integer"
    edited = [
        {**payload, "qty": corrections[key]}
        for key, payload in original_payloads.items()
    ]
    if replay_proof is not None:
        monkeypatch.setattr(
            "src.transfer.reconcile_step.run_reconciliation",
            lambda **kwargs: copy.deepcopy(replay_proof),
        )
    replay = client.post(
        f"/api/v1/connectors/jobs/{result.job_id}/quarantine/replay",
        json={"rows": edited},
    )
    assert replay.status_code == 200, replay.text
    body = replay.json()
    assert body["parent_job_id"] == result.job_id, body
    assert body["job_id"] != result.job_id, body
    if replay_proof is not None:
        assert body["rows_attempted"] == 2, body
        assert body["rows_written"] == (1 if keep_reject else 2), body
        assert body["rejected"] == (1 if keep_reject else 0), body
        assert body["reconciliation"] == replay_proof, body
        assert int(body["dest_dlq_promoted"].get("updated") or 0) == 0, body
        assert body["quarantine_closure"]["verdict"] != "closed", body
        assert body["quarantine_closure"]["open_count"] == 2, body
        assert body["quarantine_closure"]["promoted_count"] == 0, body
        with sqlite3.connect(dest_path) as db:
            expected_rows = [good_rows[0], (2, 20), good_rows[1]]
            if not keep_reject:
                expected_rows.append((4, 30))
            expected_rows.append(good_rows[2])
            assert db.execute("SELECT id, qty FROM flights ORDER BY id").fetchall() == expected_rows
            assert db.execute(
                "SELECT COUNT(*) FROM flights_df_quarantine "
                "WHERE _df_promoted_at IS NULL OR _df_promoted_at = ''"
            ).fetchone()[0] == 2
        after = store.get_job(result.job_id)
        assert {key: after.get(key) for key in history} == history
        assert after["destination_summary"]["checksum"] == checksum
        assert {
            str(detail["source_values"]["id"]): detail["source_values"]
            for detail in after["rejected_details"]
        } == original_payloads
        open_response = client.get(f"/api/v1/connectors/jobs/{result.job_id}/quarantine")
        assert open_response.status_code == 200, open_response.text
        opened = open_response.json()
        assert opened["quarantine_closure"]["verdict"] != "closed", opened
        assert opened["quarantine_closure"]["promoted_count"] == 0, opened
        assert opened["open_count"] == 2, opened
        assert body["success"] is False and body["status"] == "failed", body
        child_response = client.get(f"/api/v1/connectors/jobs/{body['job_id']}")
        assert child_response.status_code == 200, child_response.text
        child = child_response.json()
        assert child["retry_of"] == result.job_id, child
        assert child["status"] == "failed", child
        assert child["reconciliation"] == replay_proof, child
        return
    assert body["rows_attempted"] == body["rows_written"] == 2, body
    assert body["rejected"] == 0, body
    assert body["success"] is True and body["status"] == "completed", body
    assert body["reconciliation"]["passed"] is True, body
    assert body["quarantine_closure"]["verdict"] == "closed", body
    assert body["quarantine_closure"]["open_count"] == 0, body
    assert body["quarantine_closure"]["migration_proven"] is False, body
    assert body["dest_dlq_promoted"]["updated"] == 2, body

    with sqlite3.connect(dest_path) as db:
        assert db.execute("SELECT id, qty FROM flights ORDER BY id").fetchall() == [
            good_rows[0], (2, 20), good_rows[1],
            (4, 30), good_rows[2],
        ]
        assert db.execute(
            "SELECT COUNT(*) FROM flights_df_quarantine "
            "WHERE _df_promoted_at IS NULL OR _df_promoted_at = ''"
        ).fetchone()[0] == 0

    child_response = client.get(f"/api/v1/connectors/jobs/{body['job_id']}")
    assert child_response.status_code == 200, child_response.text
    child = child_response.json()
    assert child["retry_of"] == result.job_id, child
    assert child["status"] == "completed", child
    assert child["reconciliation"]["passed"] is True, child
    assert child["reconciliation"] == body["reconciliation"], child

    after = store.get_job(result.job_id)
    assert {key: after.get(key) for key in history} == history
    assert after["destination_summary"]["checksum"] == checksum
    closed_response = client.get(f"/api/v1/connectors/jobs/{result.job_id}/quarantine")
    assert closed_response.status_code == 200, closed_response.text
    closed = closed_response.json()
    assert closed["quarantine_closure"]["verdict"] == "closed", closed
    assert closed["open_count"] == 0, closed
    assert closed["quarantine_closure"]["migration_proven"] is False, closed
    assert {
        str(detail["source_values"]["id"]): detail["source_values"]
        for detail in after["rejected_details"]
    } == original_payloads