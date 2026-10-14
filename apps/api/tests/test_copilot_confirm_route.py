"""POST /api/v1/copilot/confirm must run copilot_confirm.

lifecycle_confirm_succeeded used to sit under @router.post("/confirm"), so
FastAPI bound the route to the helper and every UI/MCP confirm missed the
real handler.
"""

from __future__ import annotations

from fastapi.testclient import TestClient


def _confirm_endpoint(app):
    for route in app.routes:
        original = getattr(route, "original_router", None)
        if original is None:
            continue
        prefix = getattr(getattr(route, "include_context", None), "prefix", "") or ""
        for child in getattr(original, "routes", []):
            if getattr(child, "path", "") != "/copilot/confirm":
                continue
            if "POST" not in (getattr(child, "methods", None) or set()):
                continue
            return prefix, child.endpoint
    return "", None


def test_confirm_route_is_copilot_confirm_and_rejects_a_missing_ack():
    from src.main import app

    prefix, endpoint = _confirm_endpoint(app)
    assert prefix == "/api/v1"
    assert endpoint is not None
    assert endpoint.__name__ == "copilot_confirm"

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/copilot/confirm",
            json={"ack_id": "ack_does_not_exist"},
        )

    assert response.status_code == 404
    detail = response.json().get("detail")
    assert isinstance(detail, str)
    assert "Approval not found" in detail
    assert response.json().get("ok") is not True


def test_sync_confirm_conflict_names_the_blocking_job(monkeypatch):
    from services.job_idempotency import JobClaim
    from src.ai.copilot.ack_ledger import get_ack_ledger
    from src.main import app
    from src.transfer.engine import DuplicateTransferSubmission
    import services.sync_now as sync_now

    ledger = get_ack_ledger()
    ack_id = ledger.put(
        kind="sync_now",
        payload={"mode": "sync", "parent_job_id": "parent-1", "transfer_request": {}},
        preview={"confirm_reason": "Sync now using the signed contract"},
    )
    claim = JobClaim(
        key="ws:fp",
        job_id="",
        acquired=False,
        existing_job_id="job-live",
        existing_status="running",
        existing_created_at="2026-10-14T12:00:00Z",
    )

    def boom(_payload):
        raise DuplicateTransferSubmission(claim)

    monkeypatch.setattr(sync_now, "begin_sync_now", boom)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/copilot/confirm",
            json={"ack_id": ack_id, "reason": "operator confirmed"},
        )

    body = response.json()
    assert response.status_code == 409, body
    assert body.get("error") == "duplicate_transfer"
    assert body.get("existing_job_id") == "job-live"
    assert body.get("existing_status") == "running"
    assert body.get("existing_created_at") == "2026-10-14T12:00:00Z"
    assert "Failed to start sync" not in str(body)
