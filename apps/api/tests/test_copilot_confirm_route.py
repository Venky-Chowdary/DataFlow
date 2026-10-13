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
