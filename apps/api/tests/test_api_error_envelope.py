"""P0-5 — {error:{code,message,retry_after_sec}} envelope on new auth routes."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.api_errors import ApiError, api_error_handler


def _app() -> FastAPI:
    app = FastAPI()

    @app.exception_handler(ApiError)
    async def _handler(request, exc):
        return await api_error_handler(request, exc)

    @app.post("/boom")
    async def boom():
        raise ApiError(429, "email_quota_exceeded", "try later", retry_after_sec=42)

    @app.post("/denied")
    async def denied():
        raise ApiError(403, "email_unverified", "verify first",
                       extra={"challenge_id": "otc_x"})

    return app


def test_envelope_schema_with_retry_after():
    resp = TestClient(_app()).post("/boom")
    assert resp.status_code == 429
    assert resp.json() == {
        "error": {"code": "email_quota_exceeded", "message": "try later",
                  "retry_after_sec": 42}
    }
    assert resp.headers["Retry-After"] == "42"


def test_envelope_extra_fields():
    resp = TestClient(_app()).post("/denied")
    assert resp.status_code == 403
    body = resp.json()["error"]
    assert body["code"] == "email_unverified"
    assert body["challenge_id"] == "otc_x"
    assert "Retry-After" not in resp.headers


def test_envelope_in_main_app():
    """The handler is registered on the real app."""
    from src.main import app

    assert ApiError in app.exception_handlers
