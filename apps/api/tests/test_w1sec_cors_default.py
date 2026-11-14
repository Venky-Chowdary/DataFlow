"""SEC-16: with sign-in required, no localhost origin is trusted by default."""

from __future__ import annotations


def _clear(monkeypatch):
    for name in ("CORS_ORIGINS", "CORS_EXTRA_ORIGINS", "RAILWAY_PUBLIC_DOMAIN", "DATAFLOW_WEB_DOMAIN",
                 "DATAWRAP_WEB_DOMAIN", "RAILWAY_ENVIRONMENT", "RAILWAY_SERVICE_ID", "RAILWAY_PROJECT_ID",
                 "DATAWRAP_REQUIRE_AUTH"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAFLOW_ENV", "development")


def test_auth_required_without_cors_origins_trusts_no_localhost(monkeypatch):
    from services.platform_config import cors_origins

    _clear(monkeypatch)
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    assert not [o for o in cors_origins() if "localhost" in o or "127.0.0.1" in o]
    monkeypatch.setenv("CORS_ORIGINS", "https://app.example.com")
    assert cors_origins() == ["https://app.example.com"]


def test_local_install_with_sign_in_off_keeps_the_dev_origins(monkeypatch):
    from services.platform_config import cors_origins

    _clear(monkeypatch)
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    assert "http://localhost:5173" in cors_origins()
