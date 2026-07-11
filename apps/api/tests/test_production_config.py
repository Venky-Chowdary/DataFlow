"""Tests for production platform services."""

import os

import pytest


def test_secret_vault_roundtrip(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "test-production-key-for-unit-tests-only!!")
    from services.secret_vault import decrypt_secret, encrypt_secret

    plain = "super-secret-password"
    enc = encrypt_secret(plain)
    assert enc.startswith("enc:v1:")
    assert decrypt_secret(enc) == plain


def test_production_validation_fails_without_secrets(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.delenv("DATAFLOW_AUTH_SECRET", raising=False)
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    from services.platform_config import validate_production_config

    errors = validate_production_config()
    assert any("DATAFLOW_AUTH_SECRET" in e for e in errors)
    assert any("DATAFLOW_REQUIRE_AUTH" in e for e in errors)


def test_mongodb_uri_from_railway_mongo_url(monkeypatch):
    monkeypatch.delenv("MONGODB_URI", raising=False)
    monkeypatch.setenv("MONGO_URL", "mongodb://user:pass@containers-us-west-123.railway.app:1234")
    from importlib import reload
    import services.platform_config as pc
    reload(pc)
    assert "railway.app" in pc.mongodb_uri()


def test_railway_is_production_by_default(monkeypatch):
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.delenv("DATAFLOW_ENV", raising=False)
    from importlib import reload
    import services.platform_config as pc
    reload(pc)
    assert pc.is_production() is True


def test_dev_mode_skips_production_validation(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    from importlib import reload
    import services.platform_config as pc
    reload(pc)
    assert pc.validate_production_config() == []


def test_stub_writes_blocked_in_production(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAFLOW_ALLOW_STUB_WRITES", "1")
    from importlib import reload
    import connectors.driver_guard as dg
    reload(dg)
    assert dg.stub_writes_allowed() is False


def test_stub_writes_allowed_in_dev_with_flag(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_ALLOW_STUB_WRITES", "1")
    from importlib import reload
    import connectors.driver_guard as dg
    reload(dg)
    assert dg.stub_writes_allowed() is True
