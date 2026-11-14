"""Ops review Medium 6: BYOK encryption must never silently downgrade.

``secret_vault`` decides per secret whether the tenant's own key (BYOK) wraps
it. Both lookups it makes (the workspace's tenant, the tenant's active key)
read control-plane stores that are now shared over the network. When a read
failed, the lookups answered "no tenant" / "no BYOK key", and a BYOK tenant's
secret was stored under the platform key instead. A store error now
propagates: the secret is not stored rather than stored under the wrong key.
"""

from __future__ import annotations

import pytest

from services import byok_key_manager, secret_vault, tenant_store
from services.control_plane_store import ControlPlaneUnavailable


@pytest.fixture
def stores(tmp_path, monkeypatch):
    monkeypatch.setattr(byok_key_manager, "STORE_PATH", tmp_path / "byok_keys.json")
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    return tmp_path


def _down(monkeypatch, store) -> None:
    def unreadable(*_args, **_kwargs):
        raise ControlPlaneUnavailable("the control-plane database is configured but unreachable")

    monkeypatch.setattr(type(store), "load", unreadable)


def test_byok_tenant_secret_is_refused_not_downgraded_when_the_key_store_is_unreadable(stores, monkeypatch):
    """p7: the key store blips while a connector secret is saved."""
    byok_key_manager.create_key("tenant-1", "k", provider="local")
    assert secret_vault.encrypt_secret("s3cret", tenant_id="tenant-1").startswith("byok:")
    _down(monkeypatch, byok_key_manager._DOC)
    with pytest.raises(ControlPlaneUnavailable):
        secret_vault.encrypt_secret("s3cret", tenant_id="tenant-1")


def test_workspace_tenant_lookup_propagates_a_store_error(stores, monkeypatch):
    tenant = tenant_store.create_tenant("ws-1", "Acme")
    assert secret_vault.tenant_id_from_workspace("ws-1") == tenant.id
    _down(monkeypatch, tenant_store._DOC)
    with pytest.raises(ControlPlaneUnavailable):
        secret_vault.tenant_id_from_workspace("ws-1")


def test_healthy_stores_still_choose_the_right_key(stores):
    tenant = tenant_store.create_tenant("ws-plain", "Plain")
    assert secret_vault.tenant_id_from_workspace("ws-plain") == tenant.id
    assert secret_vault.tenant_id_from_workspace("ws-unknown") is None
    assert secret_vault.tenant_id_from_workspace("") is None
    assert secret_vault.encrypt_secret("s3cret", tenant_id=tenant.id).startswith("enc:")
    assert secret_vault.encrypt_secret("s3cret", tenant_id=None).startswith("enc:")


def test_api_answers_503_when_the_control_plane_is_unavailable():
    """The refusal reaches the user as a clean, retryable 503, not a 500."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src import main

    handler = main.app.exception_handlers.get(ControlPlaneUnavailable)
    assert handler is not None, "the API has no handler for ControlPlaneUnavailable"
    app = FastAPI()
    app.add_exception_handler(ControlPlaneUnavailable, handler)

    @app.get("/thing")
    def thing():
        raise ControlPlaneUnavailable("the control-plane database is configured but unreachable")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/thing")
    assert response.status_code == 503
    assert response.headers.get("retry-after")
    assert "unavailable" in response.json()["detail"].lower()
