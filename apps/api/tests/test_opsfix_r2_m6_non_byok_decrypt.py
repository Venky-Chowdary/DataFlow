"""Ops review round 2, Medium 6: a control-plane blip must not break non-BYOK connectors.

Round 1 made the workspace-to-tenant lookup fail closed so a BYOK tenant's
secret is never handled with the platform key. That lookup then ran before
every decrypt, even of a platform-key secret (``enc:v1:``) that needs no
tenant, so during a 2 s outage every worker run that needed a connector
failed and the connector list silently dropped them. The tenant is now looked
up only to decrypt a BYOK value; a BYOK value still fails closed.
"""

from __future__ import annotations

import pytest

from services import byok_key_manager, connector_store, mongodb_service, secret_vault, tenant_store
from services.control_plane_store import ControlPlaneUnavailable
from services.shared_record_store import SharedRecordSet


@pytest.fixture
def stores(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    monkeypatch.setattr(byok_key_manager, "STORE_PATH", tmp_path / "byok_keys.json")
    return tmp_path


def _control_plane_down(monkeypatch) -> None:
    def down(*_args, **_kwargs):
        raise ControlPlaneUnavailable("the control-plane database is unavailable")

    monkeypatch.setattr(SharedRecordSet, "load", down)


def test_platform_key_secrets_decrypt_during_a_control_plane_outage(stores, monkeypatch):
    stored = secret_vault.encrypt_secret("hunter2", tenant_id=None, label="connector-password")
    assert stored.startswith("enc:")
    _control_plane_down(monkeypatch)
    doc = {"_id": "c1", "id": "c1", "name": "pg", "type": "postgresql", "workspace_id": "ws-plain", "password": stored}
    assert mongodb_service._decrypt_connector_secrets(dict(doc))["password"] == "hunter2"
    assert connector_store.SavedConnector.from_dict(dict(doc)).password == "hunter2"
    listed = connector_store._materialize_docs([dict(doc)])
    assert [c.id for c in listed] == ["c1"] and listed[0].password == "hunter2"


def test_a_byok_secret_still_fails_closed_during_the_outage(stores, monkeypatch):
    tenant = tenant_store.create_tenant("ws-byok", "BYOK")
    byok_key_manager.create_key(tenant.id, "k0")
    stored = secret_vault.encrypt_secret("s3cret", tenant_id=tenant.id, label="connector-password")
    assert stored.startswith("byok:")
    doc = {"_id": "c2", "id": "c2", "name": "pg", "type": "postgresql", "workspace_id": "ws-byok", "password": stored}
    assert connector_store.SavedConnector.from_dict(dict(doc)).password == "s3cret"
    _control_plane_down(monkeypatch)
    with pytest.raises(ControlPlaneUnavailable):
        connector_store.SavedConnector.from_dict(dict(doc))
