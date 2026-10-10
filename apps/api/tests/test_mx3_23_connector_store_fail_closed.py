"""QA MX3-23 — a failed MongoDB connector write must not fall back to a file store.

Server log (production backend): "MongoDB update_connector failed, falling back
to file: Production refuses plaintext secret passthrough. Re-save the connector
or job so credentials are Fernet-encrypted (enc:v1:...)". With Mongo as the
configured backend, every create/update/delete failure was swallowed and the
write went to a local JSON file instead: the caller saw success, the system of
record never changed, and the credentials landed on local disk.
"""

from __future__ import annotations

import pytest

import services.connector_store as cs
from services.secret_vault import SecretVaultError

_VAULT_MSG = (
    "Production refuses plaintext secret passthrough. "
    "Re-save the connector or job so credentials are Fernet-encrypted (enc:v1:…)."
)


class _FailingColl:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def insert_one(self, *_a, **_k):
        raise self._exc

    def replace_one(self, *_a, **_k):
        raise self._exc

    def delete_one(self, *_a, **_k):
        raise self._exc

    def find_one(self, *_a, **_k):
        raise self._exc

    def find(self, *_a, **_k):
        return []


@pytest.fixture
def mongo_backend(monkeypatch, tmp_path):
    store = tmp_path / "connectors.json"
    monkeypatch.setattr(cs, "_backend_choice", "mongo")
    monkeypatch.setattr(cs, "_store_path", lambda: store)
    return store


def test_update_connector_vault_refusal_fails_closed(monkeypatch, mongo_backend):
    def _refuse(*_a, **_k):
        raise SecretVaultError(_VAULT_MSG)

    monkeypatch.setattr(cs, "_get_mongo", _refuse)
    with pytest.raises(cs.ConnectorStoreError) as exc_info:
        cs.update_connector("c-1", {"name": "pg", "password": "new-secret"})
    assert "Fernet-encrypted" in str(exc_info.value)
    assert not mongo_backend.exists(), "credentials were written to the file fallback"


def test_update_connector_mongo_write_failure_fails_closed(monkeypatch, mongo_backend):
    existing = cs.SavedConnector.from_dict({"id": "c-1", "name": "pg", "type": "postgresql"})
    monkeypatch.setattr(cs, "_get_mongo", lambda *_a, **_k: existing)
    monkeypatch.setattr(cs, "_mongo_collection", lambda: _FailingColl(RuntimeError("not primary")))
    with pytest.raises(cs.ConnectorStoreError, match="not primary"):
        cs.update_connector("c-1", {"host": "db2"})
    assert not mongo_backend.exists()


def test_create_connector_mongo_failure_fails_closed(monkeypatch, mongo_backend):
    monkeypatch.setattr(cs, "_mongo_collection", lambda: _FailingColl(RuntimeError("not primary")))
    with pytest.raises(cs.ConnectorStoreError, match="not primary"):
        cs.create_connector({"name": "pg", "type": "postgresql", "password": "s3cret"})
    assert not mongo_backend.exists()


def test_delete_connector_mongo_failure_fails_closed(monkeypatch, mongo_backend):
    monkeypatch.setattr(cs, "_mongo_collection", lambda: _FailingColl(RuntimeError("not primary")))
    with pytest.raises(cs.ConnectorStoreError, match="not primary"):
        cs.delete_connector("c-1")
    assert not mongo_backend.exists()


def test_file_backend_still_writes(monkeypatch, tmp_path):
    store = tmp_path / "connectors.json"
    monkeypatch.setattr(cs, "_backend_choice", "file")
    monkeypatch.setattr(cs, "_store_path", lambda: store)
    conn = cs.create_connector({"name": "pg", "type": "postgresql", "host": "db1"})
    assert cs.update_connector(conn.id, {"host": "db2"}).host == "db2"
    assert cs.delete_connector(conn.id) is True


def test_api_answers_store_refusal_as_503_with_reason():
    import asyncio
    import json

    from starlette.requests import Request

    from src.main import connector_store_error_handler

    request = Request({"type": "http", "method": "PUT", "path": "/api/v1/connectors/c-1", "headers": []})
    resp = asyncio.run(
        connector_store_error_handler(request, cs.ConnectorStoreError("update refused: " + _VAULT_MSG))
    )
    assert resp.status_code == 503
    body = json.loads(resp.body)
    assert body["error"] == "connector_store_unavailable"
    assert "Fernet-encrypted" in body["detail"]
