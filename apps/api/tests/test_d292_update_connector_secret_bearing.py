"""D292 - update_connector failed on every connector that stores a secret.

QA (production, Mongo connector store, Fernet vault): ``update_connector`` on a
Postgres / MySQL / SFTP / ... connector with a saved password staged fine, but
Confirm answered "Failed to update connector ... The cause is in the API log"
for a host+port change, a rename and a password change alike. Connectors with
no stored secret updated fine.

Root cause: the Mongo branch of ``connector_store.update_connector`` checked
the merged TLS plan through ``SavedConnector.from_dict(merged)``, which
decrypts every secret field. ``merged`` mixes ciphertext from the stored
document with the plaintext the Confirm route sends (it re-sends the whole
saved connector, decrypted password included), and production's vault refuses
plaintext "passthrough" -> SecretVaultError -> ConnectorStoreError. The ``ssl``
field rides every Confirm, so the check ran on every edit.

Route: MCP ``update_connector`` -> ``POST /api/v1/copilot/confirm`` -> the
Mongo connector store (live local MongoDB), vault in production mode with a
dedicated Fernet key.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(300)

_SECRET = "s3cret-Pw!"  # nosec B105 - test fixture value


@pytest.fixture
def client(tmp_path, monkeypatch):
    pymongo = pytest.importorskip("pymongo")
    pytest.importorskip("cryptography")
    if not h.port_up(27017):
        pytest.skip("MongoDB not reachable on :27017")
    from cryptography.fernet import Fernet

    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", Fernet.generate_key().decode())
    import services.connector_store as store
    import services.secret_vault as vault

    # Production's vault: plaintext secrets are never passed through.
    monkeypatch.setattr(vault, "_is_production", lambda: True)
    mongo = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    db_name = f"dataflow_d292_{uuid.uuid4().hex[:8]}"
    coll = mongo[db_name]["connectors"]
    monkeypatch.setattr(store, "_backend_choice", "mongo")
    monkeypatch.setattr(store, "_mongo_collection", lambda: coll)
    from fastapi.testclient import TestClient
    from src.main import app

    try:
        with TestClient(app) as c:
            yield c, coll
    finally:
        mongo.drop_database(db_name)
        mongo.close()


def _confirm(client, ack_id: str) -> dict:
    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": ack_id, "reason": "d292"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body
    return body


def _update(client, connector_id: str, **changes) -> dict:
    ok, staged = h.mcp_call(client, "update_connector", {"connector_id": connector_id, **changes})
    assert ok, staged
    return _confirm(client, staged["ack_id"])


def _create(**extra):
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    return create_connector({
        "name": f"d292-{token}", "type": "postgresql", "role": "source", "workspace_id": "",
        "host": "127.0.0.1", "port": 5432, "database": "dataflow", "username": "dataflow",
        "ssl": False, **extra,
    })


def test_secret_bearing_connector_host_port_rename_and_password_edits_save(client):
    c, coll = client
    from services.connector_store import get_connector

    saved = _create(password=_SECRET)
    stored = coll.find_one({"_id": saved.id})
    assert str(stored["password"]).startswith("enc:v1:"), "fixture must store ciphertext"

    _update(c, saved.id, host="127.0.0.2", port=6543)
    after = get_connector(saved.id)
    assert (after.host, int(after.port)) == ("127.0.0.2", 6543)
    assert after.password == _SECRET  # kept, never re-sent

    _update(c, saved.id, new_name=f"{saved.name}-renamed")
    after = get_connector(saved.id)
    assert after.name == f"{saved.name}-renamed"
    assert after.password == _SECRET

    _update(c, saved.id, password="n3w-Secret")
    after = get_connector(saved.id)
    assert after.password == "n3w-Secret"
    raw = coll.find_one({"_id": saved.id})
    # Every secret is stored encrypted after the edits; nothing in plaintext.
    assert str(raw["password"]).startswith("enc:v1:")
    assert "n3w-Secret" not in repr(raw)


def test_secret_bearing_connection_string_is_kept_through_a_rename(client):
    c, coll = client
    from services.connector_store import get_connector

    dsn = f"postgresql://dataflow:{_SECRET}@127.0.0.1:5432/dataflow"
    saved = _create(password=_SECRET, connection_string=dsn)
    _update(c, saved.id, new_name=f"{saved.name}-dsn")
    after = get_connector(saved.id)
    assert after.name == f"{saved.name}-dsn"
    assert (after.password, after.connection_string) == (_SECRET, dsn)
    assert str(coll.find_one({"_id": saved.id})["connection_string"]).startswith("enc:v1:")


def test_secret_less_connector_still_updates(client):
    c, _coll = client
    from services.connector_store import get_connector

    saved = _create()
    _update(c, saved.id, host="127.0.0.3", new_name=f"{saved.name}-nopw")
    after = get_connector(saved.id)
    assert (after.host, after.name, after.password) == ("127.0.0.3", f"{saved.name}-nopw", "")


def test_a_store_failure_names_its_reason_instead_of_the_api_log(client, monkeypatch):
    c, _coll = client
    import services.connector_store as store
    import src.routers.saved_connectors_router as scr

    saved = _create(password=_SECRET)

    def _fail(*_a, **_kw):
        raise store.ConnectorStoreError(
            "Connector update failed in MongoDB (the configured connector store); "
            "nothing was saved: replica set has no primary"
        )

    monkeypatch.setattr(scr, "update_connector", _fail)
    ok, staged = h.mcp_call(c, "update_connector", {"connector_id": saved.id, "port": 6000})
    assert ok, staged
    resp = c.post("/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "d292"})
    detail = resp.json().get("detail") or ""
    assert resp.status_code == 400, resp.text
    assert "replica set has no primary" in detail, detail
    assert "API log" not in detail, detail
