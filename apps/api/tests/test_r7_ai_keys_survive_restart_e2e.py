"""Settings → AI Models: a saved model key survives a restart.

QA (production 557f556c): the tab said "/data/data is not a mounted volume on
this Railway service … saved settings are lost on every deploy or restart".
True — ``services/integrations_store.py`` wrote AI keys (and SSO, Pilot, MCP
policy) only to ``integrations.json`` on the container disk. The fix stores the
settings document (secrets still ``enc:v1`` ciphertext) in the control-plane
database, so a restart that wipes the disk keeps the key. The tab shows a
user-level message, never a path or the hosting provider's name.

Runs through the real HTTP routes against a live MongoDB.
"""

from __future__ import annotations

import json
import os
import uuid

import pytest
from fastapi.testclient import TestClient

from services import integrations_store

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI") or "mongodb://localhost:27017"
_ENV = ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_MODEL", "ANTHROPIC_MODEL",
        "OLLAMA_BASE_URL", "OLLAMA_MODEL", "DATAFLOW_PILOT_ENGINE")


@pytest.fixture(autouse=True)
def _clean_env():
    before = {k: os.environ.get(k) for k in _ENV}
    for k in _ENV:
        os.environ.pop(k, None)
    integrations_store._HYDRATED_ENV.clear()
    yield
    for k, v in before.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    integrations_store._HYDRATED_ENV.clear()


@pytest.fixture
def control_plane(monkeypatch, tmp_path):
    pymongo = pytest.importorskip("pymongo")
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
    try:
        client.admin.command("ping")
    except Exception:  # noqa: BLE001
        pytest.skip("MongoDB not reachable")
    name = f"r7_settings_{uuid.uuid4().hex[:10]}"
    db = client[name]
    import services.control_plane_store as cps

    monkeypatch.setattr(cps, "mongo_collection", lambda coll: db[coll])
    disk = tmp_path / "disk1"
    monkeypatch.setattr(integrations_store, "STORE_PATH", disk / "integrations.json")
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    try:
        yield {"db": db, "tmp": tmp_path}
    finally:
        client.drop_database(name)


@pytest.fixture
def http(control_plane, monkeypatch):
    from src.ai.llm import provider
    from src.main import app

    # The live key check calls the vendor; no real key exists in CI.
    monkeypatch.setattr(provider, "verify_cloud_api_key", lambda *_a, **_k: (True, ""))

    with TestClient(app) as c:
        yield c


def _restart_with_a_fresh_disk(monkeypatch, control_plane, n: int) -> None:
    """What a Railway redeploy does: the process env and the disk are new."""
    for k in _ENV:
        os.environ.pop(k, None)
    integrations_store._HYDRATED_ENV.clear()
    monkeypatch.setattr(
        integrations_store, "STORE_PATH", control_plane["tmp"] / f"disk{n}" / "integrations.json"
    )


def test_saved_key_survives_a_restart_that_wipes_the_disk(http, control_plane, monkeypatch):
    res = http.patch(
        "/api/v1/workspace/ai-providers/anthropic",
        json={"api_key": "sk-ant-r7-persist-1234567890", "model": "claude-sonnet-4-20250514"},
    )
    assert res.status_code == 200, res.text

    _restart_with_a_fresh_disk(monkeypatch, control_plane, 2)
    integrations_store.apply_integrations_to_env()  # what app start-up does

    listed = http.get("/api/v1/workspace/ai-providers")
    assert listed.status_code == 200, listed.text
    row = listed.json()["providers"]["anthropic"] if "providers" in listed.json() else listed.json()["anthropic"]
    assert row["configured"] is True
    assert row["key_state"] == "ready"
    assert integrations_store.resolve_provider_api_key("anthropic") == "sk-ant-r7-persist-1234567890"
    assert os.environ.get("ANTHROPIC_API_KEY") == "sk-ant-r7-persist-1234567890"

    # Stored encrypted, never as plaintext.
    dump = json.dumps(list(control_plane["db"].list_collection_names()) + [
        doc for name in control_plane["db"].list_collection_names()
        for doc in control_plane["db"][name].find({}, {"_id": False})
    ], default=str)
    assert "sk-ant-r7-persist" not in dump
    assert "enc:" in dump

    # A removal is durable too: a stale copy must not resurrect the key.
    assert http.delete("/api/v1/workspace/ai-providers/anthropic/key").status_code == 200
    _restart_with_a_fresh_disk(monkeypatch, control_plane, 3)
    assert integrations_store.resolve_provider_api_key("anthropic") == ""


def test_pilot_engine_and_model_survive_a_restart(http, control_plane, monkeypatch):
    assert http.patch(
        "/api/v1/workspace/ai-providers/openai",
        json={"api_key": "sk-r7-openai-abcdef123456", "model": "gpt-4.1-mini"},
    ).status_code == 200
    assert http.patch("/api/v1/workspace/pilot-engine", json={"engine": "cloud"}).status_code == 200
    _restart_with_a_fresh_disk(monkeypatch, control_plane, 4)
    assert integrations_store.get_pilot_engine_preference() == "cloud"
    assert integrations_store.get_ai_provider_configs()["openai"]["model"] == "gpt-4.1-mini"


def test_models_tab_shows_a_user_message_not_infrastructure(http, control_plane, monkeypatch):
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    body = http.get("/api/v1/copilot/models").json()["settings_storage"]
    # Durable through the database: nothing to warn about.
    assert body["persistent"] is True
    assert body["reason"] == ""

    import services.control_plane_store as cps

    monkeypatch.setattr(cps, "mongo_collection", lambda coll: None)
    body = http.get("/api/v1/copilot/models").json()["settings_storage"]
    assert body["persistent"] is False
    text = json.dumps(body)
    for leak in ("/data", "Railway", "volume", "DATAFLOW_", "DATAWRAP_", "mounted"):
        assert leak not in text, (leak, text)
    assert "administrator" in body["reason"]
