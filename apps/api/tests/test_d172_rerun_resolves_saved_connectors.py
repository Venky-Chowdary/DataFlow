"""D172 — rerun / retry / resume use the saved connector's *current* settings.

A job's stored ``transfer_request`` holds the host, credentials and database
the connection had when the job first ran (they were resolved before the
request was persisted), and resolution lets inline values win over the saved
connector. After a server move or credential rotation a rerun therefore dialled
the old server — or, if it still answered, read and wrote the wrong database.
The rerun preview also showed blank source and destination because the stored
endpoint has no ``connector_name`` / ``type``.
"""

from __future__ import annotations

import os
import uuid

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.adapters as adapters  # noqa: E402
import src.transfer.engine as engine_mod  # noqa: E402
from services.mongodb_service import MemoryMongoDBService  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _saved(**over):
    base = {
        "type": "mysql", "host": "10.0.0.1", "port": 3306, "database": "shop",
        "schema": "", "username": "etl", "password": "old-secret",
        "connection_string": "", "warehouse": "", "ssl": False, "extra": {},
    }
    base.update(over)
    return base


@pytest.fixture()
def world(monkeypatch):
    """Two saved connectors, an in-memory job store, and a run_transfer_async spy."""
    saved = {
        "c-src": _saved(),
        "c-dst": _saved(type="postgresql", host="10.0.0.2", port=5432, database="wh", username="w"),
    }
    monkeypatch.setattr(
        adapters, "_lookup_saved_connector", lambda cid, workspace_id=None: saved.get(cid)
    )
    import services.connector_store as connector_store

    # The workspace-scoped existence check refresh_saved_endpoint runs first.
    monkeypatch.setattr(
        connector_store,
        "get_connector",
        lambda cid, workspace_id=None: object() if cid in saved else None,
    )
    store = MemoryMongoDBService()
    import services.mongodb_service as mongo_mod
    import importlib

    router = importlib.import_module("src.routers.connectors_router")

    for module in (engine_mod, router, mongo_mod):
        monkeypatch.setattr(module, "get_mongodb_service", lambda: store)
    launched: list = []
    import src.transfer.background as background

    monkeypatch.setattr(
        background, "run_transfer_async", lambda job_id, req, resume=False: launched.append((job_id, req, resume))
    )
    return saved, store, launched


def _first_run(store, status: str) -> str:
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="mysql", connector_id="c-src", table="orders"),
        destination=EndpointConfig(kind="database", format="postgresql", connector_id="c-dst", table="orders"),
        sync_mode="full_refresh_overwrite",
    )
    engine = UniversalTransferEngine()
    engine._resolve_saved_connectors(request)
    # The frozen state: the connection as it was when the job first ran.
    assert request.source.host == "10.0.0.1" and request.source.password == "old-secret"
    job_id = engine._create_pending_job(request)
    store.update_job_status(job_id, status)
    return job_id


def _client():
    from src.main import app

    return TestClient(app)


@pytest.mark.parametrize("action", ["rerun", "retry"])
def test_replay_uses_the_connector_as_saved_now(world, action):
    saved, store, launched = world
    job_id = _first_run(store, "completed" if action == "rerun" else "failed")
    # Server move + credential rotation + a different database after run 1.
    saved["c-src"].update(host="10.9.9.9", password="new-secret", username="etl2", database="shop2")
    saved["c-dst"].update(host="10.8.8.8")

    resp = _client().post(f"/api/v1/connectors/jobs/{job_id}/{action}")
    assert resp.status_code == 200, resp.text
    _new_job, request, _resume = launched[-1]
    assert (request.source.host, request.source.username, request.source.password) == (
        "10.9.9.9", "etl2", "new-secret",
    )
    assert request.source.database == "shop2"
    assert request.destination.host == "10.8.8.8"
    # What the job chose stays.
    assert request.source.table == "orders" and request.destination.table == "orders"


def test_replay_of_a_deleted_connector_is_refused_not_guessed(world):
    saved, store, _launched = world
    job_id = _first_run(store, "completed")
    del saved["c-src"]
    resp = _client().post(f"/api/v1/connectors/jobs/{job_id}/rerun")
    assert resp.status_code == 409, resp.text
    assert "Connector c-src no longer exists" in resp.text


def test_inline_endpoints_and_explicit_database_overrides_keep_their_stored_config(world):
    inline = EndpointConfig(
        kind="database", format="mysql", host="db.internal", port=3307,
        database="inline_db", username="u", password="p", table="t",
    )
    assert adapters.refresh_saved_endpoint(inline) is inline

    overridden = EndpointConfig(
        kind="database", format="mysql", connector_id="c-src", host="stale", database="chosen",
        extra={"destination_identity": {"authority": "studio_override", "database": "chosen"}},
    )
    refreshed = adapters.refresh_saved_endpoint(overridden)
    assert refreshed.host == "10.0.0.1"  # the connection follows the connector
    assert refreshed.database == "chosen"  # the operator's choice stays


def test_a_connector_outside_the_jobs_workspace_is_refused(world, monkeypatch):
    """Existence is checked in the job's workspace, never via an unscoped fallback."""
    import services.connector_store as connector_store

    seen: list = []

    def scoped(cid, workspace_id=None):
        seen.append((cid, workspace_id))
        return object() if workspace_id == "ws-a" else None

    monkeypatch.setattr(connector_store, "get_connector", scoped)
    endpoint = EndpointConfig(kind="database", format="mysql", connector_id="c-src", table="t")
    with pytest.raises(ValueError, match="Connector c-src no longer exists"):
        adapters.refresh_saved_endpoint(endpoint, workspace_id="ws-b")
    assert seen == [("c-src", "ws-b")]
    assert adapters.refresh_saved_endpoint(endpoint, workspace_id="ws-a").host == "10.0.0.1"


def test_rerun_preview_names_source_and_destination(world, monkeypatch):
    saved, store, _launched = world
    job_id = _first_run(store, "completed")
    import services.connector_store as connector_store
    from types import SimpleNamespace

    monkeypatch.setattr(
        connector_store,
        "get_connector",
        lambda cid, workspace_id=None: SimpleNamespace(name={"c-src": "Shop MariaDB", "c-dst": "Warehouse"}[cid]),
    )
    from src.ai.copilot import lifecycle_tools

    brief = lifecycle_tools._job_brief(store.get_job(job_id))
    assert brief["source"] == "Shop MariaDB" and brief["destination"] == "Warehouse"
    assert brief["table"] == "orders"
