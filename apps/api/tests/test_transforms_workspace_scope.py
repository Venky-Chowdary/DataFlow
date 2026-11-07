from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def transform_scope(tmp_path, monkeypatch):
    admin_email = f"root-{tmp_path.name}@example.com"
    editor_email = f"transform-editor-{tmp_path.name}@example.com"
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", admin_email)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-transforms-workspace")

    from services.auth_rate_limit import reset_auth_rate_limits
    from services import audit_log, team_store, user_store
    from services.transform_store import FileTransformProjectStore, TransformProject
    from src.main import app
    from src.routers import transforms_router

    reset_auth_rate_limits()
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    store = FileTransformProjectStore(path=tmp_path / "projects.json")
    monkeypatch.setattr(transforms_router, "get_transform_store", lambda: store)

    admin = TestClient(app)
    login = admin.post(
        "/api/v1/auth/login",
        json={"email": admin_email, "password": "Bootstrap-Admin-2026"},
    )
    assert login.status_code == 200, login.text
    admin.headers["Authorization"] = f"Bearer {login.json()['token']}"

    workspace_a = admin.post(
        "/api/v1/team/workspaces", json={"name": "Transform workspace A"}
    ).json()["workspace"]["id"]
    workspace_b = admin.post(
        "/api/v1/team/workspaces", json={"name": "Transform workspace B"}
    ).json()["workspace"]["id"]
    created = admin.post(
        "/api/v1/team/users",
        json={
            "email": editor_email,
            "platform_role": "member",
            "workspace_id": workspace_b,
            "workspace_role": "editor",
        },
    )
    assert created.status_code == 200, created.text
    member = TestClient(app)
    member_login = member.post(
        "/api/v1/auth/login",
        json={
            "email": editor_email,
            "password": created.json()["temporary_password"],
        },
    )
    assert member_login.status_code == 200, member_login.text
    member.headers.update(
        {
            "Authorization": f"Bearer {member_login.json()['token']}",
            "X-Workspace-Id": workspace_b,
        }
    )

    project = TransformProject(
        name="Workspace A private project",
        destination_connector_id="destination-a",
        workspace_id=workspace_a,
    )
    store.save(project)
    return {
        "admin": admin,
        "member": member,
        "workspace_a": workspace_a,
        "workspace_b": workspace_b,
        "project": project,
        "store": store,
    }


def test_foreign_project_lifecycle_is_indistinguishable_from_missing(transform_scope):
    context = transform_scope
    client = context["member"]
    project_id = context["project"].id
    random_id = "f" * 24

    operations = (
        ("GET", "/api/v1/transforms/{id}", None),
        ("PATCH", "/api/v1/transforms/{id}", {"name": "Changed by workspace B"}),
        ("DELETE", "/api/v1/transforms/{id}", None),
        ("GET", "/api/v1/transforms/{id}/export/dbt", None),
        ("POST", "/api/v1/transforms/{id}/run?dry_run=true", None),
    )
    failures = []
    for method, path, body in operations:
        foreign_path = path.replace("{id}", project_id)
        missing_path = path.replace("{id}", random_id)
        foreign = client.request(method, foreign_path, json=body)
        missing = client.request(method, missing_path, json=body)
        if (
            foreign.status_code != 404
            or missing.status_code != 404
            or foreign.json() != missing.json()
        ):
            failures.append(
                {
                    "method": method,
                    "foreign": (foreign.status_code, foreign.text),
                    "missing": (missing.status_code, missing.text),
                }
            )

    unchanged = context["store"].get(project_id)
    assert unchanged is not None
    assert unchanged.name == "Workspace A private project"
    assert unchanged.workspace_id == context["workspace_a"]
    assert failures == []


def test_legacy_blank_workspace_project_is_hidden_from_header_scoped_workspace(
    transform_scope, monkeypatch
):
    from services.transform_store import TransformProject

    context = transform_scope
    legacy = context["store"].save(
        TransformProject(
            name="Legacy project",
            destination_connector_id="legacy-destination",
            workspace_id="",
        )
    )

    response = context["member"].get(f"/api/v1/transforms/{legacy.id}")
    assert response.status_code == 404, response.text

    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "0")
    response = context["member"].get(f"/api/v1/transforms/{legacy.id}")
    assert response.status_code == 404, response.text


def test_legacy_project_lifecycle_is_indistinguishable_from_missing(transform_scope):
    from services.transform_store import TransformProject

    context = transform_scope
    legacy = context["store"].save(
        TransformProject(
            name="Legacy project",
            destination_connector_id="legacy-destination",
            workspace_id="",
        )
    )
    client = context["member"]
    operations = (
        ("GET", "/api/v1/transforms/{id}", None),
        ("PATCH", "/api/v1/transforms/{id}", {"name": "Changed by workspace B"}),
        ("DELETE", "/api/v1/transforms/{id}", None),
        ("GET", "/api/v1/transforms/{id}/export/dbt", None),
        ("POST", "/api/v1/transforms/{id}/run?dry_run=true", None),
    )
    for method, path, body in operations:
        foreign = client.request(method, path.replace("{id}", legacy.id), json=body)
        missing = client.request(
            method, path.replace("{id}", "f" * 24), json=body
        )
        assert foreign.status_code == missing.status_code == 404
        assert foreign.json() == missing.json()
    unchanged = context["store"].get(legacy.id)
    assert unchanged is not None and unchanged.name == "Legacy project"


def test_legacy_blank_workspace_project_is_readable_and_editable_without_header(
    transform_scope, monkeypatch
):
    from services.transform_store import TransformProject

    context = transform_scope
    legacy = context["store"].save(
        TransformProject(
            name="Legacy project",
            destination_connector_id="legacy-destination",
            workspace_id="",
        )
    )
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "0")
    client = context["admin"]
    client.headers.pop("X-Workspace-Id", None)
    response = client.get(f"/api/v1/transforms/{legacy.id}")
    assert response.status_code == 200, response.text
    update = client.patch(
        f"/api/v1/transforms/{legacy.id}", json={"name": "Edited legacy"}
    )
    assert update.status_code == 200, update.text
    assert update.json()["name"] == "Edited legacy"


def test_header_scoped_transform_list_excludes_legacy_unscoped_projects(transform_scope):
    from services.transform_store import TransformProject

    context = transform_scope
    legacy = context["store"].save(
        TransformProject(
            name="Legacy project",
            destination_connector_id="legacy-destination",
            workspace_id="",
        )
    )
    response = context["member"].get("/api/v1/transforms/")
    assert response.status_code == 200, response.text
    assert legacy.id not in {item["id"] for item in response.json()}


def test_named_workspace_project_is_hidden_without_header_when_isolation_is_off(
    transform_scope, monkeypatch
):
    context = transform_scope
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "0")

    response = context["admin"].get(f"/api/v1/transforms/{context['project'].id}")
    assert response.status_code == 404, response.text


def test_transform_store_get_filters_file_records_by_workspace(tmp_path):
    from services.transform_store import FileTransformProjectStore, TransformProject

    store = FileTransformProjectStore(path=tmp_path / "projects.json")
    project = store.save(
        TransformProject(
            name="Scoped",
            destination_connector_id="destination",
            workspace_id="workspace-a",
        )
    )

    assert store.get(project.id) is not None
    assert store.get(project.id, workspace_id="workspace-a") is not None
    assert store.get(project.id, workspace_id="workspace-b") is None

    legacy = store.save(
        TransformProject(
            name="Legacy",
            destination_connector_id="legacy-destination",
            workspace_id="",
        )
    )
    assert store.get(legacy.id, workspace_id="workspace-b") is None
    assert store.get(legacy.id, workspace_id="") is not None


def test_mongo_transform_lookup_includes_workspace_in_query(tmp_path):
    from services.transform_store import (
        FileTransformProjectStore,
        MongoTransformProjectStore,
        TransformProject,
    )

    project = TransformProject(
        id="project-1",
        name="Mongo scoped",
        destination_connector_id="destination",
        workspace_id="workspace-a",
    )

    class Collection:
        query = None

        def find_one(self, query):
            self.query = query
            return (
                project.to_dict()
                if query == {"id": "project-1", "workspace_id": "workspace-a"}
                else None
            )

    collection = Collection()

    class Database:
        def __getitem__(self, _name):
            return collection

    class MongoService:
        def get_database(self):
            return Database()

    store = MongoTransformProjectStore(MongoService())
    store._file = FileTransformProjectStore(path=tmp_path / "empty-projects.json")

    assert store.get("project-1", workspace_id="workspace-a") is not None
    assert collection.query == {"id": "project-1", "workspace_id": "workspace-a"}
    store.get("project-1", workspace_id="")
    assert collection.query == {
        "id": "project-1",
        "workspace_id": {"$in": ["", None]},
    }


def test_mongo_transform_list_uses_exact_workspace_filter(tmp_path):
    from services.transform_store import FileTransformProjectStore, MongoTransformProjectStore

    class Collection:
        query = None

        def find(self, query):
            self.query = query
            return []

    collection = Collection()

    class Database:
        def __getitem__(self, _name):
            return collection

    class MongoService:
        def get_database(self):
            return Database()

    store = MongoTransformProjectStore(MongoService())
    store._file = FileTransformProjectStore(path=tmp_path / "empty-projects.json")
    store.list("workspace-b")
    assert collection.query == {"workspace_id": "workspace-b"}


LIVE_MONGO_URI = os.getenv("DATAFLOW_LIVE_MONGO_URI", "").strip()


@pytest.mark.skipif(not LIVE_MONGO_URI, reason="DATAFLOW_LIVE_MONGO_URI is not configured")
def test_live_mongo_transform_lookup_hides_foreign_workspace_project(
    transform_scope, monkeypatch, tmp_path
):
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
    from uuid import uuid4

    from services.transform_store import (
        FileTransformProjectStore,
        MongoTransformProjectStore,
        TransformProject,
    )
    from src.routers import transforms_router

    client = MongoClient(LIVE_MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except PyMongoError:
        client.close()
        pytest.skip("DATAFLOW_LIVE_MONGO_URI is unavailable")

    database = client[f"df_m2_transform_scope_{uuid4().hex}"]

    class MongoService:
        def get_database(self):
            return database

    store = MongoTransformProjectStore(MongoService())
    store._file = FileTransformProjectStore(path=tmp_path / "mongo-projects.json")
    project = store.save(
        TransformProject(
            name="Live Mongo workspace A project",
            destination_connector_id="live-destination-a",
            workspace_id=transform_scope["workspace_a"],
        )
    )
    persisted = database[store.COLLECTION].find_one({"id": project.id})
    assert persisted is not None
    assert persisted["workspace_id"] == transform_scope["workspace_a"]
    monkeypatch.setattr(transforms_router, "get_transform_store", lambda: store)
    legacy = store.save(
        TransformProject(
            name="Live Mongo unscoped legacy project",
            destination_connector_id="legacy-destination",
            workspace_id="",
        )
    )

    try:
        assert store.get(project.id, workspace_id=transform_scope["workspace_a"])
        assert store.get(project.id, workspace_id=transform_scope["workspace_b"]) is None
        assert store.get(legacy.id, workspace_id="") is not None
        assert store.get(legacy.id, workspace_id=transform_scope["workspace_b"]) is None
        assert legacy.id not in {item.id for item in store.list(transform_scope["workspace_b"])}

        foreign = transform_scope["member"].get(f"/api/v1/transforms/{project.id}")
        missing = transform_scope["member"].get(
            f"/api/v1/transforms/{'f' * 24}"
        )
        assert foreign.status_code == missing.status_code == 404
        assert foreign.json() == missing.json()
        legacy_foreign = transform_scope["member"].get(
            f"/api/v1/transforms/{legacy.id}"
        )
        legacy_missing = transform_scope["member"].get(
            f"/api/v1/transforms/{'e' * 24}"
        )
        assert legacy_foreign.status_code == legacy_missing.status_code == 404
        assert legacy_foreign.json() == legacy_missing.json()
    finally:
        client.drop_database(database.name)
        client.close()
