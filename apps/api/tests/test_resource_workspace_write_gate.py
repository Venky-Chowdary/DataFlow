"""Mutations need write membership in the resource's own workspace.

The caller's RBAC permission (e.g. ``connector.write``) comes from their
effective role, which is not tied to the workspace a resource lives in. A
viewer of workspace B holding an editor credential elsewhere, sending no
``X-Workspace-Id``, must not be able to delete or change B's resources: the
per-object check on mutating routes has to ask ``can_write_workspace``.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import contract_store, resource_acl, team_store  # noqa: E402
from services.data_contract import DataContract  # noqa: E402
from services.workspace_access import assert_resource_workspace  # noqa: E402

OWNER = "owner@example.com"
VIEWER = "mallory@example.com"
EDITOR = "editor@example.com"
OUTSIDER = "outsider@example.com"


@pytest.fixture
def workspace_b(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_CONTRACTS_PATH", str(tmp_path / "contracts.json"))
    monkeypatch.delenv("DATAFLOW_REQUIRE_WORKSPACE", raising=False)
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(resource_acl, "STORE_PATH", tmp_path / "resource-acls.jsonl")
    contract_store.reset_contract_store()
    ws = team_store.create_workspace(name="Globex", created_by=OWNER)
    team_store.add_workspace_member(workspace_id=ws.id, email=VIEWER, role="viewer", added_by=OWNER)
    team_store.add_workspace_member(workspace_id=ws.id, email=EDITOR, role="editor", added_by=OWNER)
    yield ws.id
    contract_store.reset_contract_store()


def _request(email: str, *, role: str = "editor", workspace_header: str = "") -> Request:
    headers = [(b"x-workspace-id", workspace_header.encode())] if workspace_header else []
    req = Request({"type": "http", "method": "DELETE", "path": "/", "headers": headers})
    req.state.user_email = email
    req.state.user = {"email": email, "role": role}
    return req


def _status(fn) -> int:
    try:
        fn()
    except HTTPException as exc:
        return exc.status_code
    return 200


def test_read_mode_still_admits_viewer(workspace_b):
    assert _status(lambda: assert_resource_workspace(_request(VIEWER), workspace_b)) == 200


@pytest.mark.parametrize(
    ("email", "expected"),
    [(VIEWER, 403), (EDITOR, 200), (OWNER, 200), (OUTSIDER, 404)],
)
def test_write_mode_requires_write_membership(workspace_b, email, expected):
    req = _request(email)
    assert _status(lambda: assert_resource_workspace(req, workspace_b, write=True)) == expected


def test_write_mode_keeps_scope_404_before_write_403(workspace_b):
    req = _request(VIEWER, workspace_header="other-workspace")
    assert _status(lambda: assert_resource_workspace(req, workspace_b, write=True)) == 404


class _Mongo:
    def __init__(self, workspace_id: str):
        self.workspace_id = workspace_id
        self.deleted = False

    def get_connector(self, connector_id):
        return {"_id": connector_id, "id": connector_id, "workspace_id": self.workspace_id}

    def delete_connector(self, connector_id):
        self.deleted = True
        return True


@pytest.mark.parametrize(("email", "expected", "deleted"), [(VIEWER, 403, False), (EDITOR, 200, True)])
def test_delete_connector_needs_write_in_resource_workspace(workspace_b, monkeypatch, email, expected, deleted):
    module = importlib.import_module("src.routers.connectors_router")
    mongo = _Mongo(workspace_b)
    monkeypatch.setattr(module, "get_mongodb_service", lambda: mongo)
    req = _request(email)
    assert _status(lambda: asyncio.run(module.delete_connector("B_CONN", req))) == expected
    assert mongo.deleted is deleted


def test_saved_connector_mutation_needs_write_in_resource_workspace(workspace_b):
    module = importlib.import_module("src.routers.saved_connectors_router")
    conn = SimpleNamespace(id="B_SAVED", workspace_id=workspace_b)
    assert module._can_access_connector(_request(VIEWER), conn) is True
    assert module._can_access_connector(_request(VIEWER), conn, write=True) is False
    assert module._can_access_connector(_request(EDITOR), conn, write=True) is True


def test_saved_connector_mutation_honours_viewer_acl_grant(workspace_b):
    module = importlib.import_module("src.routers.saved_connectors_router")
    conn = SimpleNamespace(id="B_ACL", workspace_id=workspace_b)
    resource_acl.upsert_grant(
        tenant_id=workspace_b,
        resource_type="connector",
        resource_id=conn.id,
        principal=EDITOR,
        role="viewer",
        created_by=OWNER,
    )
    assert module._can_access_connector(_request(EDITOR), conn) is True
    assert module._can_access_connector(_request(EDITOR), conn, write=True) is False


def test_bound_schedule_write_mode(workspace_b, monkeypatch):
    module = importlib.import_module("src.routers.schedules_router")
    sched = SimpleNamespace(id="B_SCHED", workspace_id=workspace_b)
    monkeypatch.setattr(module, "get_schedule", lambda _sid: sched)
    assert _status(lambda: module._bound_schedule(_request(VIEWER), "B_SCHED")) == 200
    assert _status(lambda: module._bound_schedule(_request(VIEWER), "B_SCHED", write=True)) == 403
    assert _status(lambda: module._bound_schedule(_request(EDITOR), "B_SCHED", write=True)) == 200


def test_scoped_contract_write_mode(workspace_b):
    module = importlib.import_module("src.routers.contracts_router")
    contract = DataContract(name="B_CONTRACT", metadata={"workspace_id": workspace_b})
    contract_store.get_contract_store().save_contract(contract)
    assert _status(lambda: module._scoped_contract(_request(VIEWER), contract.id, "")) == 200
    assert _status(lambda: module._scoped_contract(_request(VIEWER), contract.id, "", write=True)) == 403
    assert _status(lambda: module._scoped_contract(_request(EDITOR), contract.id, "", write=True)) == 200


_GUARDS = ("assert_resource_workspace", "_bound_schedule", "_can_access_connector", "_scoped_contract")
_ROUTERS = ("connectors", "saved_connectors", "schedules", "contracts", "transforms", "transform_library")


def test_every_put_patch_delete_route_uses_write_mode():
    """Static guard: a DELETE/PUT/PATCH route never gates its object with the read check."""
    import ast

    offenders = []
    for name in _ROUTERS:
        path = _API_ROOT / "src" / "routers" / f"{name}_router.py"
        tree = ast.parse(path.read_text())
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            methods = {
                d.func.attr
                for d in fn.decorator_list
                if isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute)
            }
            if not methods & {"delete", "put", "patch"}:
                continue
            for call in ast.walk(fn):
                if not isinstance(call, ast.Call):
                    continue
                callee = getattr(call.func, "id", None) or getattr(call.func, "attr", None)
                if callee not in _GUARDS:
                    continue
                write = next((k.value for k in call.keywords if k.arg == "write"), None)
                if not (isinstance(write, ast.Constant) and write.value is True):
                    offenders.append(f"{name}_router.{fn.name}:{call.lineno}")
    assert not offenders, f"mutating routes gate with read-level check: {offenders}"
