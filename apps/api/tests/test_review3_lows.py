"""Security review round 3, Lows: ids named in a body are checked for ownership when written.

- ``/transfer/execute`` (and ``/transfer/run``) accepted another workspace's
  ``contract_id`` (200) while ``/contracts/test`` answered 404; the engine's
  ``stamp_request_contract`` bound it too (the contract's breaker and rules
  are that workspace's).
- Dangling foreign ids were stored: transforms ``destination_connector_id``,
  resource-ACL ``resource_id``, tenant ``byok_key_id`` (create and edit),
  ``/repair/propose/{quarantine,preflight}`` ``job_id`` and
  ``/transfer/plans`` ``source.connector_id`` / ``destination.connector_id``.

Each now answers 404 for an id the caller's workspace does not own, like a
missing id, and the owner is served.
"""

from __future__ import annotations

import pytest

from tenancy_world import tenancy_world  # noqa: F401


@pytest.fixture
def w(tenancy_world, monkeypatch):  # noqa: F811
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "review3-lows-secrets-key-distinct-from-auth-0001")
    import src.transfer.background as bg

    monkeypatch.setattr(bg, "run_transfer_async", lambda *a, **k: None)
    tenancy_world.admin_a = tenancy_world.member("lows-admin-a@example.com", tenancy_world.workspace_a, "admin")
    tenancy_world.admin_b = tenancy_world.member("lows-admin-b@example.com", tenancy_world.workspace_b, "admin")
    return tenancy_world


def _contract(workspace_id):
    from services import contract_store
    from services.data_contract import DataContract

    contract = DataContract(name="c", metadata={"workspace_id": workspace_id})
    contract_store.get_contract_store().save_contract(contract)
    return contract.id


def _connector(workspace_id, **extra):
    from services import connector_store

    return connector_store.create_connector({"name": f"c-{workspace_id[:6]}-{len(extra)}", "type": "sqlite",
                                            "database": ":memory:", "workspace_id": workspace_id, **extra}).id


def _execute(client, workspace_id, **extra):
    return client.post("/api/v1/transfer/execute", json={
        "source": {"kind": "database", "format": "sqlite", "connector_id": _connector(workspace_id), "table": "t"},
        "destination": {"kind": "file_export", "format": "csv"},
        "mappings": [{"source": "id", "target": "id", "confidence": 1.0}], "async_mode": True, **extra,
    })


def test_transfer_execute_refuses_another_workspaces_contract(w):
    a_contract = _contract(w.workspace_a)
    refused = _execute(w.b, w.workspace_b, contract_id=a_contract)
    assert refused.status_code == 404, refused.text
    assert refused.json()["detail"] == "Contract not found"
    own = _execute(w.b, w.workspace_b, contract_id=_contract(w.workspace_b))
    assert own.status_code not in (403, 404), own.text


def test_the_engine_does_not_bind_another_workspaces_contract(w):
    from src.transfer.contract_engine import stamp_request_contract
    from src.transfer.models import transfer_request_from_dict

    request = transfer_request_from_dict({
        "source": {"kind": "database", "format": "sqlite", "database": ":memory:", "table": "t"},
        "destination": {"kind": "file_export", "format": "csv"}, "workspace_id": w.workspace_b,
    })
    with pytest.raises(ValueError, match="not found"):
        stamp_request_contract(request, explicit_id=_contract(w.workspace_a))
    stamp_request_contract(request, explicit_id=_contract(w.workspace_b))
    assert request.contract_id


def test_transforms_refuse_another_workspaces_destination_connector(w):
    body = {"name": "t1", "destination_connector_id": _connector(w.workspace_a),
            "models": [{"name": "m", "sql": "select 1"}]}
    r = w.admin_b.post("/api/v1/transforms/", json=body)
    assert r.status_code == 404, r.text
    own = w.admin_b.post("/api/v1/transforms/", json={**body, "destination_connector_id": _connector(w.workspace_b)})
    assert own.status_code == 201, own.text
    project_id = own.json()["id"]
    edit = w.admin_b.patch(f"/api/v1/transforms/{project_id}",
                           json={"destination_connector_id": _connector(w.workspace_a)})
    assert edit.status_code == 404, edit.text


def test_resource_acls_name_only_the_granting_workspaces_resources(w):
    a_job = w.job(w.workspace_a, name="a-job", status="completed")
    b_job = w.job(w.workspace_b, name="b-job", status="completed")
    grant = {"resource_type": "job", "principal": "x@example.com", "role": "viewer"}
    refused = w.admin_b.post("/api/v1/resource-acls", json={**grant, "resource_id": a_job})
    assert refused.status_code == 404, refused.text
    own = w.admin_b.post("/api/v1/resource-acls", json={**grant, "resource_id": b_job})
    assert own.status_code == 200, own.text


def test_a_tenants_byok_key_must_be_its_own_workspaces(w):
    from services import byok_key_manager, tenant_store

    a_tenant = tenant_store.create_tenant(workspace_id=w.workspace_a, name="A")
    a_key = byok_key_manager.create_key(a_tenant.id, "a-key")
    refused = w.admin.post("/api/v1/workspace/tenant", json={"workspace_id": w.workspace_b, "name": "B",
                                                             "byok_key_id": a_key.id},
                           headers={"X-Workspace-Id": w.workspace_b})
    assert refused.status_code == 404, refused.text
    b_tenant = tenant_store.create_tenant(workspace_id=w.workspace_b, name="B")
    edit = w.admin.patch(f"/api/v1/workspace/tenant/{b_tenant.id}", json={"byok_key_id": a_key.id},
                         headers={"X-Workspace-Id": w.workspace_b})
    assert edit.status_code == 404, edit.text
    b_key = byok_key_manager.create_key(b_tenant.id, "b-key")
    ok = w.admin.patch(f"/api/v1/workspace/tenant/{b_tenant.id}", json={"byok_key_id": b_key.id},
                       headers={"X-Workspace-Id": w.workspace_b})
    assert ok.status_code == 200, ok.text


@pytest.mark.parametrize("route,extra", [
    ("/api/v1/repair/propose/quarantine", {"rejected_details": [{"column": "c", "reason": "bad"}]}),
    ("/api/v1/repair/propose/preflight", {"preflight": {"passed": False, "blockers": []}}),
])
def test_repair_proposals_name_only_the_callers_jobs(w, route, extra):
    a_job = w.job(w.workspace_a, name="a-job", status="failed")
    refused = w.admin_b.post(route, json={"job_id": a_job, **extra})
    assert refused.status_code == 404, refused.text
    b_job = w.job(w.workspace_b, name="b-job", status="failed")
    own = w.admin_b.post(route, json={"job_id": b_job, **extra})
    assert own.status_code == 200, own.text


@pytest.mark.parametrize("side", ["source", "destination"])
def test_transfer_plans_store_only_the_callers_connectors(w, side):
    body = {"name": "p", "source_columns": ["id"], "target_columns": ["id"]}
    endpoint = {"kind": "database", "format": "sqlite", "table": "t"}
    refused = w.admin_b.post("/api/v1/transfer/plans",
                             json={**body, side: {**endpoint, "connector_id": _connector(w.workspace_a)}})
    assert refused.status_code == 404, refused.text
    own = w.admin_b.post("/api/v1/transfer/plans",
                         json={**body, side: {**endpoint, "connector_id": _connector(w.workspace_b)}})
    assert own.status_code == 200, own.text
