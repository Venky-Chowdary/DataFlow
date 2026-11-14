"""SEC-4: a transfer plan never stores or returns raw endpoint credentials.

``POST /transfer/plans`` stored ``source.password`` and
``destination.password`` as sent, and the create response, ``GET
/transfer/plans/{id}`` and the run payload returned them to every editor.
"""

from __future__ import annotations

from pathlib import Path

from tests.tenancy_world import tenancy_world  # noqa: F401

PW = "w1secPW_s3ntinel_4471"


def _endpoint(host: str, pw: str) -> dict:
    return {"kind": "database", "format": "postgresql", "host": host, "password": pw, "table": "t",
            "username": "u", "connection_string": f"postgresql://u:{pw}@{host}:5432/d",
            "extra": {"client_secret": f"{pw}-extra"}}


def test_plan_create_get_and_run_payload_mask_credentials(tenancy_world):  # noqa: F811
    from services import transfer_plan_store

    c = tenancy_world.a
    r = c.post("/api/v1/transfer/plans", json={
        "name": "w1sec-plan", "source": _endpoint("h.example.com", PW),
        "destination": _endpoint("h2.example.com", PW + "2"),
        "source_columns": ["id"], "target_columns": ["id"]})
    assert r.status_code == 200, r.text
    assert PW not in r.text, "the create response returned a raw password"
    pid = r.json()["plan"]["id"]
    got = c.get(f"/api/v1/transfer/plans/{pid}")
    assert got.status_code == 200 and PW not in got.text, got.text[:400]
    assert got.json()["plan"]["source"]["password"] == "****"
    c.post(f"/api/v1/transfer/plans/{pid}/mappings", json={"mappings": [{"source": "id", "target": "id"}]})
    payload = c.get(f"/api/v1/transfer/plans/{pid}/run-payload")
    assert PW not in payload.text, payload.text[:400]
    assert PW not in Path(transfer_plan_store.STORE_PATH).read_text(), "the plan store holds a raw password"
    # The server-side plan still has what Validate needs to reach the endpoints.
    with tenancy_world.as_workspace(tenancy_world.workspace_a):
        plan = transfer_plan_store.get_plan(pid)
    assert plan.source["password"] == PW and plan.destination["extra"]["client_secret"] == PW + "2-extra"
    assert PW in plan.source["connection_string"]


def test_patching_a_plan_with_the_masked_endpoint_keeps_the_credential(tenancy_world):  # noqa: F811
    from services import transfer_plan_store

    c = tenancy_world.a
    pid = c.post("/api/v1/transfer/plans", json={
        "name": "w1sec-plan2", "source": _endpoint("h.example.com", PW),
        "source_columns": ["id"], "target_columns": ["id"]}).json()["plan"]["id"]
    shown = c.get(f"/api/v1/transfer/plans/{pid}").json()["plan"]["source"]
    r = c.patch(f"/api/v1/transfer/plans/{pid}", json={"source": {**shown, "table": "t2"}})
    assert r.status_code == 200 and PW not in r.text, r.text[:300]
    with tenancy_world.as_workspace(tenancy_world.workspace_a):
        plan = transfer_plan_store.get_plan(pid)
    assert plan.source["password"] == PW and plan.source["table"] == "t2"
    assert PW in plan.source["connection_string"] and plan.source["extra"]["client_secret"] == PW + "-extra"
