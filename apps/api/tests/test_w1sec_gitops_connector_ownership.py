"""SEC-5: GitOps import and apply run the POST /schedules connector-ownership check.

``/schedules/import`` and ``/schedules/gitops/apply`` saved a schedule in
workspace B over workspace A's saved connectors (201 / applied:1) where
``POST /schedules`` with the same body answers 404.
"""

from __future__ import annotations

from tests.tenancy_world import tenancy_world  # noqa: F401
from tests.test_tenancy_route_scoping import SEEDERS, world  # noqa: F401


def _spec(source: str, dest: str, name: str) -> dict:
    return {"apiVersion": "datawrap.io/v1", "kind": "PipelineSchedule", "metadata": {"name": name},
            "spec": {"name": name, "interval": "daily", "enabled": False,
                     "source_connector_id": source, "source_table": "t",
                     "dest_connector_id": dest, "dest_table": "w1sec_t",
                     "mappings": [{"source": "id", "target": "id", "confidence": 1.0, "user_override": True}]}}


def _stored(name: str) -> list:
    from services import schedule_store

    return [s for s in schedule_store.list_schedules() if s.name == name]


def test_import_refuses_another_workspaces_connector(world):  # noqa: F811
    a_conn = SEEDERS["saved_connector"](world).params["connector_id"]
    r = world.admin_b.post("/api/v1/schedules/import", json=_spec(a_conn, a_conn, "w1sec-import"))
    assert r.status_code in (403, 404), r.text
    assert not _stored("w1sec-import")
    control = world.admin_b.post("/api/v1/schedules", json=_spec(a_conn, a_conn, "w1sec-post")["spec"])
    assert control.status_code == r.status_code, (control.text, r.text)


def test_gitops_apply_refuses_another_workspaces_connector_and_applies_nothing(world):  # noqa: F811
    from services import connector_store

    a_conn = SEEDERS["saved_connector"](world).params["connector_id"]
    b_conn = connector_store.create_connector({
        "name": "w1sec-b", "type": "sqlite", "database": ":memory:", "workspace_id": world.workspace_b}).id
    manifest = {"apiVersion": "datawrap.io/v1", "kind": "DatawrapManifest",
                "resources": [_spec(b_conn, b_conn, "w1sec-own"), _spec(a_conn, b_conn, "w1sec-gitops")]}
    r = world.admin_b.post("/api/v1/schedules/gitops/apply", json=manifest)
    assert r.status_code in (403, 404), r.text
    assert not _stored("w1sec-gitops") and not _stored("w1sec-own"), "a refused manifest applied a resource"


def test_owner_can_still_import_and_apply(world):  # noqa: F811
    a_conn = SEEDERS["saved_connector"](world).params["connector_id"]
    r = world.admin_a.post("/api/v1/schedules/import", json=_spec(a_conn, a_conn, "w1sec-own-import"))
    assert r.status_code == 201, r.text
    manifest = {"apiVersion": "datawrap.io/v1", "kind": "DatawrapManifest",
                "resources": [_spec(a_conn, a_conn, "w1sec-own-apply")]}
    r = world.admin_a.post("/api/v1/schedules/gitops/apply", json=manifest)
    assert r.status_code == 200 and r.json()["applied"] == 1, r.text
