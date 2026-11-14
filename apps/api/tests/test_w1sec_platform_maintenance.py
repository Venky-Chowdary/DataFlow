"""SEC-10 / SEC-11: platform-wide maintenance and the shared knowledge base are
for platform administrators only.

A viewer emptied the embedding cache of every tenant, an editor pruned every
workspace's journals, a workspace admin started platform-wide training, and
any workspace admin wrote into the RAG store every tenant's Pilot reads.
"""

from __future__ import annotations

from tests.tenancy_world import tenancy_world  # noqa: F401


def test_tenants_cannot_run_platform_maintenance(tenancy_world):  # noqa: F811
    w = tenancy_world
    viewer = w.member("viewer-g@tenancy.example.com", w.workspace_b, "viewer")
    admin_b = w.member("admin-g@tenancy.example.com", w.workspace_b, "admin")
    assert viewer.delete("/api/v1/ai/embedding-cache").status_code == 403
    assert admin_b.delete("/api/v1/ai/embedding-cache").status_code == 403
    assert w.b.post("/api/v1/journals/prune").status_code == 403
    assert admin_b.post("/api/v1/journals/prune").status_code == 403
    for path in ("/api/v1/copilot/train", "/api/v1/training-agent/run", "/api/v1/training-agent/run/sync"):
        r = admin_b.post(path, json={"force": False})
        assert r.status_code == 403, (path, r.status_code, r.text[:200])
    r = admin_b.post("/api/v1/ai/rag/ingest", json={
        "schema_name": "w1sec_b_private", "columns": {"ssn": ["123-45-6789"]}, "industry": "finance"})
    assert r.status_code == 403, r.text[:200]


def test_the_platform_admin_still_can_and_no_server_path_is_shown(tenancy_world):  # noqa: F811
    w = tenancy_world
    stats = w.a.get("/api/v1/ai/embedding-cache")
    assert stats.status_code == 200 and '"path"' not in stats.text, stats.text[:200]
    assert w.admin.delete("/api/v1/ai/embedding-cache").status_code == 200
    assert w.admin.post("/api/v1/journals/prune").status_code == 200
