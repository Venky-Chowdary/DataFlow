"""Settings -> Security -> Download report is scoped to the workspace (QA enterprise #49).

QA (production 557f556c): the downloaded report said "Workspace ID: none —
request carried no workspace scope" although the request named the workspace.
Root cause: ``get_security_report`` printed ``posture['workspace_id']``, which
``_security_posture`` only fills from a *tenant* profile; a workspace without
a tenant (QA's: creating one is irreversible) always read as unscoped.
"""

from __future__ import annotations

from r8_ent_harness import ent  # noqa: F401


def test_report_names_the_requesting_workspace(ent):
    ws = ent.workspace("Report Co")
    admin = ent.member(email="report-admin-r8@example.com", workspace_id=ws, role="admin")
    res = admin.get("/api/v1/workspace/security/report")
    assert res.status_code == 200, res.text
    text = res.text
    assert f"Workspace ID: {ws}" in text
    assert "Workspace: Report Co" in text
    assert "no workspace scope" not in text

    posture = admin.get("/api/v1/workspace/security/posture").json()
    assert posture["scope_workspace_id"] == ws


def test_report_for_another_workspace_is_refused(ent):
    mine = ent.workspace("Report mine")
    other = ent.workspace("Report other")
    admin = ent.member(email="report-scope-r8@example.com", workspace_id=mine, role="admin")
    res = admin.get("/api/v1/workspace/security/report", headers={"X-Workspace-Id": other})
    assert res.status_code == 403
    assert f"Workspace ID: {mine}" in admin.get("/api/v1/workspace/security/report").text
