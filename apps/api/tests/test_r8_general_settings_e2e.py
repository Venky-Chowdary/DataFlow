"""Settings -> General, through the real routes (QA enterprise #2 and #3).

QA (production 557f556c):

* #2 ``PATCH /workspace/settings {timezone: "Mars/Olympus"}`` was accepted and
  saved. Root cause: ``workspace_store.update_workspace_settings`` stored any
  string up to 64 characters.
* #3 ``updated_at`` / ``updated_by`` read back ``null`` after a change. Root
  cause: the store *wrote* them, but ``_load_raw`` kept only keys present in
  ``_DEFAULTS``, so every read dropped them.
"""

from __future__ import annotations

from r8_ent_harness import ADMIN_EMAIL, ent  # noqa: F401


def test_timezone_must_be_an_iana_zone(ent):
    ws = ent.workspace("General tz")
    admin = ent.member(email="general-admin-r8@example.com", workspace_id=ws, role="admin")
    for bad in ("Mars/Olympus", "Not A Zone", "../../etc/passwd", "America/Atlantis"):
        res = admin.patch("/api/v1/workspace/settings", json={"timezone": bad})
        assert res.status_code == 400, (bad, res.text)
        assert "IANA" in res.json()["detail"]
    assert admin.get("/api/v1/workspace/settings").json()["timezone"] == "UTC"

    ok = admin.patch("/api/v1/workspace/settings", json={"timezone": "America/New_York"})
    assert ok.status_code == 200, ok.text
    assert ok.json()["timezone"] == "America/New_York"


def test_every_change_records_when_and_by_whom(ent):
    ws = ent.workspace("General attribution")
    admin_email = "general-attr-r8@example.com"
    admin = ent.member(email=admin_email, workspace_id=ws, role="admin")
    viewer = ent.member(email="general-viewer-r8@example.com", workspace_id=ws, role="viewer")

    first = admin.patch("/api/v1/workspace/settings", json={"org_name": "Acme QA"})
    assert first.status_code == 200, first.text
    assert first.json()["updated_by"] == admin_email
    assert first.json()["updated_at"]

    read = viewer.get("/api/v1/workspace/settings").json()
    assert read["updated_by"] == admin_email
    assert read["updated_at"] == first.json()["updated_at"]

    second = ent.admin.patch(
        "/api/v1/workspace/settings", json={"retention_days": 30}, headers={"X-Workspace-Id": ws}
    )
    assert second.status_code == 200, second.text
    read = viewer.get("/api/v1/workspace/settings").json()
    assert read["updated_by"] == ADMIN_EMAIL
    assert read["updated_at"] > first.json()["updated_at"]
