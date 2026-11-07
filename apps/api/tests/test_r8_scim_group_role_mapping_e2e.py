"""SCIM group -> workspace role mappings, through the real SCIM and IAM routes on Mongo.

QA (production 557f556c, enterprise report #27): with a mapping
``qa-restore-admin -> admin`` a SCIM user plus a group holding that user left
the member's workspace role unchanged. Root cause: ``scim_service`` skipped any
member who already belonged to the workspace (``old_managed_role is None and
current_role: continue``), so a mapped group could only ever add *new* people
and never set the role of an existing member — the owner QA tried to restore.

Contract after the fix:

* a mapped group sets the member's workspace role when membership changes, on
  group create (user added) and on group PATCH;
* an existing manual membership is adopted and its earlier role is restored
  (not deleted) when the member leaves the last mapped group;
* the role is capped by the SCIM caller's own grant: never above the token's
  role, and a token bound to one workspace grants nothing in another; a
  mapping itself can only be saved by someone holding that role there;
* a mapping that would remove the workspace's last admin is refused (409),
  consistent with ``team_store``'s last-admin protection.
"""

from __future__ import annotations

from r8_ent_harness import ent  # noqa: F401

SCIM_CT = "application/scim+json"


def _scim(ent, token: str):
    client = ent.bearer(token)
    client.headers["Content-Type"] = SCIM_CT
    return client


def _role(ent, workspace_id: str, email: str) -> str:
    res = ent.admin.get(f"/api/v1/team/workspaces/{workspace_id}/members")
    assert res.status_code == 200, res.text
    body = res.json()
    rows = body["members"] if isinstance(body, dict) else body
    for row in rows:
        if row["email"] == email:
            return row["role"]
    return ""


def _mint_scim_token(client, workspace_id: str | None = None) -> str:
    headers = {"X-Workspace-Id": workspace_id} if workspace_id else {}
    res = client.post("/api/v1/iam/scim-token", json={"name": "r8 scim"}, headers=headers)
    assert res.status_code == 200, res.text
    return res.json()["key"]


def _map(client, mappings: dict, workspace_id: str | None = None):
    headers = {"X-Workspace-Id": workspace_id} if workspace_id else {}
    return client.put("/api/v1/iam/scim/group-mappings", json={"mappings": mappings}, headers=headers)


def _scim_user(scim, email: str) -> str:
    res = scim.post("/api/v1/scim/v2/Users", json={"userName": email, "displayName": email})
    assert res.status_code == 201, res.text
    return res.json()["id"]


def test_mapped_group_sets_role_of_existing_member_and_restores_it(ent):
    ws = ent.workspace("SCIM restore")
    owner = "owner-r8@example.com"
    ent.member(email=owner, workspace_id=ws, role="viewer")  # QA: owner demoted to viewer
    token = _mint_scim_token(ent.admin)
    assert _map(ent.admin, {"qa-restore-admin": {"workspace_id": ws, "role": "admin"}}).status_code == 200
    scim = _scim(ent, token)

    owner_id = _scim_user(scim, owner)
    # User added to a mapped group at creation -> role set.
    created = scim.post(
        "/api/v1/scim/v2/Groups",
        json={"displayName": "qa-restore-admin", "members": [{"value": owner_id}]},
    )
    assert created.status_code == 201, created.text
    assert _role(ent, ws, owner) == "admin"

    # Leaving the group restores the manual role instead of deleting the member.
    removed = scim.patch(
        f"/api/v1/scim/v2/Groups/{created.json()['id']}",
        json={"Operations": [{"op": "remove", "path": f'members[value eq "{owner_id}"]'}]},
    )
    assert removed.status_code == 200, removed.text
    assert _role(ent, ws, owner) == "viewer"

    events = ent.audit(limit=500)
    changes = [e for e in events if e["action"] == "scim.group.role_change"]
    assert {(e["details"].get("role_before"), e["details"].get("role_after")) for e in changes} >= {
        ("viewer", "admin"),
        ("admin", "viewer"),
    }


def test_group_patch_add_sets_role_and_remove_revokes_it(ent):
    ws = ent.workspace("SCIM patch")
    analyst = "analyst-r8@example.com"
    ent.member(email=analyst, workspace_id=ws, role="viewer")
    token = _mint_scim_token(ent.admin)
    assert _map(ent.admin, {"Data Editors": {"workspace_id": ws, "role": "editor"}}).status_code == 200
    scim = _scim(ent, token)
    email = "new-hire-r8@example.com"
    uid = _scim_user(scim, email)
    analyst_id = _scim_user(scim, analyst)
    group = scim.post("/api/v1/scim/v2/Groups", json={"displayName": "Data Editors"})
    assert group.status_code == 201, group.text
    assert _role(ent, ws, email) == ""

    added = scim.patch(
        f"/api/v1/scim/v2/Groups/{group.json()['id']}",
        json={"Operations": [{"op": "add", "path": "members",
                              "value": [{"value": uid}, {"value": analyst_id}]}]},
    )
    assert added.status_code == 200, added.text
    assert _role(ent, ws, email) == "editor"  # new member: added with the mapped role
    assert _role(ent, ws, analyst) == "editor"  # existing member: role set by the group

    gone = scim.patch(
        f"/api/v1/scim/v2/Groups/{group.json()['id']}",
        json={"Operations": [{"op": "remove", "path": "members",
                              "value": [{"value": uid}, {"value": analyst_id}]}]},
    )
    assert gone.status_code == 200, gone.text
    assert _role(ent, ws, email) == ""  # SCIM brought them in, SCIM takes them out
    assert _role(ent, ws, analyst) == "viewer"  # manual membership back to its own role


def test_mapping_cannot_exceed_the_scim_tokens_own_grant(ent):
    w1 = ent.workspace("SCIM home")
    w2 = ent.workspace("SCIM other")
    ws_admin = ent.member(email="w1-admin-r8@example.com", workspace_id=w1, role="admin")

    # A workspace admin may not map a group to admin of a workspace they don't run.
    denied = _map(ws_admin, {"w2-admins": {"workspace_id": w2, "role": "admin"}}, workspace_id=w1)
    assert denied.status_code == 403, denied.text

    # The platform admin configures both mappings. The caller is an admin key
    # bound to W1: its own grant is admin *in W1* and nothing elsewhere.
    assert _map(
        ent.admin,
        {
            "w2-admins": {"workspace_id": w2, "role": "admin"},
            "w1-admins": {"workspace_id": w1, "role": "admin"},
        },
    ).status_code == 200
    token = ent.api_key(role="admin", workspace_id=w1, name="w1 provisioning")
    scim = _scim(ent, token)
    email = "provisioned-r8@example.com"
    uid = _scim_user(scim, email)

    other = scim.post(
        "/api/v1/scim/v2/Groups",
        json={"displayName": "w2-admins", "members": [{"value": uid}]},
    )
    assert other.status_code == 201, other.text
    assert _role(ent, w2, email) == ""  # no authority in W2: nothing granted

    home = scim.post(
        "/api/v1/scim/v2/Groups",
        json={"displayName": "w1-admins", "members": [{"value": uid}]},
    )
    assert home.status_code == 201, home.text
    assert _role(ent, w1, email) == "admin"

    capped = [e for e in ent.audit(limit=500) if e["action"] == "scim.group.role_capped"]
    assert capped and capped[0]["details"]["workspace_id"] == w2
    assert capped[0]["outcome"] == "skipped"


def test_mapping_never_removes_the_last_workspace_admin(ent):
    ws = ent.workspace("SCIM last admin")
    solo = "solo-admin-r8@example.com"
    ent.member(email=solo, workspace_id=ws, role="admin")
    from r8_ent_harness import ADMIN_EMAIL

    res = ent.admin.delete(f"/api/v1/team/workspaces/{ws}/members/{ADMIN_EMAIL}")
    assert res.status_code == 200, res.text
    token = _mint_scim_token(ent.admin)
    assert _map(ent.admin, {"Readers": {"workspace_id": ws, "role": "viewer"}}).status_code == 200
    scim = _scim(ent, token)
    uid = _scim_user(scim, solo)
    group = scim.post("/api/v1/scim/v2/Groups", json={"displayName": "Readers"})
    assert group.status_code == 201, group.text

    refused = scim.patch(
        f"/api/v1/scim/v2/Groups/{group.json()['id']}",
        json={"Operations": [{"op": "add", "path": "members", "value": [{"value": uid}]}]},
    )
    assert refused.status_code == 409, refused.text
    assert refused.json()["scimType"] == "mutability"
    assert _role(ent, ws, solo) == "admin"
