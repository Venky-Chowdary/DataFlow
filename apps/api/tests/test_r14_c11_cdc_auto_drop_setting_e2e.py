"""C11: dropping orphaned CDC slots automatically is opt-in, platform-admin only, audited.

The scheduler beat drops a Datawrap-owned replication slot whose source tables
stayed gone only when the deployment setting ``cdc_orphan_auto_drop`` is on.
It is off by default (a table dropped and recreated in a migration window, or
restored from a backup, would otherwise lose its slot and force a full
re-snapshot). Through the real settings route on the live MongoDB
(``r8_ent_harness``): off by default, a workspace admin cannot turn it on,
and every change records ``cdc.capture.auto_drop.changed`` with from/to.
"""

from __future__ import annotations

from r8_ent_harness import ent  # noqa: F401


def _toggles(ent) -> list[dict]:
    return list(
        ent.db["audit_events"].find({"action": "cdc.capture.auto_drop.changed"}).sort("chain_seq", 1)
    )


def test_auto_drop_is_off_by_default_admin_only_and_every_change_is_audited(ent):
    ws = ent.workspace("R14 C11 auto drop")
    settings = ent.admin.get("/api/v1/workspace/settings", headers={"X-Workspace-Id": ws}).json()
    assert settings["cdc_orphan_auto_drop"] is False
    seen = len(_toggles(ent))

    admin = ent.member(email="c11-ws-admin-r14@example.com", workspace_id=ws, role="admin")
    refused = admin.patch("/api/v1/workspace/settings", json={"cdc_orphan_auto_drop": True})
    assert refused.status_code == 403, refused.text
    assert len(_toggles(ent)) == seen

    try:
        on = ent.admin.patch("/api/v1/workspace/settings", json={"cdc_orphan_auto_drop": True},
                             headers={"X-Workspace-Id": ws})
        assert on.status_code == 200, on.text
        assert on.json()["cdc_orphan_auto_drop"] is True
        # Saving other settings without changing it records no toggle.
        same = ent.admin.patch("/api/v1/workspace/settings", json={"retention_days": 90},
                               headers={"X-Workspace-Id": ws})
        assert same.status_code == 200, same.text
    finally:
        off = ent.admin.patch("/api/v1/workspace/settings", json={"cdc_orphan_auto_drop": False},
                              headers={"X-Workspace-Id": ws})
    assert off.status_code == 200 and off.json()["cdc_orphan_auto_drop"] is False
    changes = [e.get("details") or {} for e in _toggles(ent)[seen:]]
    assert [(c.get("from"), c.get("to")) for c in changes] == [(False, True), (True, False)], changes
    assert all(c.get("target") == "cdc_orphan_auto_drop" for c in changes), changes


def test_the_setting_is_shared_across_replicas_and_each_change_is_audited_once(ent, monkeypatch):
    """Review repro (S7): the setting lived in the per-replica workspace.json, so
    on-then-off through two replicas left one replica dropping and the off went
    unaudited. Replica B here is a process with its own empty data directory."""
    import services.workspace_store as workspace_store
    from services.cdc_capture_inventory import auto_drop_enabled

    seen = len(_toggles(ent))
    try:
        assert ent.admin.patch("/api/v1/workspace/settings",
                               json={"cdc_orphan_auto_drop": True}).status_code == 200
        replica_b = ent.tmp / "replica-b"
        replica_b.mkdir()
        monkeypatch.setenv("DATAFLOW_DATA_DIR", str(replica_b))
        monkeypatch.setattr(workspace_store, "STORE_PATH", replica_b / "workspace.json")
        assert auto_drop_enabled() is True, "replica B does not see the setting"
        assert ent.admin.get("/api/v1/workspace/settings").json()["cdc_orphan_auto_drop"] is True
    finally:
        off = ent.admin.patch("/api/v1/workspace/settings", json={"cdc_orphan_auto_drop": False})
    assert off.status_code == 200
    assert auto_drop_enabled() is False
    changes = [e.get("details") or {} for e in _toggles(ent)[seen:]]
    assert [(c.get("from"), c.get("to")) for c in changes] == [(False, True), (True, False)], changes
