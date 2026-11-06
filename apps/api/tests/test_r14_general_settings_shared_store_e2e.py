"""The general settings document is shared by every replica (control-plane store).

It used to be ``data_dir()/workspace.json`` on each replica: on a multi-replica
deployment replicas disagreed, a change made through one was invisible to the
others (the retention purge beat on another replica kept its own value) and
the audit's from/to described one replica's file. Now, whenever a
control-plane database is configured, the document lives there; the file is
kept only for a deployment with no database, and an existing file seeds the
shared store once, first writer wins, audited.

Live MongoDB on :27017. A "replica" is its own ``MongoDBService`` connection
(over the same database) and its own data directory / settings file.
"""

from __future__ import annotations

import json
import uuid

import pytest

from r8_ent_harness import MONGO_URI, ent  # noqa: F401


class Replicas:
    def __init__(self, monkeypatch, tmp_path, db_name: str):
        from services import mongodb_service

        self.monkeypatch = monkeypatch
        self.tmp = tmp_path
        self.db_name = db_name
        self.svcs = {}
        for name in ("a", "b"):
            svc = mongodb_service.MongoDBService(MONGO_URI)
            svc.db_name = db_name
            assert svc.connect()
            self.svcs[name] = svc
            (tmp_path / name).mkdir(exist_ok=True)

    def use(self, name: str) -> None:
        """Run the next calls as replica ``name``: its connection, its disk."""
        from services import mongodb_service, workspace_store

        self.monkeypatch.setattr(mongodb_service, "_mongodb_service", self.svcs[name])
        self.monkeypatch.setenv("DATAFLOW_DATA_DIR", str(self.tmp / name))
        self.monkeypatch.setattr(workspace_store, "STORE_PATH", self.tmp / name / "workspace.json")

    def file(self, name: str):
        return self.tmp / name / "workspace.json"

    @property
    def db(self):
        return self.svcs["a"].get_database()


def _mongo_or_skip():
    pymongo = pytest.importorskip("pymongo")
    probe = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
    try:
        probe.admin.command("ping")
    except Exception:  # noqa: BLE001
        pytest.skip("MongoDB not reachable")
    finally:
        probe.close()


@pytest.fixture
def replicas(monkeypatch, tmp_path):
    _mongo_or_skip()
    name = f"r14_settings_{uuid.uuid4().hex[:10]}"
    world = Replicas(monkeypatch, tmp_path, name)
    try:
        yield world
    finally:
        world.svcs["a"].client.drop_database(name)


def test_two_replicas_over_one_store_see_one_value(replicas):
    from services.workspace_store import get_workspace_settings, update_workspace_settings

    replicas.use("a")
    update_workspace_settings(org_name="Northwind Ünïcode ✓", retention_days=45,
                              retention_auto_purge=True, actor="admin-a@example.com")
    replicas.use("b")
    seen = get_workspace_settings()
    assert seen["org_name"] == "Northwind Ünïcode ✓"
    assert seen["retention_days"] == 45 and seen["retention_auto_purge"] is True
    assert seen["updated_by"] == "admin-a@example.com"
    # Replica B writes another field; A's values survive (one $set per change).
    update_workspace_settings(studio_canvas=True, actor="admin-b@example.com")
    replicas.use("a")
    after = get_workspace_settings()
    assert after["org_name"] == "Northwind Ünïcode ✓" and after["studio_canvas"] is True
    # Neither replica wrote a local file: the shared store is the document.
    assert not replicas.file("a").exists() and not replicas.file("b").exists()


def test_migration_merges_every_file_deterministically_and_retires_them(replicas):
    """Second review (S3): the first writer's file won (boot order), a replica
    with no file that wrote first kept every other file out, and a file could
    re-arm after the fact. Now each file is a merge source: per field the
    newest file wins, a destructive toggle never seeds on, the retention window
    seeds as the longest any file held, writes
    keep the merge for fields they do not name, and a contributed file is
    renamed so it can never be read again."""
    from services.workspace_store import get_workspace_settings, update_workspace_settings

    newer = {
        "org_name": "Contoso", "timezone": "Europe/Berlin", "retention_days": 30,
        "retention_auto_purge": True, "studio_schema_browser": True, "studio_canvas": False,
        "updated_at": "2026-10-02T00:00:00+00:00", "updated_by": "new-admin@example.com",
    }
    older = {
        "org_name": "Old name", "timezone": "Asia/Tokyo", "retention_days": 365,
        "retention_auto_purge": False, "studio_canvas": True,
        "updated_at": "2026-09-01T00:00:00+00:00", "updated_by": "old-admin@example.com",
    }
    replicas.file("a").write_text(json.dumps(newer), encoding="utf-8")
    replicas.file("b").write_text(json.dumps(older), encoding="utf-8")
    # A replica with no file writes first: it must not keep the files out.
    (replicas.tmp / "c").mkdir()
    replicas.svcs["c"] = replicas.svcs["a"]
    replicas.use("c")
    update_workspace_settings(studio_schema_browser=False, actor="admin-c@example.com")
    for order in (("b", "a"), ("a", "b")):
        for name in order:
            replicas.use(name)
            seen = get_workspace_settings()
    assert seen["org_name"] == "Contoso" and seen["timezone"] == "Europe/Berlin"
    assert seen["retention_days"] == 365, "the retention window seeded shorter than a file held"
    assert seen["retention_auto_purge"] is False, "a stale file armed the deployment-wide purge"
    assert seen["studio_canvas"] is False  # newest file wins
    assert seen["studio_schema_browser"] is False  # the shared write wins over the files
    for name in ("a", "b"):
        assert not replicas.file(name).exists()
        assert (replicas.tmp / name / "workspace.json.migrated").exists()
    migrated = list(replicas.db["audit_events"].find({"action": "workspace.settings.migrated"}))
    assert len(migrated) == 2, migrated
    assert {m["details"]["values"]["org_name"] for m in migrated} == {"Contoso", "Old name"}


def test_a_single_stale_file_never_arms_the_purge(replicas):
    """Second review: whichever replica's file was read first could turn on
    retention_auto_purge deployment-wide."""
    from services.job_retention_schedule import auto_purge_enabled
    from services.workspace_store import get_workspace_settings

    replicas.file("b").write_text(
        json.dumps({"retention_auto_purge": True, "retention_days": 1, "org_name": "Stale"}),
        encoding="utf-8",
    )
    replicas.use("b")
    seen = get_workspace_settings()
    assert seen["org_name"] == "Stale"
    assert seen["retention_auto_purge"] is False and auto_purge_enabled() is False


def test_a_down_store_fails_closed_never_a_stale_file(replicas, monkeypatch, ent):
    """Second review (BLOCKING): with replica b's client down, b read its old
    file (auto_purge on, 1 day) and the purge beat ran on it; a PATCH landed in
    b's file only."""
    from services import mongodb_service, workspace_store
    from services.job_retention_schedule import auto_purge_enabled, run_due_retention
    from services.workspace_store import SettingsUnavailable, update_workspace_settings

    monkeypatch.setenv("MONGODB_URI", MONGO_URI)
    replicas.use("a")
    update_workspace_settings(retention_auto_purge=False, retention_days=365, actor="a@example.com")
    replicas.use("b")
    stale = {"retention_auto_purge": True, "retention_days": 1}
    replicas.file("b").write_text(json.dumps(stale), encoding="utf-8")
    replicas.svcs["b"].client = None  # the connection is down, and stays down
    monkeypatch.setattr(replicas.svcs["b"], "connect", lambda: False)
    with pytest.raises(SettingsUnavailable):
        workspace_store.get_workspace_settings()
    assert auto_purge_enabled() is False
    assert run_due_retention(force=True) is None
    with pytest.raises(SettingsUnavailable):
        update_workspace_settings(retention_auto_purge=True, actor="b@example.com")
    assert json.loads(replicas.file("b").read_text(encoding="utf-8")) == stale
    # Through the real route: 503, nothing written anywhere.
    monkeypatch.setattr(mongodb_service, "_mongodb_service", replicas.svcs["b"])
    # Turning the purge on is refused before any write (no reachable store:
    # sec-23's 422, or this store's 503); any other change is a 503.
    res = ent.admin.patch("/api/v1/workspace/settings", json={"retention_auto_purge": True})
    assert res.status_code in (422, 503), res.text
    res = ent.admin.patch("/api/v1/workspace/settings", json={"org_name": "From b"})
    assert res.status_code == 503, res.text
    assert ent.admin.get("/api/v1/workspace/settings").status_code == 503
    replicas.use("a")
    assert workspace_store.get_workspace_settings()["retention_auto_purge"] is False


def test_file_only_deployment_behaves_as_before(tmp_path, monkeypatch):
    """No control-plane database: the node-local file, read and written as before."""
    from services import workspace_store

    monkeypatch.setattr(workspace_store, "STORE_PATH", tmp_path / "workspace.json")
    assert workspace_store.get_workspace_settings()["org_name"] == "Datawrap"
    workspace_store.update_workspace_settings(org_name="Fabrikam", retention_days=7, actor="x@example.com")
    on_disk = json.loads((tmp_path / "workspace.json").read_text(encoding="utf-8"))
    assert on_disk["org_name"] == "Fabrikam" and on_disk["retention_days"] == 7
    assert on_disk["updated_by"] == "x@example.com"
    assert workspace_store.get_workspace_settings()["retention_days"] == 7


def test_on_through_one_replica_off_through_the_other_is_audited_and_the_beat_follows(ent, monkeypatch):
    """Real routes on the ent harness; replica B is a second connection to the
    same database with its own data directory."""
    from services import mongodb_service, workspace_store
    from services.job_retention_schedule import auto_purge_enabled

    replica_b = mongodb_service.MongoDBService(MONGO_URI)
    replica_b.db_name = ent.db.name
    assert replica_b.connect()
    replica_a = mongodb_service.get_mongodb_service()
    seen = ent.db["audit_events"].count_documents({"action": "workspace.settings.update"})
    on = ent.admin.patch("/api/v1/workspace/settings", json={"retention_auto_purge": True})
    assert on.status_code == 200, on.text
    (ent.tmp / "replica-b").mkdir()
    monkeypatch.setattr(mongodb_service, "_mongodb_service", replica_b)
    monkeypatch.setattr(workspace_store, "STORE_PATH", ent.tmp / "replica-b" / "workspace.json")
    assert auto_purge_enabled() is True, "replica B's beat does not see the setting"
    off = ent.admin.patch("/api/v1/workspace/settings", json={"retention_auto_purge": False})
    assert off.status_code == 200, off.text
    monkeypatch.setattr(mongodb_service, "_mongodb_service", replica_a)
    assert auto_purge_enabled() is False, "replica A's beat still purges"
    updates = list(
        ent.db["audit_events"].find({"action": "workspace.settings.update"}).sort("chain_seq", 1)
    )[seen:]
    changes = [u["details"]["changes"].get("retention_auto_purge") for u in updates]
    assert changes == [{"from": False, "to": True}, {"from": True, "to": False}], changes


# ── Third review (6b131dd08) ───────────────────────────────────────────────


def test_a_boot_time_outage_fails_closed_and_the_client_reconnects(replicas, monkeypatch):
    """A client that never connected and no URI env: the file was used. Now a
    node that reached the store before fails closed, and the client retries."""
    import services.control_plane_store as cps
    from services.workspace_store import SettingsUnavailable, get_workspace_settings, update_workspace_settings

    for key in ("MONGODB_URI", "MONGO_URL", "MONGO_PRIVATE_URL", "MONGODB_URL", "MONGO_PUBLIC_URL"):
        monkeypatch.delenv(key, raising=False)
    replicas.use("b")
    update_workspace_settings(org_name="Shared", actor="a@example.com")  # b reached the store
    replicas.file("b").write_text(json.dumps({"org_name": "Stale file", "retention_auto_purge": True}),
                                  encoding="utf-8")
    replicas.svcs["b"].client = None  # the next boot: the connect failed
    monkeypatch.setattr(replicas.svcs["b"], "connect", lambda: False)
    monkeypatch.setattr(cps, "_last_reconnect", [0.0])
    with pytest.raises(SettingsUnavailable):
        get_workspace_settings()
    # The database comes back: the client reconnects on its own.
    real = __import__("services.mongodb_service", fromlist=["MongoDBService"]).MongoDBService.connect
    monkeypatch.setattr(replicas.svcs["b"], "connect", lambda: real(replicas.svcs["b"]))
    monkeypatch.setattr(cps, "_last_reconnect", [0.0])
    assert get_workspace_settings()["org_name"] == "Shared"


def test_a_c32b61c4c_seed_never_keeps_the_first_files_purge_switch(replicas):
    from services.workspace_store import get_workspace_settings, update_workspace_settings

    replicas.use("a")
    replicas.db["workspace_settings"].insert_one({
        "_id": "general", "org_name": "Seeded", "retention_auto_purge": True, "retention_days": 1,
        "_migrated_from": "/data/workspace.json", "_migrated_at": "2026-11-06T00:00:00+00:00",
    })
    seen = get_workspace_settings()
    assert seen["org_name"] == "Seeded" and seen["retention_auto_purge"] is False
    update_workspace_settings(retention_auto_purge=True, actor="admin@example.com")
    assert get_workspace_settings()["retention_auto_purge"] is True


def test_a_failed_auto_drop_step_still_audits_the_settings_written(ent, monkeypatch):
    import services.cdc_capture_inventory as inv
    from services.control_plane_store import ControlPlaneUnavailable

    seen = ent.db["audit_events"].count_documents({"action": "workspace.settings.update"})

    def down(*_a, **_k):
        raise ControlPlaneUnavailable("down")

    monkeypatch.setattr(inv, "set_auto_drop", down)
    res = ent.admin.patch("/api/v1/workspace/settings",
                          json={"org_name": "Audited Ünï", "cdc_orphan_auto_drop": True})
    assert res.status_code == 503, res.text
    assert "mongodb://" not in res.text and "localhost" not in res.text, res.text
    updates = list(ent.db["audit_events"].find({"action": "workspace.settings.update"}))[seen:]
    assert len(updates) == 1, updates
    assert updates[0]["details"]["changes"]["org_name"]["to"] == "Audited Ünï"


def test_health_reports_the_settings_store_not_the_file(ent):
    from services.health_service import _workspace_settings_readable

    assert _workspace_settings_readable(ent.tmp) is True  # shared store, no file on disk
