"""Ops review round 2, High 2: a crashed import must not wedge a store forever.

The process that imports its node's file into an empty shared store claims
the store (``state: importing``) and every replica fails closed until the
import is done. The claim had no owner and no expiry, so a process killed
mid-import left every replica answering 503 for that store, forever; only a
restart with the identical file recovered. The claim now has an owner and a
lease that the importer renews as it writes. Once the lease runs out another
process takes the claim over: with the same file it finishes the import
(idempotently), otherwise it removes the half-imported records and the store
is usable again (and a process with its own file imports that one).
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from services import mongodb_service, shared_doc_store, tenant_store
from services.control_plane_store import ControlPlaneUnavailable
from tests._control_plane_fakes import install_shared_store

ITEMS = "control_plane_items"
DOCS = shared_doc_store.DOCS_COLLECTION
API_ROOT = Path(__file__).resolve().parents[1]


def _tenants_file(path: Path, names: list[str]) -> str:
    body = json.dumps({"tenants": [{"id": f"t-{n}", "workspace_id": f"ws-{n}", "name": n} for n in names]})
    path.write_text(body, encoding="utf-8")
    return hashlib.sha256(body.encode()).hexdigest()


def _fresh_process(monkeypatch, tmp_path, *, lease: str = "30") -> None:
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    for flag in ("_settled", "_split_checked"):
        monkeypatch.setattr(tenant_store._DOC, flag, False)
    monkeypatch.setattr(tenant_store._DOC, "_imported", set())
    monkeypatch.setattr(tenant_store._DOC, "_seen_stamps", set())
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_IMPORT_LEASE_SECONDS", lease)


def _crashed_import(db, digest: str, *, lease_left: float) -> None:
    """What a process killed after its claim and its first record leaves behind."""
    db[DOCS].insert_one({"_id": "tenant_settings", "kind": "store", "import_sources": [digest], "state": "importing",
                         "node": "pod-a", "owner": "pod-a:1:dead", "lease_until": time.time() + lease_left,
                         "at": time.time() - 60})
    body = json.dumps({"id": "t-half", "workspace_id": "ws-half", "name": "half"})
    db[ITEMS].insert_one({"_id": "tenant_settings:t-half", "store": "tenant_settings", "key": "t-half", "part": "ws-half",
                          "rev": 1, "seq": 0, "body": body, "imp": digest})


def test_reads_fail_closed_while_the_importers_lease_is_live(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    _fresh_process(monkeypatch, tmp_path)
    _crashed_import(db, "a" * 64, lease_left=20)
    with pytest.raises(ControlPlaneUnavailable, match="being imported"):
        tenant_store.list_tenants()


def test_a_crashed_import_is_abandoned_after_its_lease_and_the_store_serves_again(tmp_path, monkeypatch, caplog):
    """Pod B has no file (a rescheduled pod with an emptyDir): it may not wait forever."""
    db = install_shared_store(monkeypatch)
    _fresh_process(monkeypatch, tmp_path)
    _crashed_import(db, "a" * 64, lease_left=-1)
    caplog.set_level("WARNING")
    assert tenant_store.list_tenants() == [], "the half-imported record is not served as if complete"
    assert db[DOCS].find_one({"_id": "tenant_settings"}) is None
    assert list(db[ITEMS].find({"store": "tenant_settings"})) == []
    assert any("abandon" in r.getMessage() for r in caplog.records)
    tenant_store.create_tenant("ws-new", "New")
    assert [t.workspace_id for t in tenant_store.list_tenants()] == ["ws-new"]


def test_a_process_with_the_same_file_completes_a_crashed_import(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    _fresh_process(monkeypatch, tmp_path)
    digest = _tenants_file(tmp_path / "tenants.json", ["a", "b", "c"])
    _crashed_import(db, digest, lease_left=20)
    assert sorted(t.name for t in tenant_store.list_tenants()) == ["a", "b", "c", "half"]
    assert db[DOCS].find_one({"_id": "tenant_settings"})["state"] == "done"


def test_a_process_with_another_file_restarts_the_import_with_its_own_after_the_lease(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    _fresh_process(monkeypatch, tmp_path)
    mine = _tenants_file(tmp_path / "tenants.json", ["c1"])
    _crashed_import(db, "f" * 64, lease_left=-1)
    assert [t.name for t in tenant_store.list_tenants()] == ["c1"]
    meta = db[DOCS].find_one({"_id": "tenant_settings"})
    assert meta["state"] == "done" and meta["import_sources"] == [mine]
    assert tenant_store._DOC.status()["local_file_conflicts"] == []


def test_an_imported_record_over_the_record_limit_is_skipped_not_wedging_the_store(tmp_path, monkeypatch, caplog):
    from services.shared_record_store import MAX_RECORD_BYTES

    db = install_shared_store(monkeypatch)
    _fresh_process(monkeypatch, tmp_path)
    rows = [{"id": "t-ok", "workspace_id": "ws-ok", "name": "ok"},
            {"id": "t-big", "workspace_id": "ws-big", "name": "x" * (MAX_RECORD_BYTES + 10)}]
    (tmp_path / "tenants.json").write_text(json.dumps({"tenants": rows}), encoding="utf-8")
    caplog.set_level("ERROR")
    report = tenant_store._DOC.import_local_file()
    assert report["action"] == "imported" and report["skipped_too_large"] == ["t-big"]
    assert [t.name for t in tenant_store.list_tenants()] == ["ok"]
    assert db[DOCS].find_one({"_id": "tenant_settings"})["state"] == "done"
    assert any("t-big" in r.getMessage() for r in caplog.records)


def test_the_importer_renews_its_lease_while_it_writes(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    _fresh_process(monkeypatch, tmp_path, lease="5")
    _tenants_file(tmp_path / "tenants.json", [f"n{i}" for i in range(4)])
    seen: list[float] = []
    docs = db[DOCS]
    real_update = docs.update_one

    def spy(flt, update, *args, **kwargs):
        lease = (update.get("$set") or {}).get("lease_until")
        if lease and flt.get("_id") == "tenant_settings":
            seen.append(lease)
        return real_update(flt, update, *args, **kwargs)

    monkeypatch.setattr(docs, "update_one", spy)
    tenant_store._DOC.import_local_file()
    assert len(seen) >= 4, "one renewal per record written"
    assert all(lease > time.time() for lease in seen)


_CHILD = r"""
import os, sys, time
os.environ["DATAFLOW_CONTROL_STORE_BACKEND"] = "mongo"
os.environ["DATAFLOW_CONTROL_STORE_CACHE_SECONDS"] = "0"
from services import mongodb_service, control_plane_store, shared_doc_store, shared_record_store
svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
assert svc.connect()
svc.db_name = os.environ["R2F2_DB"]
mongodb_service._mongodb_service = svc
control_plane_store._remember_seen = lambda: None
role = sys.argv[1]
if role == "crash":
    real = shared_record_store.SharedRecordSet._write_imported
    calls = [0]
    def dying(self, *a, **k):
        calls[0] += 1
        if calls[0] == 2:
            os._exit(9)  # killed mid-import: claim and one record written
        return real(self, *a, **k)
    shared_record_store.SharedRecordSet._write_imported = dying
from services import tenant_store
from services.control_plane_store import ControlPlaneUnavailable
for _ in range(int(sys.argv[2]) if len(sys.argv) > 2 else 1):
    try:
        print("tenants", sorted(t.name for t in tenant_store.list_tenants()), flush=True)
    except ControlPlaneUnavailable as exc:
        print("503", exc, flush=True)
    time.sleep(0.5)
"""


def _pod(role: str, data_dir: Path, db_name: str, *, reads: int = 1, lease: str = "1") -> tuple[int, list[str]]:
    env = dict(os.environ, R2F2_DB=db_name, DATAFLOW_DATA_DIR=str(data_dir), DATAFLOW_CONTROL_STORE_IMPORT_LEASE_SECONDS=lease,
               PYTHONPATH=os.pathsep.join([str(API_ROOT), os.environ.get("PYTHONPATH", "")]))
    data_dir.mkdir(parents=True, exist_ok=True)
    run = subprocess.run([sys.executable, "-c", _CHILD, role, str(reads)], env=env, cwd=str(API_ROOT),
                         capture_output=True, text=True, timeout=120)
    return run.returncode, [line for line in run.stdout.splitlines() if line.startswith(("tenants", "503"))]


@pytest.fixture
def live_db():
    svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    names: list[str] = []

    def new() -> str:
        names.append(f"opsfix_r2f2_{uuid.uuid4().hex[:10]}")
        return names[-1]

    try:
        yield svc, new
    finally:
        for name in names:
            svc.client.drop_database(name)
        svc.disconnect()


def test_killed_importer_is_taken_over_by_other_processes_on_live_mongodb(live_db, tmp_path):
    svc, new_db = live_db
    # 1. Pod A dies mid-import; pod A2 (restarted with the same file) completes it.
    db = new_db()
    (tmp_path / "a").mkdir()
    _tenants_file(tmp_path / "a" / "tenants.json", ["t1", "t2", "t3", "t4", "t5"])
    code, _out = _pod("crash", tmp_path / "a", db)
    assert code == 9
    assert svc.client[db][DOCS].find_one({"_id": "tenant_settings"})["state"] == "importing"
    code, out = _pod("serve", tmp_path / "a", db)
    assert out == ["tenants ['t1', 't2', 't3', 't4', 't5']"]
    assert svc.client[db][DOCS].find_one({"_id": "tenant_settings"})["state"] == "done"

    # 2. Pod A dies mid-import; pod B (no file) fails closed within the lease, then serves.
    db = new_db()
    code, _out = _pod("crash", tmp_path / "a", db, lease="4")  # outlasts pod B's start-up
    assert code == 9
    code, out = _pod("serve", tmp_path / "b", db, reads=16)
    assert out[0].startswith("503"), out
    assert out[-1] == "tenants []", out
    assert svc.client[db][ITEMS].count_documents({"store": "tenant_settings"}) == 0
