"""Ops review round 3, Medium A: an import must never leave a partial store that reads as complete.

The importer renewed its lease and then wrote a record, two separate steps.
A holder that stalled between them for longer than the lease (a VM or cgroup
pause), or a reader whose clock ran more than the lease ahead, let another
process abandon the claim and delete the imported records; the holder's
in-flight record then landed after that delete. The store was non-empty with
no claim, so every replica treated it as settled: 1 of 5 tenants (stall) and
1 of 300 (reader clock +40 s, 30 s lease) were all that was left, and the
holder's own file was then recorded as a conflict and never imported.

Imported records are now staged under the claim's epoch, invisible to reads
and to the "store has data" check, and become visible in one write when the
import completes; an abandoned epoch is never served, its late records are
removed, and leases are compared on the database server's clock.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from services import control_plane_import, mongodb_service, shared_doc_store, tenant_store
from services.control_plane_store import ControlPlaneUnavailable
from tests._control_plane_fakes import install_shared_store

ITEMS = "control_plane_items"
DOCS = shared_doc_store.DOCS_COLLECTION
STORE = "tenant_settings"
API_ROOT = Path(__file__).resolve().parents[1]


def _tenants_file(path: Path, count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"id": f"t{i}", "workspace_id": f"ws-{i}", "name": f"T{i}"} for i in range(count)]
    path.write_text(json.dumps({"tenants": rows}), encoding="utf-8")


def _fresh_process(monkeypatch, path: Path) -> None:
    monkeypatch.setattr(tenant_store, "STORE_PATH", path)
    for flag in ("_settled", "_split_checked"):
        monkeypatch.setattr(tenant_store._DOC, flag, False)
    monkeypatch.setattr(tenant_store._DOC, "_imported", set())
    monkeypatch.setattr(tenant_store._DOC, "_seen_stamps", set())
    monkeypatch.setattr(tenant_store._DOC, "_cache", {})


# -- one process, the in-memory store ---------------------------------------------------------------
def test_a_reader_whose_clock_runs_ahead_does_not_abandon_a_live_import(tmp_path, monkeypatch):
    """The lease is compared on the database server's clock, not the reader's."""
    db = install_shared_store(monkeypatch)
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_IMPORT_LEASE_SECONDS", "30")
    _fresh_process(monkeypatch, tmp_path / "none.json")
    db[DOCS].insert_one({"_id": STORE, "kind": "store", "import_sources": ["a" * 64], "state": "importing",
                         "epoch": 1, "owner": "pod-a:1:x", "node": "pod-a", "lease_until": time.time() + 20,
                         "at": time.time()})
    real_now = shared_doc_store._now
    monkeypatch.setattr(shared_doc_store, "_now", lambda: real_now() + 40)
    with pytest.raises(ControlPlaneUnavailable, match="being imported"):
        tenant_store.list_tenants()
    assert db[DOCS].find_one({"_id": STORE})["state"] == "importing", "the live claim was not taken over"


def test_records_of_an_unfinished_import_are_never_served_and_appear_together(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    _tenants_file(tmp_path / "a" / "tenants.json", 4)
    _fresh_process(monkeypatch, tmp_path / "a" / "tenants.json")
    seen: list[int] = []
    real_renew = control_plane_import.LeasedImport._renew_lease

    def renew_and_peek(self, docs, ticket):
        # What any reader would get from the database mid-import: no visible record.
        seen.append(db[ITEMS].count_documents({"store": STORE, "stg": None}))
        return real_renew(self, docs, ticket)

    monkeypatch.setattr(control_plane_import.LeasedImport, "_renew_lease", renew_and_peek)
    assert sorted(t.name for t in tenant_store.list_tenants()) == ["T0", "T1", "T2", "T3"]
    assert seen == [0, 0, 0, 0]
    meta = db[DOCS].find_one({"_id": STORE})
    assert meta["state"] == "done" and meta["unstaged"] is True
    assert db[ITEMS].count_documents({"store": STORE, "stg": {"$exists": True}}) == 0


def test_an_import_completed_but_not_unstaged_is_served_whole_and_unstaged_by_the_next_reader(tmp_path, monkeypatch):
    """The importer stopped between the one completing write and the clean-up."""
    db = install_shared_store(monkeypatch)
    _fresh_process(monkeypatch, tmp_path / "none.json")
    for i in range(3):
        body = json.dumps({"id": f"t{i}", "workspace_id": f"ws-{i}", "name": f"T{i}"})
        db[ITEMS].insert_one({"_id": f"{STORE}:t{i}", "store": STORE, "key": f"t{i}", "part": f"ws-{i}", "rev": 1,
                              "seq": i, "body": body, "tags": [], "stg": 7})
    left = json.dumps({"id": "t-old", "workspace_id": "ws-old", "name": "abandoned"})
    db[ITEMS].insert_one({"_id": f"{STORE}:t-old", "store": STORE, "key": "t-old", "part": "ws-old", "rev": 1,
                          "seq": 9, "body": left, "tags": [], "stg": 6})
    db[DOCS].insert_one({"_id": STORE, "kind": "store", "import_sources": ["b" * 64], "state": "done", "epoch": 7})
    assert sorted(t.name for t in tenant_store.list_tenants()) == ["T0", "T1", "T2"]
    assert db[ITEMS].count_documents({"store": STORE, "stg": {"$exists": True}}) == 0
    assert db[ITEMS].find_one({"_id": f"{STORE}:t-old"}) is None, "an abandoned epoch's left-over is deleted"


def test_a_late_record_of_an_abandoned_epoch_never_replaces_or_joins_the_current_import(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    _tenants_file(tmp_path / "a" / "tenants.json", 2)
    _fresh_process(monkeypatch, tmp_path / "a" / "tenants.json")
    db[DOCS].insert_one({"_id": f"import_epoch:{STORE}", "kind": "import_epoch", "n": 4})
    assert sorted(t.name for t in tenant_store.list_tenants()) == ["T0", "T1"]
    assert db[DOCS].find_one({"_id": STORE})["epoch"] == 5
    # The stalled holder of epoch 4 resumes and writes two records late.
    coll = shared_doc_store._collection(ITEMS)
    rows = tenant_store._DOC._records({"tenants": [{"id": "t0", "workspace_id": "ws-0", "name": "STALE"},
                                                   {"id": "t9", "workspace_id": "ws-9", "name": "STALE"}]})
    for seq, (key, row) in enumerate(rows.items()):
        tenant_store._DOC._write_imported(coll, db[DOCS], key, row, seq, stamp="c" * 64, epoch=4)
    _fresh_process(monkeypatch, tmp_path / "a" / "tenants.json")
    assert sorted(t.name for t in tenant_store.list_tenants()) == ["T0", "T1"]
    assert json.loads(db[ITEMS].find_one({"_id": f"{STORE}:t0"})["body"])["name"] == "T0"


# -- several processes, live MongoDB (the reviewer's scenarios) -----------------------------------------
_CHILD = r"""
import json, os, sys, time
os.environ["DATAFLOW_CONTROL_STORE_BACKEND"] = "mongo"
os.environ["DATAFLOW_CONTROL_STORE_CACHE_SECONDS"] = "0"
from services import mongodb_service, control_plane_store, shared_doc_store, control_plane_import
svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
assert svc.connect()
svc.db_name = os.environ["R3A_DB"]
mongodb_service._mongodb_service = svc
control_plane_store._remember_seen = lambda: None
role, seconds = sys.argv[1], float(sys.argv[2])
if role in ("stall", "slow"):
    real = control_plane_import.LeasedImport._renew_lease
    calls = [0]
    def renew(self, docs, ticket):
        ok = real(self, docs, ticket)
        calls[0] += 1
        if role == "stall" and calls[0] == 3:
            time.sleep(control_plane_import.import_lease_seconds() + 1.5)  # paused after renewing, before writing
        if role == "slow":
            time.sleep(0.05)
        return ok
    control_plane_import.LeasedImport._renew_lease = renew
if os.environ.get("R3A_SKEW"):
    real_now = shared_doc_store._now
    shared_doc_store._now = lambda: real_now() + float(os.environ["R3A_SKEW"])
from services import tenant_store
from services.control_plane_store import ControlPlaneUnavailable
counts, end = set(), time.time() + seconds
while True:
    try:
        counts.add(len(tenant_store.list_tenants()))
    except ControlPlaneUnavailable:
        counts.add("503")
    if time.time() >= end:
        break
    time.sleep(0.1)
print("COUNTS", json.dumps(sorted(counts, key=str)), flush=True)
"""


@pytest.fixture
def live_db():
    svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    name = f"opsfix_r3a_{uuid.uuid4().hex[:10]}"
    try:
        yield svc.client[name]
    finally:
        svc.client.drop_database(name)
        svc.disconnect()


def _pod(db, role: str, data_dir: Path, seconds: float, *, lease: str, skew: str = "") -> subprocess.Popen:
    data_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, R3A_DB=db.name, R3A_SKEW=skew, DATAFLOW_DATA_DIR=str(data_dir),
               DATAFLOW_CONTROL_STORE_IMPORT_LEASE_SECONDS=lease,
               PYTHONPATH=os.pathsep.join([str(API_ROOT), os.environ.get("PYTHONPATH", "")]))
    return subprocess.Popen([sys.executable, "-c", _CHILD, role, str(seconds)], env=env, cwd=str(API_ROOT),
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)


def _counts(pod: subprocess.Popen) -> list:
    out = pod.communicate(timeout=180)[0]
    lines = [line for line in out.splitlines() if line.startswith("COUNTS ")]
    assert lines, out[-2000:]
    return json.loads(lines[-1][len("COUNTS "):])


def _visible(db) -> int:
    return db[ITEMS].count_documents({"store": STORE, "stg": {"$exists": False}})


def _all_or_nothing(db, readers: list[list], total: int) -> None:
    for counts in readers:
        partial = [c for c in counts if c not in ("503", 0, total)]
        assert partial == [], f"a reader was served a partial store: {counts} of {total}"
    assert _visible(db) in (0, total), f"{_visible(db)} of {total} records visible"
    assert db[ITEMS].count_documents({"store": STORE, "stg": {"$exists": True}}) == 0, "staged left-overs"


def test_an_importer_stalled_past_its_lease_leaves_all_or_nothing(live_db, tmp_path):
    _tenants_file(tmp_path / "a" / "tenants.json", 5)
    holder = _pod(live_db, "stall", tmp_path / "a", 6.0, lease="2")
    time.sleep(1.0)
    reader = _pod(live_db, "read", tmp_path / "b", 6.0, lease="2")
    holder_counts, reader_counts = _counts(holder), _counts(reader)
    _all_or_nothing(live_db, [holder_counts, reader_counts], 5)
    assert _visible(live_db) == 5, "the holder imports its file again once its stalled attempt is gone"
    assert live_db[DOCS].count_documents({"kind": "local_file_conflict"}) == 0
    assert live_db[DOCS].count_documents({"kind": control_plane_import.TOMBSTONE_KIND}) == 1


def test_a_reader_with_its_clock_40s_ahead_of_a_30s_lease_leaves_all_or_nothing(live_db, tmp_path):
    _tenants_file(tmp_path / "a" / "tenants.json", 60)
    holder = _pod(live_db, "slow", tmp_path / "a", 0.5, lease="30")
    time.sleep(1.5)
    reader = _pod(live_db, "read", tmp_path / "b", 5.0, lease="30", skew="40")
    holder_counts, reader_counts = _counts(holder), _counts(reader)
    _all_or_nothing(live_db, [holder_counts, reader_counts], 60)
    assert _visible(live_db) == 60
    assert live_db[DOCS].count_documents({"kind": control_plane_import.TOMBSTONE_KIND}) == 0, "nothing abandoned"


def test_concurrent_first_loads_of_one_shared_file_import_every_record(live_db, tmp_path):
    _tenants_file(tmp_path / "shared" / "tenants.json", 400)
    pods = [_pod(live_db, "read", tmp_path / "shared", 3.0, lease="30") for _ in range(4)]
    results = [_counts(pod) for pod in pods]
    _all_or_nothing(live_db, results, 400)
    assert _visible(live_db) == 400
    assert live_db[DOCS].find_one({"_id": STORE})["records"] == 400
