"""The per-workspace template cap holds across API replicas (live MongoDB).

Two processes create the last free template slot at the same moment. An
in-process lock cannot order them and a new uuid-keyed record never conflicts
on its own revision, so the cap is enforced by the store's unique claims: each
template holds ``slot:<workspace>:<n>`` for one n below the cap.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from services import mongodb_service

pytestmark = pytest.mark.timeout(180)

API_ROOT = Path(__file__).resolve().parents[1]

CHILD = r'''
import os, sys, time
os.environ["DATAFLOW_CONTROL_STORE_BACKEND"] = "mongo"
os.environ["DATAFLOW_CONTROL_STORE_CACHE_SECONDS"] = "0"
from services import mongodb_service, control_plane_store
svc = mongodb_service.MongoDBService(os.environ["PROBE_MONGO"])
assert svc.connect()
svc.db_name = os.environ["PROBE_DB"]
mongodb_service._mongodb_service = svc
control_plane_store._remember_seen = lambda: None
from services import mapping_templates as mt
mt.MAX_TEMPLATES_PER_WORKSPACE = 2
doc = {"spec": {"mappings": [], "computed": []}, "checksum": "x"}
role, sync = sys.argv[1], os.environ["PROBE_SYNC"]
if role == "seed":
    mt.create_template(name="t0", description="", document=doc, workspace_id="wsR")
    print("seeded"); sys.exit()
if role == "count":
    print("templates", len(mt.list_templates(workspace_id="wsR"))); sys.exit()
if role == "delete_one":
    first = mt.list_templates(workspace_id="wsR")[0]["id"]
    mt.delete_template(first); print("deleted"); sys.exit()
real = mt._DOC.load
def load(*a, **k):
    # Both replicas read the partition, then wait for each other: both see room.
    out = real(*a, **k)
    if k.get("part") == "wsR" and not os.path.exists(sync + "." + role + ".done"):
        open(sync + "." + role, "w").close()
        open(sync + "." + role + ".done", "w").close()
        other = "b" if role == "a" else "a"
        t = time.time()
        while not os.path.exists(sync + "." + other) and time.time() - t < 10:
            time.sleep(0.02)
    return out
mt._DOC.load = load
try:
    mt.create_template(name="r" + role, description="", document=doc, workspace_id="wsR")
    print(role, "created")
except mt.TemplateLimitReached:
    print(role, "full")
'''


@pytest.fixture
def live_db():
    uri = os.environ.get("MONGO_URI") or "mongodb://localhost:27017/"
    svc = mongodb_service.MongoDBService(uri)
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    name = f"maptpl_cap_{uuid.uuid4().hex[:10]}"
    try:
        yield uri, name
    finally:
        svc.client.drop_database(name)
        svc.disconnect()


def _run(role: str, env: dict[str, str]) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", CHILD, role], cwd=API_ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


def _last(proc: subprocess.Popen) -> str:
    out = proc.communicate(timeout=120)[0].strip().splitlines()
    return out[-1] if out else ""


def test_two_replicas_racing_for_the_last_slot_leave_the_cap_intact(live_db, tmp_path):
    uri, db = live_db
    env = dict(os.environ, PROBE_MONGO=uri, PROBE_DB=db, PROBE_SYNC=str(tmp_path / "sync"))
    assert _last(_run("seed", env)) == "seeded"
    a, b = _run("a", env), _run("b", env)
    outcomes = sorted([_last(a), _last(b)])
    assert outcomes in (["a created", "b full"], ["a full", "b created"]), outcomes
    assert _last(_run("count", env)) == "templates 2"
    # Deleting a template frees its slot for the next create.
    assert _last(_run("delete_one", env)) == "deleted"
    env2 = dict(env, PROBE_SYNC=str(tmp_path / "sync2"))
    (tmp_path / "sync2.a").touch()  # a lone create: nobody to wait for
    c = _run("c", env2)
    assert _last(c) == "c created"
    assert _last(_run("count", env)) == "templates 2"
