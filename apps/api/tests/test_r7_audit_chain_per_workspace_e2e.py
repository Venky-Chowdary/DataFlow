"""Settings → Audit Logs → Verify chain, end to end on a real MongoDB store.

QA (production 557f556c): "Chain verification failed — 1371 finding(s) in
other workspaces (not shown)" for a workspace admin whose own records were fine.

Why the chain had findings at all, each reproduced here through the real
append path and the real ``/api/v1/audit/verify`` route:

* **Hash input changed on the way through Mongo.** The HMAC was computed over
  the Python objects handed to ``append_audit_event``; BSON then stored any
  ``datetime`` in ``details`` truncated to milliseconds and without its zone.
  Re-walking the record hashed different bytes, so an untouched record read
  as "altered" (``event_hash_mismatch``).
* **Three writer processes, one chain, a process-local lock.** API, worker and
  scheduler each read the tip and inserted on their own; two appends racing
  across processes claimed the same predecessor (``fork``/``broken_link``).
* **Every tenant interleaved in one global chain**, so any finding anywhere
  failed every workspace's verdict.

The contract after the fix: a workspace admin's verdict covers their own
workspace's chain only; platform-wide findings belong to the platform admin;
records broken by the old hash input are migrated by an audited re-anchor,
never hidden.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from services import audit_log, team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits

API_ROOT = Path(__file__).resolve().parents[1]
ADMIN_EMAIL = "root@example.com"
ADMIN_PASSWORD = "Bootstrap-Admin-2026"
MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI") or "mongodb://localhost:27017"


def _client_for(db_name: str):
    pymongo = pytest.importorskip("pymongo")
    from services.value_serializer import control_plane_codec_options

    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
    try:
        client.admin.command("ping")
    except Exception:  # noqa: BLE001
        pytest.skip("MongoDB not reachable")
    db = client.get_database(
        db_name, codec_options=control_plane_codec_options(client[db_name].codec_options)
    )
    return client, db


@pytest.fixture
def chain(monkeypatch, tmp_path):
    """The production carrier: an ``audit_events`` collection on a live Mongo."""
    name = f"r7_chain_{uuid.uuid4().hex[:10]}"
    client, db = _client_for(name)
    coll = db["audit_events"]
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: coll)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    try:
        yield {"coll": coll, "db_name": name}
    finally:
        client.drop_database(name)


@pytest.fixture
def admin(chain, tmp_path, monkeypatch):
    from src.main import app

    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", ADMIN_EMAIL)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", ADMIN_PASSWORD)
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    import services.tenant_store as tenant_store

    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    reset_auth_rate_limits()
    client = TestClient(app)
    login = client.post(
        "/api/v1/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    )
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return client


def _workspace(client: TestClient, name: str) -> str:
    res = client.post("/api/v1/team/workspaces", json={"name": name})
    assert res.status_code == 200, res.text
    return res.json()["workspace"]["id"]


def _member(admin: TestClient, *, email: str, workspace_id: str, role: str = "admin") -> TestClient:
    from src.main import app

    created = admin.post(
        "/api/v1/team/users",
        json={
            "email": email,
            "platform_role": "member",
            "workspace_id": workspace_id,
            "workspace_role": role,
        },
    )
    assert created.status_code == 200, created.text
    peer = TestClient(app)
    login = peer.post(
        "/api/v1/auth/login",
        json={"email": email, "password": created.json()["temporary_password"]},
    )
    assert login.status_code == 200, login.text
    peer.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return peer


def _verify(client: TestClient, workspace_id: str | None = None) -> dict:
    headers = {"X-Workspace-Id": workspace_id} if workspace_id else {}
    res = client.get("/api/v1/audit/verify?limit=20000", headers=headers)
    assert res.status_code == 200, res.text
    return res.json()


def _legacy_append(coll, *, workspace_id: str, details: dict, action: str = "legacy.event") -> dict:
    """Write a record exactly the way production 557f556c did.

    HMAC over the in-memory Python objects, then ``insert_one`` — so a
    ``datetime`` in ``details`` is hashed as microseconds-with-zone and stored
    as BSON milliseconds without one.
    """
    tip = next(iter(coll.find({}).sort([("chain_seq", -1)]).limit(1)), None)
    event = {
        "_id": str(uuid.uuid4()),
        "id": str(uuid.uuid4()),
        "time": datetime.now(timezone.utc).isoformat(),
        "actor": "system",
        "action": action,
        "resource": "legacy",
        "level": "info",
        "correlation_id": None,
        "workspace_id": workspace_id,
        "tenant_id": "",
        "details": details,
        "prev_hash": (tip or {}).get("event_hash"),
        "chain_seq": int((tip or {}).get("chain_seq") or 0) + 1,
        "hash_alg": "HMAC-SHA256",
    }
    event["event_hash"] = audit_log._hmac_event_hash(event, audit_log._platform_hmac_secret())
    coll.insert_one(event)
    return event


# ---------------------------------------------------------------------------
# 1. Hash input: a datetime in details must not break an untouched record
# ---------------------------------------------------------------------------


def test_datetime_detail_does_not_read_as_tampering(admin):
    aware = datetime(2026, 10, 30, 9, 15, 42, 123456, tzinfo=timezone.utc)
    naive = datetime(2026, 10, 30, 9, 15, 42, 654321)
    for details in (
        {"plain": "row"},
        {"started_at": aware},
        {"nested": {"at": naive, "items": [aware, None, "", " ", "Zoë"]}},
        {"plain": "after"},
    ):
        audit_log.append_audit_event(action="job.sample", resource="job:1", details=details)

    report = _verify(admin)
    assert report["findings"] == [], report["findings"]
    assert report["verified"] is True
    assert report["checked"] >= 4


# ---------------------------------------------------------------------------
# 2. Per-workspace verdict
# ---------------------------------------------------------------------------


def test_workspace_admin_gets_a_verdict_for_their_own_chain_only(admin, chain):
    acme = _workspace(admin, "Acme")
    other = _workspace(admin, "Other Co")
    acme_admin = _member(admin, email="ops@acme.example", workspace_id=acme)
    for i in range(3):
        audit_log.append_audit_event(
            action="connector.create", resource="/connectors", workspace_id=acme,
            details={"name": f"acme-{i}"},
        )
        audit_log.append_audit_event(
            action="connector.create", resource="/connectors", workspace_id=other,
            details={"name": f"other-{i}"},
        )
    # A real tamper in the OTHER workspace.
    chain["coll"].update_one(
        {"workspace_id": other, "details.name": "other-1"},
        {"$set": {"details.name": "rewritten"}},
    )

    mine = _verify(acme_admin, acme)
    assert mine["scope"] == "workspace"
    assert mine["verified"] is True, mine
    assert mine["findings"] == []
    # Nothing about another tenant's chain reaches a workspace admin.
    assert not mine.get("withheld_findings")
    assert "other workspace" not in str(mine).lower()
    assert mine["checked"] >= 3

    platform = _verify(admin)
    assert platform["scope"] == "platform"
    assert platform["verified"] is False
    kinds = {(f["kind"], f.get("workspace_id")) for f in platform["findings"]}
    assert ("event_hash_mismatch", other) in kinds


def test_workspace_admin_sees_their_own_tamper_and_deletion(admin, chain, monkeypatch):
    # Isolation on (self-serve deployments): unscoped records are platform-only,
    # so the deletion must show on the workspace's OWN line, not via a global
    # link attributed to someone else's record.
    monkeypatch.setenv("DATAWRAP_REQUIRE_WORKSPACE", "1")
    acme = _workspace(admin, "Acme")
    acme_admin = _member(admin, email="ops@acme.example", workspace_id=acme)
    for i in range(4):
        audit_log.append_audit_event(
            action="connector.create", resource="/connectors", workspace_id=acme,
            details={"name": f"acme-{i}"},
        )
        audit_log.append_audit_event(action="http.request", resource="/health", details={})
    chain["coll"].update_one({"details.name": "acme-1"}, {"$set": {"details.name": "x"}})
    chain["coll"].delete_one({"details.name": "acme-2"})

    mine = _verify(acme_admin, acme)
    assert mine["verified"] is False
    kinds = sorted(f["kind"] for f in mine["findings"])
    assert "event_hash_mismatch" in kinds
    # The deletion is visible on the workspace's own chain, not only globally.
    assert "broken_link" in kinds, mine["findings"]


# ---------------------------------------------------------------------------
# 3. Cross-process writers
# ---------------------------------------------------------------------------

_WRITER = textwrap.dedent(
    """
    import os, sys, time
    sys.path[:0] = [p for p in os.environ["R7_PATH"].split(os.pathsep) if p]
    os.environ["DATAFLOW_AUTH_SECRET"] = "test-secret-not-production"
    import pymongo
    from services.value_serializer import control_plane_codec_options
    from services import audit_log
    client = pymongo.MongoClient(os.environ["R7_MONGO"])
    name = os.environ["R7_DB"]
    db = client.get_database(name, codec_options=control_plane_codec_options(client[name].codec_options))
    coll = db["audit_events"]
    audit_log._mongo_collection = lambda: coll
    go = float(os.environ["R7_GO"])
    while time.time() < go:
        time.sleep(0.001)
    for i in range(int(os.environ["R7_N"])):
        audit_log.append_audit_event(action="proc.write", resource=os.environ["R7_TAG"],
                                     workspace_id=os.environ["R7_WS"], details={"i": i})
    """
)


def test_writers_in_separate_processes_keep_one_line_of_history(chain, tmp_path):
    import time

    script = tmp_path / "writer.py"
    script.write_text(_WRITER, encoding="utf-8")
    env_path = os.pathsep.join(
        [str(API_ROOT.parents[1] / "packages" / "preflight" / "src"), str(API_ROOT)]
    )
    go = time.time() + 4.0
    procs = []
    for tag, ws in (("api", "ws-a"), ("worker", "ws-b"), ("scheduler", "ws-a")):
        env = {
            **os.environ,
            "R7_PATH": env_path,
            "R7_MONGO": MONGO_URI,
            "R7_DB": chain["db_name"],
            "R7_GO": str(go),
            "R7_N": "40",
            "R7_TAG": tag,
            "R7_WS": ws,
            "DATAFLOW_DATA_DIR": str(tmp_path / tag),
        }
        procs.append(
            subprocess.Popen([sys.executable, str(script)], cwd=str(API_ROOT), env=env)
        )
    for proc in procs:
        assert proc.wait(timeout=180) == 0

    from services.evidence_chain import verify_chain

    report = verify_chain()
    assert report["checked"] == 120
    bad = [f for f in report["findings"] if f["kind"] in {"fork", "broken_link"}]
    assert not bad, bad[:5]
    assert report["verified"] is True
    # Each workspace's own chain holds too.
    for ws in ("ws-a", "ws-b"):
        scoped = verify_chain(workspace_id=ws)
        assert scoped["verified"] is True, scoped["findings"][:5]


# ---------------------------------------------------------------------------
# 4. Audited re-anchor for records broken by the old hash input
# ---------------------------------------------------------------------------


def test_reanchor_migrates_hash_input_findings_and_is_audited(admin, chain):
    acme = _workspace(admin, "Acme")
    acme_admin = _member(admin, email="ops@acme.example", workspace_id=acme)
    coll = chain["coll"]
    audit_log.append_audit_event(action="before", resource="r", workspace_id=acme, details={})
    legacy = _legacy_append(
        coll, workspace_id=acme,
        details={"at": datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)},
    )
    tampered = _legacy_append(coll, workspace_id=acme, details={"name": "orig"})
    coll.update_one({"id": tampered["id"]}, {"$set": {"details.name": "forged"}})
    audit_log.append_audit_event(action="after", resource="r", workspace_id=acme, details={})

    before = _verify(admin)
    by_id = {f["event_id"]: f for f in before["findings"]}
    assert by_id[legacy["id"]]["kind"] == "event_hash_mismatch"
    assert by_id[legacy["id"]]["cause"] == "legacy_datetime_roundtrip"
    assert by_id[tampered["id"]]["cause"] == "unexplained"

    # Workspace admins cannot re-anchor; it is a platform decision.
    refused = acme_admin.post(
        "/api/v1/audit/chain/reanchor",
        json={"reason": "migrate hash input change", "dry_run": False},
        headers={"X-Workspace-Id": acme},
    )
    assert refused.status_code == 403, refused.text

    plan = admin.post(
        "/api/v1/audit/chain/reanchor",
        json={"reason": "migrate records hashed before canonical input", "dry_run": True},
    )
    assert plan.status_code == 200, plan.text
    assert plan.json()["dry_run"] is True
    assert [e["event_id"] for e in plan.json()["accepted"]] == [legacy["id"]]
    assert _verify(admin)["verified"] is False  # a dry run changes nothing

    applied = admin.post(
        "/api/v1/audit/chain/reanchor",
        json={"reason": "migrate records hashed before canonical input", "dry_run": False},
    )
    assert applied.status_code == 200, applied.text
    body = applied.json()
    assert body["dry_run"] is False and len(body["accepted"]) == 1
    assert body["skipped_unexplained"] == 1
    receipt = coll.find_one({"action": "audit.chain.reanchored"})
    assert receipt is not None
    assert receipt["details"]["accepted_count"] == 1
    assert receipt["details"]["reason"].startswith("migrate records")
    assert receipt["actor"] == ADMIN_EMAIL

    after = _verify(admin)
    ids = {f["event_id"] for f in after["findings"]}
    assert legacy["id"] not in ids
    assert tampered["id"] in ids  # a real tamper is not migrated away
    assert [r["event_id"] for r in after["reanchored"]] == [legacy["id"]]

    # The workspace admin sees the migrated record as re-anchored, not as broken.
    mine = _verify(acme_admin, acme)
    assert legacy["id"] not in {f["event_id"] for f in mine["findings"]}
    assert legacy["id"] in {r["event_id"] for r in mine["reanchored"]}

    # Re-anchoring seals the record as it stood: a later edit is a finding again.
    coll.update_one({"id": legacy["id"]}, {"$set": {"details.extra": "late edit"}})
    again = _verify(admin)
    assert legacy["id"] in {f["event_id"] for f in again["findings"]}
