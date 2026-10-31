"""D221: the audit hash chain forks across processes and is signed with the login secret.

QA (production 557f556c): 1,371 audit-chain verification failures. Two write
defects, not tampering, produce them:

* **Forks.** API, worker and scheduler append to one hash-chained log. The
  only serialization was a process-local lock (557f556c), so two processes
  read the same tip and both linked to it. Round 7 added a unique index on
  the chain position, but the index is optional: when the server cannot build
  it (here: one historic duplicate position already in the store, which is
  exactly what the old race leaves behind) the append logs a warning and
  carries on unserialized. The file store (no Mongo, processes sharing a data
  volume) never had a cross-process guard at all.
* **The signing key.** Every record was HMAC-ed with the login/JWT secret, so
  rotating the login secret turned every older record into a "tampered" one.

Contract after the fix, checked here with real processes on a real MongoDB and
through the real ``/api/v1/audit/verify`` route:

* concurrent appends from separate processes never share a predecessor, with
  or without the optional index, and on the shared file store too;
* the chain has its own key (``DATAFLOW_AUDIT_HMAC_KEY``), each record names
  the key it was signed with (``key_id``), the login secret is no longer used,
  old records keep verifying under the legacy key, and retired keys keep their
  records verifiable;
* verification reports forks and key mismatches as their own categories,
  separate from tampering;
* the owner's production check (``scripts/audit_chain_check.py``) counts forks
  and records signed under another key, and never writes.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from services import audit_log, team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits

API_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = API_ROOT.parents[1]
ADMIN_EMAIL = "root@example.com"
ADMIN_PASSWORD = "Bootstrap-Admin-2026"
MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI") or "mongodb://localhost:27017"
LOGIN_SECRET = "login-secret-before-rotation"
AUDIT_KEY = "audit-chain-key-for-tests-0001"


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
    name = f"r9_d221_{uuid.uuid4().hex[:10]}"
    client, db = _client_for(name)
    coll = db["audit_events"]
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: coll)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", LOGIN_SECRET)
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", AUDIT_KEY)
    monkeypatch.delenv("DATAWRAP_AUDIT_HMAC_KEY", raising=False)
    monkeypatch.delenv("DATAWRAP_AUTH_SECRET", raising=False)
    monkeypatch.delenv("DATAFLOW_AUDIT_HMAC_RETIRED_KEYS", raising=False)
    monkeypatch.delenv("DATAWRAP_AUDIT_HMAC_RETIRED_KEYS", raising=False)
    try:
        yield {"client": client, "db": db, "coll": coll, "db_name": name, "dir": tmp_path}
    finally:
        client.drop_database(name)


@pytest.fixture
def admin(chain, tmp_path, monkeypatch):
    """A logged-in platform admin, created on first call.

    Lazy so a test can put history in the store before the login writes its
    own audit records (and before anything could build an index over it).
    """
    from src.main import app

    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", ADMIN_EMAIL)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", ADMIN_PASSWORD)
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    import services.tenant_store as tenant_store

    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    return lambda: _login(app)


def _login(app) -> TestClient:
    reset_auth_rate_limits()
    client = TestClient(app)
    login = client.post(
        "/api/v1/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    )
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return client


def _verify(client: TestClient) -> dict:
    res = client.get("/api/v1/audit/verify?limit=20000")
    assert res.status_code == 200, res.text
    return res.json()


def _legacy_record(coll, *, action: str, secret: str, seq: int | None = None,
                   prev: str | None = None, time_text: str | None = None,
                   chain_v: int | None = None) -> dict:
    """A record as written before D221: no key id, HMAC under the login secret."""
    from services.audit_chain_link import canonical_record

    if seq is None:
        tip = coll.find_one(sort=[("chain_seq", -1), ("time", -1)])
        seq = int((tip or {}).get("chain_seq") or 0) + 1
        prev = (tip or {}).get("event_hash")

    event = {
        "_id": str(uuid.uuid4()),
        "id": str(uuid.uuid4()),
        "time": time_text or datetime.now(timezone.utc).isoformat(),
        "actor": "system",
        "action": action,
        "resource": "legacy",
        "level": "info",
        "correlation_id": None,
        "workspace_id": "",
        "tenant_id": "",
        "details": {"seq": seq},
        "prev_hash": prev,
        "chain_seq": seq,
        "ws_prev_hash": None,
        "hash_alg": "HMAC-SHA256",
    }
    if chain_v is not None:
        event["chain_v"] = chain_v
    event = canonical_record(event)
    event["event_hash"] = audit_log._hmac_event_hash(event, secret.encode("utf-8"))
    coll.insert_one(dict(event))
    return event


def _seed_historic_duplicate_position(coll) -> list[dict]:
    """What the old cross-process race leaves behind: two records at one position.

    Both claim the same predecessor and the same ``chain_seq``. With this in the
    store MongoDB cannot build a unique index on the position.
    """
    root = _legacy_record(coll, seq=1, prev=None, action="seed.root", secret=LOGIN_SECRET,
                          chain_v=2, time_text="2026-10-01T00:00:00+00:00")
    a = _legacy_record(coll, seq=2, prev=root["event_hash"], action="seed.branch_a",
                       secret=LOGIN_SECRET, chain_v=2, time_text="2026-10-01T00:00:01+00:00")
    b = _legacy_record(coll, seq=2, prev=root["event_hash"], action="seed.branch_b",
                       secret=LOGIN_SECRET, chain_v=2, time_text="2026-10-01T00:00:02+00:00")
    return [root, a, b]


# ---------------------------------------------------------------------------
# 1. Concurrent appenders in separate processes
# ---------------------------------------------------------------------------

#: One writer process (API, worker, scheduler, CDC worker). Its first append is
#: held at a barrier right after it has read the chain tip, so every process
#: links its first record to the same tip and then inserts at the same instant —
#: the race the process-local lock cannot see, made deterministic. Nothing else
#: in the append path is replaced: real tip reads, real hashing, real inserts.
_WRITER = textwrap.dedent(
    """
    import os, sys, time
    sys.path[:0] = [p for p in os.environ["D221_PATH"].split(os.pathsep) if p]
    import pymongo
    from services.value_serializer import control_plane_codec_options
    from services import audit_log
    if os.environ.get("D221_MONGO"):
        client = pymongo.MongoClient(os.environ["D221_MONGO"])
        name = os.environ["D221_DB"]
        db = client.get_database(name, codec_options=control_plane_codec_options(client[name].codec_options))
        coll = db["audit_events"]
        audit_log._mongo_collection = lambda: coll
    else:
        audit_log._mongo_collection = lambda: None
    barrier = float(os.environ["D221_BARRIER"])
    real_tip = audit_log._chain_tip
    held = {"done": False}
    def tip_then_wait():
        tip = real_tip()
        if not held["done"]:
            held["done"] = True
            while time.time() < barrier:
                time.sleep(0.0005)
        return tip
    audit_log._chain_tip = tip_then_wait
    for i in range(int(os.environ["D221_N"])):
        audit_log.append_audit_event(action="proc.write", resource=os.environ["D221_TAG"],
                                     workspace_id=os.environ["D221_WS"], details={"i": i})
    """
)


def _run_writers(tmp_path: Path, *, db_name: str | None, data_dir: Path, n: int = 15) -> None:
    script = tmp_path / "d221_writer.py"
    script.write_text(_WRITER, encoding="utf-8")
    env_path = os.pathsep.join([str(REPO_ROOT / "packages" / "preflight" / "src"), str(API_ROOT)])
    barrier = time.time() + 6.0
    procs = []
    for tag, ws in (("api", "ws-a"), ("worker", "ws-b"), ("scheduler", "ws-a"), ("cdc", "")):
        env = {
            **os.environ,
            "D221_PATH": env_path,
            "D221_MONGO": MONGO_URI if db_name else "",
            "D221_DB": db_name or "",
            "D221_BARRIER": str(barrier),
            "D221_N": str(n),
            "D221_TAG": tag,
            "D221_WS": ws,
            "DATAFLOW_DATA_DIR": str(data_dir),
            "DATAFLOW_AUTH_SECRET": LOGIN_SECRET,
            "DATAFLOW_AUDIT_HMAC_KEY": AUDIT_KEY,
            "DATAFLOW_AUDIT_MAX_EVENTS": "100000",
        }
        procs.append(subprocess.Popen([sys.executable, str(script)], cwd=str(API_ROOT), env=env))
    for proc in procs:
        assert proc.wait(timeout=240) == 0


def _shared_predecessors(records: list[dict], field: str = "prev_hash") -> dict[str, int]:
    counts: dict[str, int] = {}
    for rec in records:
        prev = rec.get(field)
        if prev:
            counts[prev] = counts.get(prev, 0) + 1
    return {k: v for k, v in counts.items() if v > 1}


@pytest.mark.parametrize("store_state", ["fresh", "historic_duplicate_position"])
def test_writer_processes_never_fork_the_chain(admin, chain, tmp_path, store_state):
    coll = chain["coll"]
    seeded = _seed_historic_duplicate_position(coll) if store_state != "fresh" else []
    seed_ids = {s["id"] for s in seeded}
    platform_admin = admin()

    _run_writers(tmp_path, db_name=chain["db_name"], data_dir=tmp_path / "writers")

    written = [
        {k: v for k, v in doc.items() if k != "_id"}
        for doc in coll.find({"action": "proc.write"})
    ]
    assert len(written) == 60
    everything = [{k: v for k, v in d.items() if k != "_id"} for d in coll.find({})]
    # The seeds' own historic fork (branch_a / branch_b share root) is expected;
    # no record written by the processes may share a predecessor with another.
    forks = {
        prev: n for prev, n in _shared_predecessors(everything).items()
        if not (seeded and prev == seeded[0]["event_hash"] and n == 2)
    }
    assert forks == {}, f"{len(forks)} predecessor(s) claimed twice after a cross-process race"
    positions = [rec["chain_seq"] for rec in written]
    assert len(set(positions)) == len(positions), "two appends took one chain position"
    for ws in ("ws-a", "ws-b"):
        line = [r for r in written if r.get("workspace_id") == ws]
        assert _shared_predecessors(line, "ws_prev_hash") == {}

    report = _verify(platform_admin)
    assert report["checked"] >= 60 + len(seeded)
    new_bad = [
        f for f in report["findings"]
        if f["event_id"] not in seed_ids and f["kind"] in {"fork", "broken_link"}
    ]
    assert new_bad == [], new_bad[:5]


def test_writer_processes_sharing_the_file_store_never_fork(chain, tmp_path, monkeypatch):
    """No Mongo: API and worker share one data volume (one JSONL file)."""
    shared = tmp_path / "shared-volume"
    shared.mkdir()
    _run_writers(tmp_path, db_name=None, data_dir=shared)

    lines = (shared / "audit_events.jsonl").read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in lines if line.strip()]
    assert len(records) == 60
    assert _shared_predecessors(records) == {}, "file store forked across processes"
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", shared / "audit_events.jsonl")
    from services.evidence_chain import verify_chain

    report = verify_chain()
    assert [f for f in report["findings"] if f["kind"] in {"fork", "broken_link"}] == []
    assert report["verified"] is True, report["findings"][:5]


# ---------------------------------------------------------------------------
# 2. The chain's own key, a key id per record, and rotation
# ---------------------------------------------------------------------------


def test_chain_has_its_own_key_and_login_rotation_breaks_nothing(admin, chain, monkeypatch):
    coll = chain["coll"]
    platform_admin = admin()
    old = _legacy_record(coll, action="legacy.before_d221", secret=LOGIN_SECRET)
    audit_log.append_audit_event(action="after.d221", resource="r", details={"n": 1})
    newest = coll.find_one({"action": "after.d221"})
    assert newest["prev_hash"] == old["event_hash"]

    from services.audit_chain_keys import key_id_for

    # The record names its key, and that key is the chain key, not the login secret.
    assert newest["key_id"] == key_id_for(AUDIT_KEY.encode())
    assert newest["key_id"] != key_id_for(LOGIN_SECRET.encode())
    body = {k: v for k, v in newest.items() if k not in ("_id", "event_hash")}
    assert audit_log._hmac_event_hash(body, LOGIN_SECRET.encode()) != newest["event_hash"]
    assert audit_log._hmac_event_hash(body, AUDIT_KEY.encode()) == newest["event_hash"]

    report = _verify(platform_admin)
    assert report["verified"] is True, report["findings"]
    assert report["signing"]["current_key_id"] == newest["key_id"]
    assert AUDIT_KEY not in json.dumps(report) and LOGIN_SECRET not in json.dumps(report)

    # Rotate the login secret, retiring the old one for legacy records only.
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "login-secret-after-rotation")
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_RETIRED_KEYS", LOGIN_SECRET)
    from src.main import app

    rotated = _login(app)
    audit_log.append_audit_event(action="after.login.rotation", resource="r", details={})
    report = _verify(rotated)
    assert report["verified"] is True, report["findings"]
    assert report["categories"]["tampering"] == 0

    # Rotate the chain key itself: the old key retires, its records still verify.
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", "audit-chain-key-for-tests-0002")
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_RETIRED_KEYS", f"{LOGIN_SECRET},{AUDIT_KEY}")
    audit_log.append_audit_event(action="after.key.rotation", resource="r", details={})
    report = _verify(rotated)
    assert report["verified"] is True, report["findings"]
    kids = {d.get("key_id") for d in coll.find({})}
    assert kids == {None, key_id_for(AUDIT_KEY.encode()),
                    key_id_for(b"audit-chain-key-for-tests-0002")}

    # Forget the retired chain key: its records are a key mismatch, not tampering.
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_RETIRED_KEYS", LOGIN_SECRET)
    report = _verify(rotated)
    assert report["verified"] is False
    assert report["categories"]["tampering"] == 0
    under_retired = coll.count_documents({"key_id": key_id_for(AUDIT_KEY.encode())})
    assert under_retired >= 2
    assert report["categories"]["key_mismatch"] == under_retired
    assert {f["kind"] for f in report["findings"]} == {"key_mismatch"}


def test_unset_audit_key_never_signs_with_the_login_secret(chain, monkeypatch):
    monkeypatch.delenv("DATAFLOW_AUDIT_HMAC_KEY", raising=False)
    audit_log.append_audit_event(action="no.key.configured", resource="r", details={})
    audit_log.append_audit_event(action="no.key.configured", resource="r", details={})
    docs = list(chain["coll"].find({"action": "no.key.configured"}))
    from services.audit_chain_keys import key_id_for

    assert len({d["key_id"] for d in docs}) == 1
    assert docs[0]["key_id"] != key_id_for(LOGIN_SECRET.encode())
    body = {k: v for k, v in docs[0].items() if k not in ("_id", "event_hash")}
    assert audit_log._hmac_event_hash(body, LOGIN_SECRET.encode()) != docs[0]["event_hash"]
    key_file = chain["dir"] / "audit_chain.key"
    assert key_file.exists() and (key_file.stat().st_mode & 0o077) == 0
    from services.evidence_chain import verify_chain

    report = verify_chain()
    assert report["verified"] is True, report["findings"]
    assert report["signing"]["source"] == "generated_local"


# ---------------------------------------------------------------------------
# 3. Verification categories: fork and key mismatch are not tampering
# ---------------------------------------------------------------------------


def test_verify_reports_forks_and_key_mismatches_apart_from_tampering(admin, chain):
    coll = chain["coll"]
    seeded = _seed_historic_duplicate_position(coll)
    platform_admin = admin()
    audit_log.append_audit_event(action="clean.1", resource="r", details={"v": "a"})
    audit_log.append_audit_event(action="to.tamper", resource="r", details={"v": "b"})
    audit_log.append_audit_event(action="clean.2", resource="r", details={"v": ""})
    # A record signed under a key this deployment does not hold.
    from services.audit_chain_link import canonical_record
    from services.audit_chain_keys import key_id_for

    tip = coll.find_one(sort=[("chain_seq", -1)])
    foreign = canonical_record({
        "_id": str(uuid.uuid4()), "id": str(uuid.uuid4()),
        "time": datetime.now(timezone.utc).isoformat(), "actor": "system",
        "action": "foreign.key", "resource": "r", "level": "info",
        "correlation_id": None, "workspace_id": "", "tenant_id": "",
        "details": {"v": "Zoë"}, "prev_hash": tip["event_hash"],
        "chain_seq": tip["chain_seq"] + 1, "ws_prev_hash": tip["event_hash"],
        "chain_v": 3, "key_id": key_id_for(b"a-key-nobody-configured"),
        "hash_alg": "HMAC-SHA256",
    })
    foreign["event_hash"] = audit_log._hmac_event_hash(foreign, b"a-key-nobody-configured")
    coll.insert_one(dict(foreign))
    tampered = coll.find_one({"action": "to.tamper"})
    coll.update_one({"_id": tampered["_id"]}, {"$set": {"details.v": "rewritten"}})

    report = _verify(platform_admin)
    assert report["verified"] is False
    cats = report["categories"]
    assert cats["tampering"] == 1, report["findings"]
    assert cats["key_mismatch"] == 1
    assert cats["fork"] >= 1
    assert report["tampering_detected"] is True
    by_kind = {}
    for f in report["findings"]:
        by_kind.setdefault(f["category"], set()).add(f["event_id"])
    assert by_kind["tampering"] == {tampered["id"]}
    assert by_kind["key_mismatch"] == {foreign["id"]}
    assert seeded[2]["id"] in by_kind["fork"]
    # The historic fork must not be reported as a deletion (broken_link).
    assert not [f for f in report["findings"] if f["kind"] == "broken_link"]


# ---------------------------------------------------------------------------
# 4. The owner's read-only production check
# ---------------------------------------------------------------------------


def _db_hash(chain) -> dict:
    res = chain["db"].command("dbHash")
    return {"md5": res["md5"], "collections": res.get("collections")}


def test_production_check_counts_forks_and_foreign_keys_and_never_writes(chain):
    coll = chain["coll"]
    _seed_historic_duplicate_position(coll)
    audit_log.append_audit_event(action="keyed", resource="r", details={})
    audit_log.append_audit_event(action="keyed", resource="r", workspace_id="ws-a", details={})
    before = _db_hash(chain)
    count_before = coll.count_documents({})
    indexes_before = sorted(i["name"] for i in coll.list_indexes())

    env = {
        **os.environ,
        # --db names the database; the URI may carry options (?authSource=...).
        "MONGODB_URI": MONGO_URI,
        "DATAFLOW_AUDIT_HMAC_KEY": AUDIT_KEY,
        "DATAFLOW_AUTH_SECRET": LOGIN_SECRET,
        "PYTHONPATH": os.pathsep.join(
            [str(REPO_ROOT / "packages" / "preflight" / "src"), str(API_ROOT)]
        ),
    }
    out = subprocess.run(
        [sys.executable, str(API_ROOT / "scripts" / "audit_chain_check.py"),
         "--db", chain["db_name"], "--json", "--hmac"],
        cwd=str(API_ROOT), env=env, capture_output=True, text=True, timeout=120,
    )
    assert out.returncode == 0, out.stderr
    report = json.loads(out.stdout)
    assert report["read_only"] is True
    assert report["total_records"] == 5
    assert report["forks"]["predecessors_claimed_more_than_once"] == 1
    assert report["forks"]["records_beyond_first_claim"] == 1
    assert report["forks"]["duplicate_positions"] == 1
    assert report["keys"]["by_key_id"]["legacy (no key_id)"] == 3
    assert report["keys"]["records_not_under_current_key"] == 3
    assert report["hmac"]["verified_under_current_key"] == 2
    assert report["hmac"]["verified_under_legacy_login_secret"] == 3
    assert report["hmac"]["verified_under_no_configured_key"] == 0
    assert AUDIT_KEY not in out.stdout and LOGIN_SECRET not in out.stdout

    assert _db_hash(chain) == before
    assert coll.count_documents({}) == count_before
    assert sorted(i["name"] for i in coll.list_indexes()) == indexes_before
    names = chain["db"].list_collection_names()
    assert names == ["audit_events"], names
