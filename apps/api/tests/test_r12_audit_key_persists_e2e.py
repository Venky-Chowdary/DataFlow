"""QA r12 #3: audit verify says ``verified: false`` after every deploy.

QA (build 8f7d246d, workspace 44e009ee-…): ``GET /api/v1/audit/verify`` checked
1,983 records, tampering 0, ``verified: false`` — 326 records were signed with
key ``ak_ca4273e3…``, which the deployment no longer held.

Root cause (services/audit_chain_keys.py ``signing_key`` / ``_generated_key``
on 8f7d246d and 2eb5e3c0): with ``DATAFLOW_AUDIT_HMAC_KEY`` unset (production)
each process generated a key into ``<data dir>/audit_chain.key``. On Railway
that path is the container's own filesystem, so every deploy, and every
separate service, signed with a new key nobody else held.

Contract checked here on a live MongoDB through the real app
(``r8_ent_harness``: the control-plane singleton points at a throwaway
database, as production's ``MONGO_URL`` does) and with real separate
processes, each with its own empty data directory (a fresh container):

* processes and restarts with no env key sign with one key, kept encrypted in
  the control-plane database and created once (concurrent first writers agree);
* rotating starts a new key; records signed before it still verify, records
  after it name the new key;
* a key file left by the old scheme is imported, so its records keep verifying
  after the file is gone;
* records whose key is truly lost are ``unverifiable_key_lost`` — counted apart
  from tampering, never reported as verified — and verify again once the key
  is supplied as a retired key.
"""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
import textwrap
import time
import uuid
from pathlib import Path

import pytest

from r8_ent_harness import MONGO_URI, ent  # noqa: F401

API_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = API_ROOT.parents[1]
#: Values the chain must carry intact: NULL, '', whitespace, unicode.
DETAILS = {"none": None, "empty": "", "blank": "   ", "name": "Zoë 名前 ✓"}


def _keystore():
    """The persisted keyring module; its absence is the defect, not an error."""
    try:
        from services import audit_chain_keystore
    except ImportError:
        pytest.fail("no persisted audit keyring: the chain key lives only on the node's disk")
    return audit_chain_keystore


def _forget_process_keys() -> None:
    """What a process restart forgets: every in-memory key cache."""
    from services import audit_chain_keys

    audit_chain_keys._generated.clear()
    audit_chain_keys._warned.clear()
    try:
        from services import audit_chain_keystore

        audit_chain_keystore.reset_cache()
    except ImportError:
        pass


def _fresh_container(ent, monkeypatch, name: str) -> Path:
    """Restart this process into a new, empty data directory (a redeploy)."""
    data = ent.tmp / name
    data.mkdir()
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(data))
    _forget_process_keys()
    return data


@pytest.fixture(autouse=True)
def _no_configured_audit_key(monkeypatch):
    for brand in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{brand}_AUDIT_HMAC_KEY", raising=False)
        monkeypatch.delenv(f"{brand}_AUDIT_HMAC_RETIRED_KEYS", raising=False)
    _forget_process_keys()
    yield
    _forget_process_keys()


def _write(n: int, *, action: str, workspace_id: str = "") -> None:
    from services.audit_log import append_audit_event

    for i in range(n):
        append_audit_event(
            action=action, resource=f"/r12/{i}", actor="qa@example.com",
            workspace_id=workspace_id, details={**DETAILS, "i": i},
        )


def _records(ent, action: str) -> list[dict]:
    return list(ent.db["audit_events"].find({"action": action}).sort("chain_seq", 1))


def _start_from_an_empty_store(ent) -> None:
    """Drop what the harness's own login wrote, so the first writers really race."""
    ent.db["audit_events"].delete_many({})
    ent.db["audit_chain_keys"].delete_many({})
    _forget_process_keys()


def _verify(ent, workspace_id: str | None = None) -> dict:
    headers = {"X-Workspace-Id": workspace_id} if workspace_id else {}
    res = ent.admin.get("/api/v1/audit/verify", params={"limit": 20000}, headers=headers)
    assert res.status_code == 200, res.text
    return res.json()


# ---------------------------------------------------------------------------
# Separate processes, each a fresh container with no env key
# ---------------------------------------------------------------------------

_DEPLOY = textwrap.dedent(
    """
    import os, sys, time
    sys.path[:0] = [p for p in os.environ["R12_PATH"].split(os.pathsep) if p]
    from services import mongodb_service
    svc = mongodb_service.MongoDBService(os.environ["R12_MONGO"])
    svc.db_name = os.environ["R12_DB"]
    assert svc.connect()
    mongodb_service._mongodb_service = svc
    from services import audit_log
    barrier = float(os.environ["R12_BARRIER"])
    while time.time() < barrier:
        time.sleep(0.0005)
    for i in range(int(os.environ["R12_N"])):
        audit_log.append_audit_event(
            action="r12.deploy.write", resource=os.environ["R12_TAG"],
            workspace_id=os.environ["R12_WS"],
            details={"none": None, "empty": "", "blank": "   ", "name": "Zoë 名前 ✓", "i": i},
        )
    """
)


def _deploy(ent, tags: list[str], *, workspace_id: str, n: int = 4) -> None:
    """Start one process per service at the same instant, each on an empty disk."""
    path = os.pathsep.join([str(REPO_ROOT / "packages" / "preflight" / "src"), str(API_ROOT)])
    barrier = time.time() + 5.0
    procs = []
    for tag in tags:
        data = ent.tmp / f"container-{tag}-{uuid.uuid4().hex[:6]}"
        data.mkdir()
        env = {
            k: v for k, v in os.environ.items()
            if not k.endswith(("_AUDIT_HMAC_KEY", "_AUDIT_HMAC_RETIRED_KEYS"))
        }
        env.update(
            {
                "R12_PATH": path, "R12_MONGO": MONGO_URI, "R12_DB": ent.db.name,
                "R12_BARRIER": str(barrier), "R12_N": str(n), "R12_TAG": tag,
                "R12_WS": workspace_id, "DATAFLOW_DATA_DIR": str(data),
                "DATAFLOW_AUDIT_MAX_EVENTS": "100000",
            }
        )
        procs.append(subprocess.Popen([sys.executable, "-c", _DEPLOY], cwd=str(API_ROOT), env=env))
    for proc in procs:
        assert proc.wait(timeout=240) == 0


def test_every_process_and_redeploy_without_env_key_signs_with_one_persisted_key(ent, monkeypatch):
    ws = ent.workspace("R12 audit key")
    _start_from_an_empty_store(ent)
    # Deploy 1: API, worker and scheduler start together, each on its own disk,
    # so all three race to create the key.
    _deploy(ent, ["api", "worker", "scheduler"], workspace_id=ws)
    # Deploy 2: new containers, empty disks; the old ones are gone.
    _deploy(ent, ["api2", "worker2"], workspace_id=ws)
    written = _records(ent, "r12.deploy.write")
    assert len(written) == 20
    assert len({r["key_id"] for r in written}) == 1, {r["key_id"] for r in written}
    assert all(r["details"]["name"] == "Zoë 名前 ✓" and r["details"]["none"] is None for r in written)

    # This API process restarts too, into an empty disk, and verifies.
    _fresh_container(ent, monkeypatch, "verify-container")
    for report in (_verify(ent), _verify(ent, ws)):
        assert report["verified"] is True, report["findings"][:3]
        assert report["status"] == "verified"
        assert report["categories"]["tampering"] == 0
        assert report["signing"]["source"] == "persisted"
        assert report["signing"]["current_key_id"] == written[0]["key_id"]

    store = _keystore()
    docs = list(ent.db[store.COLLECTION].find({}))
    assert len(docs) == 1, "the keyring was created more than once"
    entry = docs[0]["keys"][0]
    assert entry["key_id"] == written[0]["key_id"]
    assert str(entry["key_enc"]).startswith("enc:v1:"), "the key must be stored encrypted"


# ---------------------------------------------------------------------------
# Rotation
# ---------------------------------------------------------------------------


def test_rotation_signs_new_records_with_the_new_key_and_old_records_still_verify(ent, monkeypatch):
    ws = ent.workspace("R12 rotation")
    _write(3, action="r12.before.rotation", workspace_id=ws)
    before = _records(ent, "r12.before.rotation")
    old_kid = before[0]["key_id"]

    from services import audit_chain_keys

    rotate = getattr(audit_chain_keys, "rotate_signing_key", None)
    assert rotate is not None, "no way to rotate the audit chain key"
    status = rotate(actor="root-admin@example.com")
    assert status["current_key_id"] != old_kid
    retired = {k["key_id"]: k["retired_at"] for k in status["keys"]}
    assert retired[old_kid] and retired[status["current_key_id"]] is None

    _write(3, action="r12.after.rotation", workspace_id=ws)
    after = _records(ent, "r12.after.rotation")
    assert {r["key_id"] for r in after} == {status["current_key_id"]}

    # A redeploy after the rotation: both keys come back from the database.
    _fresh_container(ent, monkeypatch, "after-rotation")
    for report in (_verify(ent), _verify(ent, ws)):
        assert report["verified"] is True, report["findings"][:3]
        assert report["signing"]["current_key_id"] == status["current_key_id"]
        assert old_kid in report["signing"]["known_key_ids"]


# ---------------------------------------------------------------------------
# Import of the old node-local key file
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("keyring_exists", [False, True])
def test_old_node_local_key_file_is_imported_and_its_records_keep_verifying(
    ent, monkeypatch, keyring_exists
):
    store = _keystore()
    ws = ent.workspace("R12 import")
    if not keyring_exists:
        _start_from_an_empty_store(ent)
    # The running (pre-fix) deploy: its key sits in its data directory and
    # signed its records; simulate that by writing them while the keyring
    # cannot see the database.
    old_disk = _fresh_container(ent, monkeypatch, "old-deploy")
    with monkeypatch.context() as m:
        m.setattr(store, "_collection", lambda: None)
        _write(3, action="r12.old.file.key", workspace_id=ws)
    file_key = (old_disk / "audit_chain.key").read_text(encoding="utf-8").strip().encode()
    from services.audit_chain_keys import key_id_for

    old = _records(ent, "r12.old.file.key")
    assert {r["key_id"] for r in old} == {key_id_for(file_key)}

    # The fixed build starts on that same disk and reaches the database.
    _forget_process_keys()
    _write(1, action="r12.after.import", workspace_id=ws)
    doc = ent.db[store.COLLECTION].find_one({"_id": store.DOC_ID})
    by_id = {k["key_id"]: k for k in doc["keys"]}
    assert key_id_for(file_key) in by_id
    assert by_id[key_id_for(file_key)]["origin"] == "imported_local_file"
    if not keyring_exists:
        # No keyring yet: the file key becomes the shared key, so signing continues unchanged.
        assert doc["current_key_id"] == key_id_for(file_key)

    # Next deploy: the file is gone with its container.
    _fresh_container(ent, monkeypatch, "next-deploy")
    report = _verify(ent, ws)
    assert report["verified"] is True, report["findings"][:3]


# ---------------------------------------------------------------------------
# A key that is truly lost
# ---------------------------------------------------------------------------


def test_records_under_a_lost_key_are_reported_apart_from_tampering_never_as_verified(
    ent, monkeypatch
):
    ws = ent.workspace("R12 lost key")
    lost = "lost-" + secrets.token_urlsafe(24)
    from services.audit_chain_keys import key_id_for

    lost_kid = key_id_for(lost.encode())
    # An earlier deploy signed with a key that went with its container.
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", lost)
    _write(4, action="r12.lost.key", workspace_id=ws)
    monkeypatch.delenv("DATAFLOW_AUDIT_HMAC_KEY")
    _forget_process_keys()
    _write(3, action="r12.current.key", workspace_id=ws)

    report = _verify(ent, ws)
    assert report["status"] == "verified_with_exceptions", report["status"]
    assert report["verified"] is False, "records that could not be checked were called verified"
    assert report["tampering_detected"] is False
    assert report["categories"]["tampering"] == 0
    assert report["categories"]["unverifiable_key_lost"] == 4
    assert report["unverifiable_key_lost"]["count"] == 4
    assert report["unverifiable_key_lost"]["by_key_id"] == {lost_kid: 4}
    assert report["unverifiable_key_lost"]["owner_action"]
    assert {f["key_id"] for f in report["findings"]} == {lost_kid}
    assert "DATAFLOW_" not in str(report) and "DATAWRAP_" not in str(report)

    # Tampering next to a lost key is still tampering, and still counted apart.
    victim = _records(ent, "r12.current.key")[0]
    ent.db["audit_events"].update_one({"_id": victim["_id"]}, {"$set": {"resource": "/edited"}})
    report = _verify(ent, ws)
    assert report["status"] == "findings"
    assert report["categories"]["tampering"] == 1
    assert report["categories"]["unverifiable_key_lost"] == 4
    ent.db["audit_events"].update_one({"_id": victim["_id"]}, {"$set": {"resource": victim["resource"]}})

    # The owner finds the old key and supplies it as a retired key.
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_RETIRED_KEYS", lost)
    report = _verify(ent, ws)
    assert report["verified"] is True, report["findings"][:3]
    assert report["status"] == "verified"
    assert report["unverifiable_key_lost"]["count"] == 0
