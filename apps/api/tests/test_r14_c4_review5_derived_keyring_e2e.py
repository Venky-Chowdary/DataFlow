"""C4, fifth review: the keyring cannot lose a key by being edited.

The keyring was unauthenticated Mongo state holding the key material, so
pulling a key (or deleting the document) turned every edit of its records
into a "lost key". Keyring-era chain keys are now derived from the vault
master (HKDF, by generation); the document holds sealed metadata only, and
the chain signs the key sets (``audit.keyring.sealed``, began / re-anchor).
Live MongoDB through ``GET /api/v1/audit/verify`` (``r8_ent_harness``).
"""

from __future__ import annotations

import secrets

from r8_ent_harness import ent  # noqa: F401
from tests.test_r12_audit_key_persists_e2e import (  # noqa: F401 - autouse fixture
    _forget_process_keys,
    _no_configured_audit_key,
    _records,
    _verify,
    _write,
)

ROUTE = "/api/v1/audit/chain/key-history/reanchor"


def _keyring(ent) -> dict:
    from services import audit_chain_keystore as store

    return ent.db[store.COLLECTION].find_one({"_id": store.DOC_ID})


def _pull(ent, key_id: str) -> None:
    from services import audit_chain_keystore as store

    ent.db[store.COLLECTION].update_one({"_id": store.DOC_ID}, {"$pull": {"keys": {"key_id": key_id}}})


def _edit(ent, record) -> None:
    ent.db["audit_events"].update_one({"_id": record["_id"]}, {"$set": {"resource": "/edited"}})


def _kinds(report: dict) -> list[str]:
    return [f["kind"] for f in report["findings"] if f["category"] == "tampering"]


def _rotate():
    from services import audit_chain_keys

    return audit_chain_keys.rotate_signing_key(actor="root-admin@example.com")


def test_p1_pulling_a_retired_key_then_editing_its_records_is_tampering(ent):
    ws = ent.workspace("R14 C4 r5 p1")
    _write(3, action="r14c4r5.p1.k1", workspace_id=ws)
    k1 = _records(ent, "r14c4r5.p1.k1")[0]["key_id"]
    _rotate()
    _write(2, action="r14c4r5.p1.k2", workspace_id=ws)
    _pull(ent, k1)
    _edit(ent, _records(ent, "r14c4r5.p1.k1")[1])
    _forget_process_keys()
    report = _verify(ent)
    assert _kinds(report) == ["event_hash_mismatch"], report["findings"]
    assert report["unverifiable_key_lost"]["count"] == 0
    assert report["keyring_tampered"], report


def test_p1b_pulling_the_current_key_then_editing_its_records_is_tampering(ent):
    ws = ent.workspace("R14 C4 r5 p1b")
    _rotate()
    _write(3, action="r14c4r5.p1b", workspace_id=ws)
    k2 = _keyring(ent)["current_key_id"]
    for record in _records(ent, "r14c4r5.p1b"):
        assert record["key_id"] == k2
        _edit(ent, record)
    _pull(ent, k2)
    _forget_process_keys()
    report = _verify(ent, ws)
    assert _kinds(report).count("event_hash_mismatch") == 3, report["findings"]
    assert report["unverifiable_key_lost"]["count"] == 0
    assert report["keyring_tampered"], report


def test_p2_deleting_the_keyring_and_the_history_loses_nothing(ent):
    from services import audit_chain_keystore as store

    ws = ent.workspace("R14 C4 r5 p2")
    _rotate()  # generation 2 signs: recreating the keyring cannot bring it back unnoticed
    _write(4, action="r14c4r5.p2", workspace_id=ws)
    _edit(ent, _records(ent, "r14c4r5.p2")[2])
    ent.db[store.COLLECTION].delete_many({})
    ent.db[store.HISTORY_COLLECTION].delete_many({})
    _forget_process_keys()
    _write(1, action="r14c4r5.p2.after", workspace_id=ws)
    report = _verify(ent)
    assert report["unverifiable_key_lost"]["count"] == 0, report["unverifiable_key_lost"]
    assert "event_hash_mismatch" in _kinds(report), report["findings"]
    assert "reset" in (report["key_history_tampered"] or ""), report["key_history_tampered"]
    assert report["keyring_tampered"], report


def test_a_normal_rotation_verifies_and_needs_no_key_material_on_the_document(ent):
    ws = ent.workspace("R14 C4 r5 rotation")
    _write(2, action="r14c4r5.rot.before", workspace_id=ws)
    _rotate()
    _write(2, action="r14c4r5.rot.after", workspace_id=ws)
    doc = _keyring(ent)
    assert all("key_enc" not in e and e["generation"] for e in doc["keys"]), doc["keys"]
    _forget_process_keys()
    report = _verify(ent)
    assert report["status"] == "verified", report["findings"][:3]
    assert report["keyring_tampered"] is None


def test_an_env_key_only_deployment_still_reads_a_dropped_env_key_as_lost(ent, monkeypatch):
    ws = ent.workspace("R14 C4 r5 env")
    env_key = "env-r5-" + secrets.token_urlsafe(24)
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", env_key)
    _forget_process_keys()
    _write(3, action="r14c4r5.env", workspace_id=ws)
    monkeypatch.delenv("DATAFLOW_AUDIT_HMAC_KEY")
    _forget_process_keys()
    _write(1, action="r14c4r5.env.after", workspace_id=ws)
    report = _verify(ent, ws)
    assert report["status"] == "verified_with_exceptions", (report["status"], report["findings"][:3])
    assert report["unverifiable_key_lost"]["count"] == 3
    assert _kinds(report) == [] and report["keyring_tampered"] is None


def test_an_upgrade_from_random_keys_seals_them_and_a_later_pull_is_tampering(ent, monkeypatch):
    import secrets as s

    from services import audit_chain_keystore as store
    from services.audit_chain_keys import key_id_for
    from services.secret_vault import encrypt_secret

    ws = ent.workspace("R14 C4 r5 upgrade")
    # A keyring from before derived keys: one random key, encrypted, no seal.
    legacy = s.token_urlsafe(48).encode()
    ent.db[store.COLLECTION].replace_one({"_id": store.DOC_ID}, {
        "_id": store.DOC_ID, "current_key_id": key_id_for(legacy), "revision": 1,
        "keys": [{"key_id": key_id_for(legacy), "key_enc": encrypt_secret(legacy.decode()),
                  "origin": "generated", "created_at": "2026-01-01T00:00:00+00:00",
                  "created_by": "system", "retired_at": None}],
    }, upsert=True)
    _forget_process_keys()
    with monkeypatch.context() as m:
        m.setattr(store, "_upgrade", lambda coll, doc: None)  # the pre-upgrade build
        _write(3, action="r14c4r5.legacy", workspace_id=ws)
    assert {r["key_id"] for r in _records(ent, "r14c4r5.legacy")} == {key_id_for(legacy)}
    _forget_process_keys()
    _write(2, action="r14c4r5.upgraded", workspace_id=ws)
    doc = _keyring(ent)
    assert doc["seal"] and any(e.get("generation") for e in doc["keys"])
    assert {r["key_id"] for r in _records(ent, "r14c4r5.upgraded")} == {doc["current_key_id"]}
    assert ent.db["audit_events"].find_one({"action": store.KEYRING_SEAL_ACTION}) is not None
    _forget_process_keys()
    assert _verify(ent)["status"] == "verified"
    # The legacy key is pulled and one of its records edited.
    _pull(ent, key_id_for(legacy))
    _edit(ent, _records(ent, "r14c4r5.legacy")[1])
    _forget_process_keys()
    report = _verify(ent, ws)
    assert "keyring_key_removed" in _kinds(report), report["findings"]
    assert report["unverifiable_key_lost"]["count"] == 0


def test_a_secrets_key_change_is_inconclusive_until_a_reanchor_rekeys_the_keyring(ent, monkeypatch):
    ws = ent.workspace("R14 C4 r5 secrets")
    _write(3, action="r14c4r5.secrets", workspace_id=ws)
    for name in ("DATAFLOW_SECRETS_KEY", "DATAWRAP_SECRETS_KEY"):
        monkeypatch.setenv(name, "rotated-secrets-" + secrets.token_urlsafe(16))
    _forget_process_keys()
    report = _verify(ent)
    assert report["secrets_key_changed"] is True, report
    assert report["status"] == "inconclusive" and report["tampering_detected"] is False
    assert report["unverifiable_key_lost"]["count"] == 0
    assert ent.admin.post(ROUTE, json={"reason": "vault secrets key rotated"}).status_code == 409
    applied = ent.admin.post(ROUTE, json={"reason": "vault secrets key rotated", "dry_run": False,
                                          "acknowledge_findings": True})
    assert applied.status_code == 200, applied.text
    assert applied.json()["keyring_rekeyed"]["lost"], applied.json()
    _forget_process_keys()
    _write(2, action="r14c4r5.secrets.after", workspace_id=ws)
    after = _verify(ent)
    assert after["tampering_detected"] is False, after["findings"][:3]
    assert after["status"] == "verified_with_exceptions", (after["status"], after["findings"][:3])
    assert after["unverifiable_key_lost"]["count"] >= 3
    fresh = _verify(ent, ws)
    assert fresh["keyring_tampered"] is None and fresh["secrets_key_changed"] is False
