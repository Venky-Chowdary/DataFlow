"""C4: the audit-chain verdict after rotations, redeploys and a truly lost key.

QA (production, after a deploy): 1,983 records checked, 326 unverifiable
because their signing key was gone, ``verified: false``. Round 12 persisted the
keyring (``audit_chain_keystore``) and split ``unverifiable_key_lost`` from
tampering. This guards the whole QA shape in one chain, through the real
``GET /api/v1/audit/verify`` on a live MongoDB (``r8_ent_harness``):

* records signed under the persisted key, then a rotation, then a redeploy into
  an empty data directory: every one of them verifies from the keyring;
* records an earlier deploy signed under a key nobody kept are reported as
  ``unverifiable_key_lost`` with their key id — never as tampering, never as
  verified — and an edited record next to them is still tampering;
* a keyring-signed record edited and relabelled to a key nobody ever had is
  tampering, not a lost key (independent review repro).

Decided semantics (secure and honest): ``verified`` is true only when every
record in scope was checked and holds up. A record signed under a lost key
cannot be proven unaltered, so it is never counted as verified, and no "known
lost key" allow-list turns it into one (that would let whoever lost a key
launder edits under it). Such a chain reports ``status:
verified_with_exceptions``, ``verified: false``, ``tampering_detected: false``
and ``unverifiable_key_lost: {count, by_key_id}``.
"""

from __future__ import annotations

import secrets

from r8_ent_harness import ent  # noqa: F401
from tests.test_r12_audit_key_persists_e2e import (  # noqa: F401 - autouse fixture
    _forget_process_keys,
    _fresh_container,
    _no_configured_audit_key,
    _records,
    _verify,
    _write,
)


def test_rotation_redeploy_and_a_lost_key_in_one_chain(ent, monkeypatch):
    from services import audit_chain_keys
    from services.audit_chain_keys import key_id_for

    ws = ent.workspace("R14 C4 chain")
    lost = "lost-" + secrets.token_urlsafe(24)
    lost_kid = key_id_for(lost.encode())
    # Deploy 0 signed with a key that went with its container.
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", lost)
    _write(5, action="r14c4.lost", workspace_id=ws)
    monkeypatch.delenv("DATAFLOW_AUDIT_HMAC_KEY")
    _forget_process_keys()
    # Deploy 1: the persisted keyring signs; then the owner rotates.
    _write(4, action="r14c4.persisted", workspace_id=ws)
    persisted_kid = _records(ent, "r14c4.persisted")[0]["key_id"]
    status = audit_chain_keys.rotate_signing_key(actor="root-admin@example.com")
    _write(3, action="r14c4.rotated", workspace_id=ws)
    # Deploy 2: a new container with an empty disk.
    _fresh_container(ent, monkeypatch, "r14c4-redeploy")
    _write(2, action="r14c4.redeployed", workspace_id=ws)
    assert {r["key_id"] for r in _records(ent, "r14c4.rotated")} == {status["current_key_id"]}
    assert {r["key_id"] for r in _records(ent, "r14c4.redeployed")} == {status["current_key_id"]}

    report = _verify(ent, ws)
    assert report["status"] == "verified_with_exceptions", report["status"]
    assert report["verified"] is False
    assert report["tampering_detected"] is False
    assert report["categories"]["tampering"] == 0
    assert report["unverifiable_key_lost"]["count"] == 5
    assert report["unverifiable_key_lost"]["by_key_id"] == {lost_kid: 5}
    unverifiable = {f["event_id"] for f in report["findings"]}
    lost_ids = {str(r.get("id") or r.get("event_id")) for r in _records(ent, "r14c4.lost")}
    assert unverifiable == lost_ids, "a record under a held key was reported unverifiable"
    assert {persisted_kid, status["current_key_id"]} <= set(report["signing"]["known_key_ids"])

    # An edited record under a held key is tampering, counted apart.
    victim = _records(ent, "r14c4.rotated")[0]
    ent.db["audit_events"].update_one({"_id": victim["_id"]}, {"$set": {"actor": "x@example.com"}})
    report = _verify(ent, ws)
    assert report["status"] == "findings"
    assert report["tampering_detected"] is True
    assert report["categories"]["tampering"] == 1
    assert report["unverifiable_key_lost"]["count"] == 5


def test_an_edited_record_relabelled_to_an_unknown_key_is_tampering(ent):
    """Reviewer repro: edit a keyring-signed record and set its key_id to a key
    nobody ever had. It used to read as a lost key (verified_with_exceptions,
    tampering_detected false) — hiding the edit. After the keyring signed the
    chain, an unknown key id is a relabel."""
    ws = ent.workspace("R14 C4 relabel")
    _write(3, action="r14c4.relabel", workspace_id=ws)
    victim = _records(ent, "r14c4.relabel")[1]
    ent.db["audit_events"].update_one(
        {"_id": victim["_id"]},
        {"$set": {"resource": "/edited", "key_id": "k-nobody-ever-had"}},
    )
    report = _verify(ent, ws)
    assert report["status"] == "findings", report["status"]
    assert report["tampering_detected"] is True
    assert report["categories"]["tampering"] == 1
    assert report["unverifiable_key_lost"]["count"] == 0
    kinds = [f["kind"] for f in report["findings"]]
    assert kinds == ["unknown_key_after_keyring"], kinds
    # The platform-wide walk agrees.
    platform = _verify(ent)
    assert any(
        f["kind"] == "unknown_key_after_keyring" and f["category"] == "tampering"
        for f in platform["findings"]
    ), platform["findings"][:3]


def _relabel(ent, record, key_id: str) -> None:
    ent.db["audit_events"].update_one(
        {"_id": record["_id"]}, {"$set": {"resource": "/edited", "key_id": key_id}}
    )


def _tampered(report: dict) -> list[str]:
    return [f["kind"] for f in report["findings"] if f["category"] == "tampering"]


def test_a_forged_history_row_does_not_excuse_a_relabel(ent):
    """Second review: the key history was a plain collection; inserting
    {_id: 'k-forged'} and relabelling an edited record to it hid the edit."""
    from services.audit_chain_keystore import HISTORY_COLLECTION

    ws = ent.workspace("R14 C4 forged history")
    _write(3, action="r14c4.forged", workspace_id=ws)
    ent.db[HISTORY_COLLECTION].insert_one(
        {"_id": "k-forged", "origin": "configured", "last_seen": "2999-01-01T00:00:00+00:00"}
    )
    _relabel(ent, _records(ent, "r14c4.forged")[1], "k-forged")
    report = _verify(ent, ws)
    assert _tampered(report) == ["unknown_key_after_keyring"], report["findings"]
    assert report["unverifiable_key_lost"]["count"] == 0


def test_a_relabel_to_a_known_but_lost_key_after_it_stopped_signing_is_tampering(ent, monkeypatch):
    import time

    from services import audit_chain_keystore, evidence_chain
    from services.audit_chain_keys import key_id_for

    monkeypatch.setattr(audit_chain_keystore, "HISTORY_REFRESH_S", 0.0)
    monkeypatch.setattr(evidence_chain, "RELABEL_SLACK", __import__("datetime").timedelta(seconds=1))
    ws = ent.workspace("R14 C4 known lost key")
    env_key = "env-" + secrets.token_urlsafe(24)
    kid = key_id_for(env_key.encode())
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", env_key)
    _write(3, action="r14c4.env", workspace_id=ws)
    monkeypatch.delenv("DATAFLOW_AUDIT_HMAC_KEY")
    _forget_process_keys()
    time.sleep(2.5)
    _write(3, action="r14c4.after", workspace_id=ws)
    _relabel(ent, _records(ent, "r14c4.after")[2], kid)
    report = _verify(ent, ws)
    # The env key's own records read as a lost key; the relabel does not.
    assert report["unverifiable_key_lost"]["by_key_id"] == {kid: 3}, report["unverifiable_key_lost"]
    assert _tampered(report) == ["unknown_key_after_keyring"], report["findings"]


def test_env_key_only_deployment_flags_a_relabel(ent, monkeypatch):
    ws = ent.workspace("R14 C4 env only")
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", "env-only-" + secrets.token_urlsafe(24))
    _forget_process_keys()
    _write(3, action="r14c4.envonly", workspace_id=ws)
    _relabel(ent, _records(ent, "r14c4.envonly")[1], "k-nobody-ever-had")
    report = _verify(ent, ws)
    assert _tampered(report) == ["unknown_key_after_keyring"], report["findings"]


def test_imported_file_keyring_flags_a_relabel(ent, monkeypatch):
    """A keyring whose only key was imported from a node file (origin
    imported_local_file) had no anchor, so every relabel read as lost."""
    from services import audit_chain_keystore as store

    ws = ent.workspace("R14 C4 imported keyring")
    ent.db["audit_events"].delete_many({})
    ent.db[store.COLLECTION].delete_many({})
    _fresh_container(ent, monkeypatch, "imported-old")
    with monkeypatch.context() as m:
        m.setattr(store, "_collection", lambda: None)
        _write(1, action="r14c4.pre", workspace_id=ws)
    _forget_process_keys()
    _write(3, action="r14c4.imported", workspace_id=ws)
    doc = ent.db[store.COLLECTION].find_one({"_id": store.DOC_ID})
    assert {k["origin"] for k in doc["keys"]} == {"imported_local_file"}, doc["keys"]
    _relabel(ent, _records(ent, "r14c4.imported")[1], "k-nobody-ever-had")
    report = _verify(ent, ws)
    assert _tampered(report) == ["unknown_key_after_keyring"], report["findings"]
