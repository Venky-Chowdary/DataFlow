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
* the semantics of ``verified`` are part of the route's published contract.

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


def test_verified_semantics_are_part_of_the_route_contract():
    """The route docstring is its OpenAPI description: clients read it there."""
    from src.routers.audit_router import verify_audit_chain

    text = verify_audit_chain.__doc__ or ""
    for term in ("verified_with_exceptions", "unverifiable_key_lost", "tampering_detected"):
        assert term in text, f"/audit/verify does not document {term}"
    assert "never counted as verified" in text
