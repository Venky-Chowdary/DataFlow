"""Evidence Ledger signing keys: custody, create / rotate / retire, export, signing.

Pins: the key id is the SHA-256 of the public key (DER SubjectPublicKeyInfo)
and is recomputable from the exported JWK and PEM; private material never
appears in a view, an export, a repr, the audit event or the store in clear;
a key the vault cannot encrypt is never kept; a retired key verifies and never
signs again; rotation retires the previous set; a hybrid signer signs with
Ed25519 and ML-DSA-65 when the build has it, and "classical" when it does not;
two racing creators end with one keyring.
"""

from __future__ import annotations

import base64
import json
import threading

import pytest

from tools import datawrap_verify as verifier

PAYLOAD = verifier.STATEMENT_PAYLOAD


@pytest.fixture()
def keys(tmp_path, monkeypatch):
    from services import ledger_keys, ledger_store

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(ledger_store, "_db", lambda: None)
    events: list[tuple[str, dict]] = []
    monkeypatch.setattr(ledger_keys, "_audit",
                        lambda action, scope, actor, details: events.append((action, {**details, "actor": actor})))
    ledger_keys.events = events  # type: ignore[attr-defined]
    return ledger_keys


def _stored(tmp_path) -> str:
    return (tmp_path / "evidence_ledger.json").read_text(encoding="utf-8")


def test_create_gives_a_hybrid_key_set_and_never_shows_private_material(keys, tmp_path):
    views = keys.create("ws:w1", actor="admin@example.com")
    assert [v["alg"] for v in views] == (["Ed25519", "ML-DSA-65"] if keys.pq_enabled() else ["Ed25519"])
    assert keys.signature_scheme("ws:w1") == ("hybrid" if keys.pq_enabled() else "classical")
    for view in views:
        assert set(view) == {"key_id", "alg", "role", "workspace_id", "status", "custody", "created_at",
                             "created_by", "retired_at", "retired_by"}
        assert view["role"] == "statement" and view["workspace_id"] == "w1" and view["status"] == "active"
        assert view["custody"] == "vault" and len(view["key_id"]) == 64
    # The store holds the private key only as a vault reference.
    doc = json.loads(_stored(tmp_path))["evidence_ledger_keys"]["ws:w1"]
    for entry in doc["keys"]:
        assert entry["private_enc"].startswith("enc:v1:")
    signer = keys.signer("ws:w1")
    assert "private" not in repr(signer).lower() and not hasattr(signer, "__dict__")
    assert keys.events == [("evidence.key.created", {
        "key_ids": [v["key_id"] for v in views], "algorithms": [v["alg"] for v in views],
        "actor": "admin@example.com"})]
    with pytest.raises(keys.KeyConflict):
        keys.create("ws:w1", actor="admin@example.com")


def test_key_id_is_the_sha256_of_the_public_key_and_exports_round_trip(keys):
    import hashlib

    from cryptography.hazmat.primitives import serialization

    views = keys.create("ws:w1", actor="a")
    for view in views:
        entry, scope = keys.find_key(view["key_id"])
        assert scope == "ws:w1"
        pem = keys.pem(entry)
        public = serialization.load_pem_public_key(pem.encode())
        der = public.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
        assert hashlib.sha256(der).hexdigest() == view["key_id"]
        jwk = keys.jwk(entry)
        assert jwk["kid"] == view["key_id"] and jwk["datawrap_role"] == "statement"
        assert "d" not in jwk and "priv" not in jwk
        for text in (pem, json.dumps(jwk), json.dumps({"keys": [jwk]})):
            loaded = verifier.KeySet()
            loaded.add_text(text)
            assert loaded.get(view["key_id"]).alg == view["alg"]
    ed = next(keys.jwk(keys.find_key(v["key_id"])[0]) for v in views if v["alg"] == "Ed25519")
    assert ed["kty"] == "OKP" and ed["crv"] == "Ed25519" and ed["alg"] == "EdDSA"
    forged = {**ed, "kid": "0" * 64}
    with pytest.raises(ValueError):
        verifier.KeySet().add_jwk(forged)


def test_a_signer_signs_with_every_active_key_and_the_signatures_verify(keys):
    keys.create("ws:w1", actor="a")
    signer = keys.signer("ws:w1")
    body = b'{"hello":"world"}'
    signatures = signer.sign(PAYLOAD, body)
    assert sorted(s["key_id"] for s in signatures) == signer.key_ids
    trusted = keys.trusted_keys(signer.key_ids)
    status, _detail, rows = verifier.check_signature_set(
        {"signer_key_ids": signer.key_ids}, signatures, message=verifier.pae(PAYLOAD, body), keys=trusted,
        role="statement")
    assert status == "passed" and all(r["status"] == "passed" for r in rows)
    # Another payload type, another body, a checkpoint role: none verifies.
    for message, role in ((verifier.pae(verifier.CHECKPOINT_PAYLOAD, body), "statement"),
                          (verifier.pae(PAYLOAD, body + b" "), "statement"),
                          (verifier.pae(PAYLOAD, body), "checkpoint")):
        assert verifier.check_signature_set({"signer_key_ids": signer.key_ids}, signatures, message=message,
                                            keys=trusted, role=role)[0] == "failed"


def test_dropping_one_signature_of_a_hybrid_pair_is_detected(keys):
    if not keys.pq_enabled():
        pytest.skip("this build signs classical only")
    signer = keys.signer("ws:w1")
    body = b"{}"
    signatures = signer.sign(PAYLOAD, body)
    assert signer.scheme == "hybrid" and {s["alg"] for s in signatures} == {"Ed25519", "ML-DSA-65"}
    trusted = keys.trusted_keys(signer.key_ids)
    only_classical = [s for s in signatures if s["alg"] == "Ed25519"]
    status, detail, _rows = verifier.check_signature_set(
        {"signer_key_ids": signer.key_ids}, only_classical, message=verifier.pae(PAYLOAD, body), keys=trusted,
        role="statement")
    assert status == "failed" and "no signature for a listed signer key" in detail


def test_without_ml_dsa_the_signer_is_classical_and_says_so(keys, monkeypatch):
    monkeypatch.setenv("DATAWRAP_EVIDENCE_PQ", "off")
    assert keys.post_quantum_status()["enabled"] is False
    assert "Classical signature only" in keys.post_quantum_status()["note"]
    views = keys.create("ws:w2", actor="a")
    assert [v["alg"] for v in views] == ["Ed25519"]
    assert keys.signer("ws:w2").scheme == "classical" and keys.signature_scheme("ws:w2") == "classical"
    monkeypatch.setattr(verifier, "ml_dsa_available", lambda: False)
    monkeypatch.delenv("DATAWRAP_EVIDENCE_PQ")
    status = keys.post_quantum_status()
    assert status == {"alg": "ML-DSA-65", "available": False, "enabled": False,
                      "note": "Classical signature only: this build has no ML-DSA implementation."}


def test_rotation_retires_the_previous_set_which_still_verifies_and_never_signs(keys):
    keys.create("ws:w1", actor="a")
    old = keys.signer("ws:w1")
    body = b'{"n":1}'
    old_signatures = old.sign(PAYLOAD, body)
    views = keys.rotate("ws:w1", actor="b@example.com")
    by_status = {s: [v["key_id"] for v in views if v["status"] == s] for s in ("active", "retired")}
    assert sorted(by_status["retired"]) == old.key_ids
    new = keys.signer("ws:w1")
    assert sorted(by_status["active"]) == new.key_ids and not set(new.key_ids) & set(old.key_ids)
    retired = next(v for v in views if v["status"] == "retired")
    assert retired["retired_by"] == "b@example.com" and retired["retired_at"]
    trusted = keys.trusted_keys(old.key_ids)
    status, _detail, rows = verifier.check_signature_set(
        {"signer_key_ids": old.key_ids}, old_signatures, message=verifier.pae(PAYLOAD, body), keys=trusted,
        role="statement")
    assert status == "passed" and {r["key_status"] for r in rows} == {"retired"}
    assert keys.events[-1][0] == "evidence.key.rotated"
    assert sorted(keys.events[-1][1]["retired_key_ids"]) == old.key_ids


def test_retiring_every_key_leaves_no_signer_and_nothing_is_created_silently(keys):
    views = keys.create("ws:w1", actor="a")
    for view in views:
        assert keys.retire("ws:w1", view["key_id"], actor="a")["status"] == "retired"
    assert keys.signer("ws:w1") is None and keys.signature_scheme("ws:w1") == "none"
    assert len(keys.list_keys("ws:w1")) == len(views)
    with pytest.raises(keys.KeyConflict):
        keys.retire("ws:w1", views[0]["key_id"], actor="a")
    with pytest.raises(keys.KeyNotFound):
        keys.retire("ws:w1", "f" * 64, actor="a")
    with pytest.raises(keys.KeyNotFound):
        keys.retire("ws:other", views[0]["key_id"], actor="a")
    # An explicit rotation brings a new set.
    keys.rotate("ws:w1", actor="a")
    assert keys.signer("ws:w1") is not None


def test_first_use_creates_the_key_set_and_the_log_scope_holds_checkpoint_keys(keys):
    assert keys.list_keys("log") == []
    signer = keys.signer("log")
    assert signer is not None and {v["role"] for v in keys.list_keys("log")} == {"checkpoint"}
    assert {v["workspace_id"] for v in keys.list_keys("log")} == {None}
    assert keys.events[0][0] == "evidence.key.created" and keys.events[0][1]["actor"] == "system"
    assert set(k["kid"] for k in keys.jwks(["log", "ws:none"])["keys"]) == set(signer.key_ids)


def test_a_vault_that_does_not_encrypt_is_refused_and_no_key_is_kept(keys, monkeypatch, tmp_path):
    from services import secret_vault

    monkeypatch.setattr(secret_vault, "encrypt_secret",
                        lambda plain, **_kw: "enc:v0:" + base64.b64encode(plain.encode()).decode())
    with pytest.raises(keys.KeystoreUnavailable):
        keys.create("ws:w1", actor="a")
    assert keys.list_keys("ws:w1") == []

    def boom(plain, **_kw):
        raise RuntimeError("vault down: " + plain)

    monkeypatch.setattr(secret_vault, "encrypt_secret", boom)
    with pytest.raises(keys.KeystoreUnavailable) as caught:
        keys.create("ws:w1", actor="a")
    assert "ledgerkey" not in str(caught.value)


def test_a_stored_key_that_was_swapped_is_not_used(keys, tmp_path):
    keys.create("ws:w1", actor="a")
    keys.create("ws:w2", actor="a")
    path = tmp_path / "evidence_ledger.json"
    data = json.loads(path.read_text())
    one, two = data["evidence_ledger_keys"]["ws:w1"]["keys"][0], data["evidence_ledger_keys"]["ws:w2"]["keys"][0]
    one["private_enc"] = two["private_enc"]  # another key's private half under this key's id
    path.write_text(json.dumps(data))
    with pytest.raises(keys.KeystoreUnavailable) as caught:
        keys.signer("ws:w1")
    assert "does not match its id" in str(caught.value)


def test_byok_custody_wraps_with_the_tenants_key(keys, monkeypatch):
    from services import secret_vault

    seen: dict[str, str] = {}

    def tenant_wrap(plain, *, tenant_id=None, label=""):
        seen.update(tenant=tenant_id, label=label)
        return "byok:k1:" + base64.b64encode(plain.encode()).decode()

    def tenant_unwrap(stored, *, tenant_id=None):
        assert tenant_id == "tenant-9"
        return base64.b64decode(stored.split(":", 2)[2]).decode()

    monkeypatch.setattr(secret_vault, "tenant_id_from_workspace", lambda ws: "tenant-9" if ws == "w1" else None)
    monkeypatch.setattr(secret_vault, "encrypt_secret", tenant_wrap)
    monkeypatch.setattr(secret_vault, "decrypt_secret", tenant_unwrap)
    views = keys.create("ws:w1", actor="a")
    assert {v["custody"] for v in views} == {"byok"}
    assert seen == {"tenant": "tenant-9", "label": "evidence_ledger_key"}
    assert keys.signer("ws:w1").key_ids == sorted(v["key_id"] for v in views)


def test_two_racing_first_uses_end_with_one_keyring(keys):
    barrier = threading.Barrier(4)
    seen: list[list[str]] = []

    def use():
        barrier.wait()
        seen.append(keys.signer("ws:race").key_ids)

    threads = [threading.Thread(target=use) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(seen) == 4 and all(ids == seen[0] for ids in seen)
    assert len(keys.list_keys("ws:race")) == len(seen[0])
