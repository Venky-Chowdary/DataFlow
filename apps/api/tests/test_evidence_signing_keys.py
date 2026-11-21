"""Evidence signing keys (trust audit P0-1, P0-2).

Proof packs, certificates, signed Trust Reports and Exit Evidence Packs were
HMAC-signed with the login token secret under ``key_id`` ``platform_auth_secret``
while the product said "workspace proof key". These tests pin the replacement:

* each workspace signs with its own key, kept encrypted in the evidence keyring
  and never equal to (or derived from) the login secret;
* every signature names its key id, key version and keyring;
* rotation keeps every earlier pack verifiable;
* packs signed with the login secret before this change still verify, named as
  the legacy key; nothing new is signed with it;
* a workspace's keyring is not an oracle for a caller who cannot read it;
* key status never carries key material;
* the verify routes are reads (``job.read`` / ``audit.read``), so a viewer can verify.
"""

from __future__ import annotations

import hashlib
import hmac
import json

import pytest

from services import evidence_signing_keys as esk
from services.signed_proof_pack import (
    build_signed_parity_pack,
    build_signed_proof_pack,
    canonical_json,
    sha256_hex,
    verify_signed_proof_pack,
)


@pytest.fixture(autouse=True)
def _isolated_keyring(tmp_path, monkeypatch):
    """Each test gets an empty file-backed keyring (no control-plane database)."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(esk, "_collection", lambda: None)
    monkeypatch.setattr("services.auth_service._token_secret", lambda: "login-secret-" + "x" * 40)
    yield


def _pack(job_id: str = "job-1", workspace_id: str | None = "ws-a") -> dict:
    pack = build_signed_proof_pack(
        job_id=job_id,
        reconciliation={"passed": True, "source_rows": 10, "target_rows": 10},
        workspace_id=workspace_id,
    )
    return json.loads(json.dumps(pack))


def test_signature_names_a_workspace_evidence_key_not_the_login_secret():
    pack = _pack()
    sig = pack["signature"]
    assert sig["alg"] == "HMAC-SHA256"
    assert sig["key_id"].startswith("ev1:") and sig["key_id"] != esk.LEGACY_KEY_ID
    assert sig["key_scope"] == "workspace"
    assert sig["workspace_id"] == "ws-a"
    assert sig["key_version"] == 1
    login = b"login-secret-" + b"x" * 40
    forged = hmac.new(login, f"{pack['content_sha256']}:job-1".encode(), hashlib.sha256).hexdigest()
    assert sig["value"] != forged, "the login secret must not produce a valid evidence signature"
    result = verify_signed_proof_pack(pack)
    assert result["ok"] is True, result
    assert result["signing_key"] == {
        "key_id": sig["key_id"], "key_scope": "workspace", "workspace_id": "ws-a", "legacy_key": False,
    }


def test_workspaces_have_separate_keys_and_no_workspace_uses_the_deployment_keyring():
    a, b, none = _pack(workspace_id="ws-a"), _pack(workspace_id="ws-b"), _pack(workspace_id=None)
    assert a["signature"]["key_id"] != b["signature"]["key_id"]
    assert none["signature"]["key_scope"] == "deployment"
    assert none["signature"]["workspace_id"] == ""
    assert all(verify_signed_proof_pack(p)["ok"] for p in (a, b, none))
    # A's signature does not verify under B's keyring (relabelled envelope).
    relabelled = {**a, "signature": {**a["signature"], "workspace_id": "ws-b"}}
    out = verify_signed_proof_pack(relabelled)
    assert out["ok"] is False
    assert any("not in this deployment's evidence keyring" in e for e in out["errors"])


def test_rotation_keeps_earlier_packs_verifiable_and_signs_new_ones_with_the_new_key():
    before = _pack(job_id="job-before")
    status = esk.rotate("ws-a", actor="admin@example.com")
    assert status["active_version"] == 2
    assert [v["version"] for v in status["versions"]] == [1, 2]
    assert status["versions"][0]["retired_at"] and not status["versions"][1]["retired_at"]
    after = _pack(job_id="job-after")
    assert after["signature"]["key_id"] == status["active_key_id"] != before["signature"]["key_id"]
    assert after["signature"]["key_version"] == 2
    assert verify_signed_proof_pack(before)["ok"] is True
    assert verify_signed_proof_pack(after)["ok"] is True


def test_tampered_pack_fails_and_a_key_id_nobody_holds_is_named():
    pack = _pack()
    tampered = {**pack, "gate8": {**pack.get("gate8", {}), "passed": False}}
    out = verify_signed_proof_pack(tampered)
    assert out["ok"] is False and "content_sha256 mismatch" in out["errors"]
    unknown = {**pack, "signature": {**pack["signature"], "key_id": "ev1:0000000000000000"}}
    out = verify_signed_proof_pack(unknown)
    assert out["ok"] is False
    assert "signing key ev1:0000000000000000 is not in this deployment's evidence keyring" in out["errors"]


def test_legacy_login_secret_packs_still_verify_and_are_named_legacy(monkeypatch):
    """A pack issued before evidence keys (key_id platform_auth_secret) keeps
    verifying when the evidence chain recorded it before the cutover (T2);
    one it never recorded is refused, named as the legacy key."""
    from services.signed_proof_pack import LEGACY_UNANCHORED, pack_body_digest_excluding_anchor

    body = {"version": "1", "job_id": "job-old", "assurance": {}, "gate8": {"passed": True}}
    digest = sha256_hex(canonical_json(body))
    login = b"login-secret-" + b"x" * 40
    legacy = {
        **body,
        "content_sha256": digest,
        "signature": {
            "alg": "HMAC-SHA256", "key_id": "platform_auth_secret",
            "value": hmac.new(login, f"{digest}:job-old".encode(), hashlib.sha256).hexdigest(),
        },
    }
    refused = verify_signed_proof_pack(legacy)
    assert refused["ok"] is False and refused["legacy_status"] == LEGACY_UNANCHORED
    recorded = {"time": "2020-01-01T00:00:00+00:00",
                "details": {"evidence_sha256": pack_body_digest_excluding_anchor(legacy)}}
    monkeypatch.setattr(
        "services.evidence_chain.find_anchor",
        lambda digest, **_kw: recorded if digest == recorded["details"]["evidence_sha256"] else None,
    )
    out = verify_signed_proof_pack(legacy)
    assert out["ok"] is True, out
    assert out["signing_key"]["legacy_key"] is True
    assert out["signing_key"]["key_scope"] == "legacy_login_secret"
    # The legacy key verifies only what it signed: a new pack is never signed with it.
    assert _pack()["signature"]["key_id"] != "platform_auth_secret"


def test_a_workspace_keyring_is_not_an_oracle_for_a_caller_who_cannot_read_it():
    pack = _pack(workspace_id="ws-a")
    refused = verify_signed_proof_pack(pack, may_use_workspace=lambda ws: ws == "ws-b")
    assert refused["ok"] is False
    assert refused["errors"] == [
        "signed with another workspace's evidence key: it can be verified only by someone "
        "with access to that workspace"
    ]
    assert verify_signed_proof_pack(pack, may_use_workspace=lambda ws: ws == "ws-a")["ok"] is True


def test_key_material_is_stored_encrypted_and_never_reported(tmp_path):
    pack = _pack()
    stored = (tmp_path / "evidence_signing_keys.json").read_text()
    entry = json.loads(stored)["keys"]["ws-a"]["versions"][0]
    assert entry["key_enc"].startswith("enc:"), "key material must go through the secret vault"
    key = esk.key_for("ws-a", pack["signature"]["key_id"])
    assert key and key.hex() not in stored
    status = esk.key_status("ws-a")
    assert set(status["versions"][0]) == {"version", "key_id", "created_at", "created_by", "retired_at"}
    assert key.hex() not in json.dumps(status)
    assert status["separate_from_login"] is True


def test_signed_trust_report_and_exit_pack_use_the_workspace_key():
    from services.signed_proof_pack import build_signed_exit_pack

    parity = json.loads(json.dumps(build_signed_parity_pack(
        job_id="job-p", report={"verdict": "proven"}, actor="a@example.com", workspace_id="ws-a",
    )))
    exit_pack = json.loads(json.dumps(build_signed_exit_pack(
        {"system_id": "s1", "rehearsal_id": "r1", "issued_at": "x"}, workspace_id="ws-a",
    )))
    for pack in (parity, exit_pack):
        assert pack["signature"]["workspace_id"] == "ws-a"
        assert verify_signed_proof_pack(pack)["ok"] is True


def test_migration_certificate_signature_uses_the_job_workspace_key(monkeypatch):
    from services.migration_certificate import verify_migration_certificate
    from services.signed_proof_pack import sign_body

    cert = json.loads(json.dumps(sign_body(
        {"job": {"job_id": "job-c"}, "verdict": {}}, subject="job-c", workspace_id="ws-a",
    )))
    out = verify_migration_certificate(cert)
    assert out["ok"] is True and out["signing_key"]["workspace_id"] == "ws-a"
    assert verify_migration_certificate(cert, may_use_workspace=lambda ws: False)["ok"] is False


@pytest.mark.parametrize("route, permission", [
    ("/api/v1/transfer/proof-pack/verify", "job.read"),
    ("/api/v1/transfer/certificate/verify", "job.read"),
    ("/api/v1/audit/verify-pack", "audit.read"),
    ("/api/v1/exit/evidence/verify", "job.read"),
])
def test_verify_routes_are_reads_a_viewer_holds(route, permission):
    """P0-2 / P1-7: a read-only auditor can verify; no route needs connector.write."""
    from services.rbac import _required_permission, role_permissions

    assert _required_permission("POST", route) == permission
    assert permission in role_permissions("viewer")
