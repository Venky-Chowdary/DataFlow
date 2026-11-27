"""T1 / T2 (train review): what a signature envelope may claim.

T1: ``signature.key_scope`` is not authenticated. Relabelling a workspace
pack's key_scope ("deployment", "", "Workspace") kept workspace_id and made
the verifier use that workspace's keyring with no read check — a
cross-workspace oracle. The scope is now resolved from where the key_id is
found, and a named workspace always needs ``may_use_workspace``.

T2: a pack HMAC'd with the login secret under ``platform_auth_secret``
verified for anyone. The login secret now verifies only a pack whose digest
the legacy evidence register recorded at the cutover (review 2 item 4: the
register replaced the evidence-chain lookup, which purge and the 20,000-record
window defeated; see test_train1_r2_legacy_evidence_register).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time

import pytest

from services import audit_log, evidence_signing_keys as esk
from services import legacy_evidence_register as register
from services.signed_proof_pack import (
    LEGACY_NOT_ON_RECORD,
    LEGACY_NOT_ON_RECORD_REASON,
    LEGACY_ON_RECORD,
    build_signed_proof_pack,
    canonical_json,
    pack_body_digest_excluding_anchor,
    sha256_hex,
    verify_signed_proof_pack,
)

_LOGIN = b"login-secret-" + b"x" * 40


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(esk, "_collection", lambda: None)
    monkeypatch.setattr("services.auth_service._token_secret", lambda: _LOGIN.decode())
    monkeypatch.setattr(register, "_collection", lambda: None)
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit_events.jsonl")

    def _anchor(digest: str) -> None:
        from services.evidence_chain import EVIDENCE_ACTION

        audit_log.append_audit_event(action=EVIDENCE_ACTION, resource="job:legacy",
                                     details={"evidence_sha256": digest})

    yield _anchor


def _ws_pack(workspace_id: str = "ws-a") -> dict:
    return json.loads(json.dumps(build_signed_proof_pack(
        job_id="job-1",
        reconciliation={"passed": True, "source_rows": 10, "target_rows": 10},
        workspace_id=workspace_id,
    )))


@pytest.mark.parametrize("relabel", ["deployment", "", "Workspace", "WORKSPACE", "legacy"])
def test_relabelled_key_scope_does_not_bypass_the_workspace_read_check(relabel):
    pack = _ws_pack()
    forged = {**pack, "signature": {**pack["signature"], "key_scope": relabel}}
    out = verify_signed_proof_pack(forged, may_use_workspace=lambda ws: False)
    assert out["ok"] is False
    assert any("another workspace's evidence key" in e for e in out["errors"]), out
    # The caller who may read ws-a still verifies it, and the scope reported
    # is where the key was found, not what the envelope said.
    ok = verify_signed_proof_pack(forged, may_use_workspace=lambda ws: ws == "ws-a")
    assert ok["ok"] is True, ok
    assert ok["signing_key"]["key_scope"] == "workspace"


def test_dropping_the_workspace_id_does_not_find_the_key_in_the_deployment_ring():
    pack = _ws_pack()
    forged = {**pack, "signature": {**pack["signature"], "workspace_id": "", "key_scope": "deployment"}}
    out = verify_signed_proof_pack(forged, may_use_workspace=lambda ws: False)
    assert out["ok"] is False
    assert any("is not in this deployment's evidence keyring" in e for e in out["errors"]), out


def _legacy(job_id: str, extra: dict | None = None) -> dict:
    body = {"version": "1", "job_id": job_id, "assurance": {}, "gate8": {"passed": True}, **(extra or {})}
    digest = sha256_hex(canonical_json(body))
    return {
        **body,
        "content_sha256": digest,
        "signature": {
            "alg": "HMAC-SHA256", "key_id": "platform_auth_secret",
            "value": hmac.new(_LOGIN, f"{digest}:{job_id}".encode(), hashlib.sha256).hexdigest(),
        },
    }


def test_a_new_login_secret_pack_never_verifies(_isolated):
    _ws_pack()  # evidence keys exist: the cutover has happened
    forged = _legacy("job-new")
    out = verify_signed_proof_pack(forged)
    assert out["ok"] is False
    assert out["legacy_status"] == LEGACY_NOT_ON_RECORD
    assert LEGACY_NOT_ON_RECORD_REASON in out["errors"]
    assert out["signing_key"]["legacy_key"] is True


def test_a_legacy_pack_recorded_before_the_cutover_verifies(_isolated):
    old = _legacy("job-old")
    _isolated(pack_body_digest_excluding_anchor(old))  # filed by the old build
    time.sleep(0.01)
    _ws_pack()  # first evidence keyring: the cutover
    out = verify_signed_proof_pack(old)
    assert out["ok"] is True, out
    assert out["legacy_status"] == LEGACY_ON_RECORD


def test_a_legacy_signature_on_a_pack_recorded_after_the_cutover_is_refused(_isolated):
    _ws_pack()
    time.sleep(0.01)
    late = _legacy("job-late")
    _isolated(pack_body_digest_excluding_anchor(late))
    out = verify_signed_proof_pack(late)
    assert out["ok"] is False
    assert out["legacy_status"] == LEGACY_NOT_ON_RECORD
