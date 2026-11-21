"""T1 / T2 (train review): what a signature envelope may claim.

T1: ``signature.key_scope`` is not authenticated. Relabelling a workspace
pack's key_scope ("deployment", "", "Workspace") kept workspace_id and made
the verifier use that workspace's keyring with no read check — a
cross-workspace oracle. The scope is now resolved from where the key_id is
found, and a named workspace always needs ``may_use_workspace``.

T2: a pack HMAC'd with the login secret under ``platform_auth_secret``
verified for anyone. The login secret now verifies only a pack the evidence
chain recorded before evidence signing keys existed.
"""

from __future__ import annotations

import json

import pytest

from services import evidence_signing_keys as esk
from services.signed_proof_pack import build_signed_proof_pack, verify_signed_proof_pack

_LOGIN = b"login-secret-" + b"x" * 40


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(esk, "_collection", lambda: None)
    monkeypatch.setattr("services.auth_service._token_secret", lambda: _LOGIN.decode())
    chain: list[dict] = []

    def _anchor(digest: str) -> None:
        chain.append({"action": "evidence.anchor", "time": esk._now(),
                      "details": {"evidence_sha256": digest}})

    def _find(digest: str, **_kw):
        return next((e for e in reversed(chain) if e["details"]["evidence_sha256"] == digest), None)

    monkeypatch.setattr("services.evidence_chain.find_anchor", _find)
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


