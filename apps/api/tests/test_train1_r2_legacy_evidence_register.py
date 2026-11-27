"""Review 2 item 4: legacy (login-secret) evidence stays verifiable after the cutover.

T2 accepted a login-secret signature only when the evidence chain still held
an anchor for the pack: only build_signed_proof_pack(anchor_in_chain=True)
files one, the lookup read the newest 20,000 audit records and the retention
purge deletes them, so pre-cutover Migration Certificates, parity sign-off
packs, Exit Evidence Packs and unanchored proof packs became permanently
unverifiable. The legacy evidence register records, at the cutover, the
digest of every legacy-signed evidence object the server holds; a
login-secret pack verifies when its digest is on record, and only then.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone

import pytest

from services import audit_log, evidence_signing_keys as esk, exit_store
from services import legacy_evidence_register as register
from services.signed_proof_pack import (
    EXIT_PACK_KIND,
    LEGACY_NOT_ON_RECORD,
    LEGACY_ON_RECORD,
    PARITY_PACK_KIND,
    build_signed_proof_pack,
    canonical_json,
    pack_body_digest_excluding_anchor,
    pack_subject,
    sha256_hex,
    verify_signed_proof_pack,
)

_LOGIN = b"login-secret-" + b"x" * 40


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    from services import mongodb_service

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(esk, "_collection", lambda: None)
    monkeypatch.setattr(register, "_collection", lambda: None)
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit_events.jsonl")
    monkeypatch.setattr(exit_store, "_db", lambda: None)
    monkeypatch.setattr(exit_store, "STORE_PATH", tmp_path / "exit_inventory.json")
    memory = mongodb_service.MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongodb_service, "_mongodb_service", memory)
    monkeypatch.setattr("services.auth_service._token_secret", lambda: _LOGIN.decode())
    yield memory


def _legacy(body: dict) -> dict:
    """``body`` signed the way every pack was before evidence keys existed."""
    digest = sha256_hex(canonical_json(body))
    pack = {**body, "content_sha256": digest}
    subject = pack_subject(pack)
    pack["signature"] = {
        "alg": "HMAC-SHA256", "key_id": "platform_auth_secret",
        "value": hmac.new(_LOGIN, f"{digest}:{subject}".encode(), hashlib.sha256).hexdigest(),
    }
    return pack


def _seed_pre_cutover_evidence(memory) -> dict[str, dict]:
    from services.evidence_chain import EVIDENCE_ACTION

    proof = _legacy({"version": "1", "job_id": "job-proof", "assurance": {}, "gate8": {"passed": True}})
    cert = _legacy({"version": "1", "job_id": "job-cert", "job": {"job_id": "job-cert"}, "verdict": {}})
    anchored = _legacy({"version": "1", "job_id": "job-anchor", "assurance": {}, "gate8": {"passed": True}})
    parity = _legacy({"kind": PARITY_PACK_KIND, "job_id": "job-parity", "verdict": "proven"})
    exit_pack = _legacy({"kind": EXIT_PACK_KIND, "system_id": "sys1", "rehearsal_id": "r1"})
    audit_log.append_audit_event(action="proof_pack.export", resource="job:job-proof",
                                 details={"content_sha256": proof["content_sha256"]})
    audit_log.append_audit_event(action="migration_certificate.export", resource="job:job-cert",
                                 details={"content_sha256": cert["content_sha256"]})
    audit_log.append_audit_event(action=EVIDENCE_ACTION, resource="job:job-anchor",
                                 details={"evidence_sha256": pack_body_digest_excluding_anchor(anchored),
                                          "job_id": "job-anchor"})
    memory._jobs["job-parity"] = {"job_id": "job-parity", "workspace_id": "",
                                  "parity": {"signed": {"pack": parity, "signed_at": "2026-11-01"}}}
    exit_store.insert(exit_store.REHEARSALS, {"id": "r1", "workspace_id": "",
                                              "signed": {"pack": exit_pack, "signed_at": "2026-11-01"}})
    return {"proof_pack": proof, "migration_certificate": cert, "anchored_proof_pack": anchored,
            "parity_signoff_pack": parity, "exit_evidence_pack": exit_pack}


def _verify(kind: str, pack: dict) -> dict:
    if kind == "migration_certificate":
        from services.migration_certificate import verify_migration_certificate

        return verify_migration_certificate(pack)
    return verify_signed_proof_pack(pack)


def _flood_and_purge(monkeypatch) -> None:
    """20,001 newer audit records, then a retention purge of everything old."""
    path = audit_log.STORE_PATH
    with path.open("a", encoding="utf-8") as fh:
        for i in range(20_001):
            fh.write(json.dumps({"id": f"n{i}", "action": "job.read", "time": "2027-06-01T00:00:00+00:00",
                                 "details": {}}) + "\n")
    monkeypatch.setenv("DATAFLOW_AUDIT_RETENTION_DAYS", "30")
    audit_log.purge_expired_audit_events(now=datetime(2028, 1, 1, tzinfo=timezone.utc))
    assert not [line for line in path.read_text().splitlines() if "proof_pack.export" in line]


def test_every_legacy_evidence_type_verifies_after_cutover_flood_and_purge(_isolated, monkeypatch):
    packs = _seed_pre_cutover_evidence(_isolated)
    # Cutover: the first evidence keyring is created by the next signature.
    build_signed_proof_pack(job_id="job-new", reconciliation={"passed": True}, workspace_id="ws-a")
    assert register.snapshot_taken()
    _flood_and_purge(monkeypatch)
    for kind, pack in packs.items():
        out = _verify(kind, json.loads(json.dumps(pack)))
        assert out["ok"] is True, (kind, out)
        assert out["signing_key"].get("legacy_status") == LEGACY_ON_RECORD, (kind, out)


def test_a_forged_login_secret_pack_never_verifies(_isolated, tmp_path):
    _seed_pre_cutover_evidence(_isolated)
    build_signed_proof_pack(job_id="job-new", reconciliation={"passed": True}, workspace_id="ws-a")
    forged = _legacy({"version": "1", "job_id": "job-forged", "assurance": {"migration_proven": False},
                      "gate8": {"passed": True}})
    out = verify_signed_proof_pack(forged)
    assert out["ok"] is False
    assert out["legacy_status"] == LEGACY_NOT_ON_RECORD
    assert any("cannot be told apart from a forgery" in e for e in out["errors"]), out
    # An export record filed after the cutover is not legacy evidence either.
    audit_log.append_audit_event(action="proof_pack.export", resource="job:job-forged",
                                 details={"content_sha256": forged["content_sha256"]})
    assert verify_signed_proof_pack(forged)["ok"] is False


def test_a_deployment_that_cut_over_before_the_register_snapshots_on_first_need(_isolated):
    packs = _seed_pre_cutover_evidence(_isolated)
    # A keyring made by the earlier build (no register then), one day later.
    later = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(esk._file_path(), {"keys": {}}) as data:
        data.setdefault("keys", {})["ws-old"] = {"workspace_id": "ws-old", "active_version": 1,
                                                 "versions": [], "created_at": later}
    assert not register.snapshot_taken()
    out = verify_signed_proof_pack(json.loads(json.dumps(packs["proof_pack"])))
    assert out["ok"] is True, out
    assert register.snapshot_taken()


def test_register_records_are_append_only(_isolated):
    assert register._insert_if_absent("abc", {"evidence_type": "proof_pack"}) is True
    assert register._insert_if_absent("abc", {"evidence_type": "forged"}) is False
    assert register.lookup("abc")["evidence_type"] == "proof_pack"
