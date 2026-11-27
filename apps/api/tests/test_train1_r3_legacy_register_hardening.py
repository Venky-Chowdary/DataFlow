"""Review 3 items 4 and 5: the legacy evidence register records only genuine
pre-cutover evidence, survives an unreadable store without rescanning it on
every verify, and keeps each pack to its own workspace.

Item 4: parity and exit packs were recorded whatever their signing time and
on their stored ``content_sha256`` alone; one unreadable source left the
snapshot marker unwritten, so every legacy verify (any ``job.read`` caller)
rescanned the whole audit store.

Item 5: the proof-pack and certificate export records carried no workspace,
so any workspace reader could verify any legacy pack recorded from them.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from services import audit_log, exit_store
from services import legacy_evidence_register as register
from services.signed_proof_pack import EXIT_PACK_KIND, PARITY_PACK_KIND, verify_signed_proof_pack
from tests.test_train1_r2_legacy_evidence_register import _isolated, _legacy  # noqa: F401


def _cutover() -> datetime:
    return datetime.now(timezone.utc)


def test_parity_and_exit_packs_signed_after_the_cutover_are_not_recorded(_isolated):
    later = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    parity = _legacy({"kind": PARITY_PACK_KIND, "job_id": "job-late", "verdict": "proven"})
    exit_pack = _legacy({"kind": EXIT_PACK_KIND, "system_id": "s", "rehearsal_id": "late"})
    _isolated._jobs["job-late"] = {"job_id": "job-late", "workspace_id": "",
                                   "parity": {"signed": {"pack": parity, "signed_at": later}}}
    exit_store.insert(exit_store.REHEARSALS, {"id": "late", "workspace_id": "",
                                              "signed": {"pack": exit_pack, "signed_at": later}})
    register.snapshot(_cutover())
    assert register.lookup(parity["content_sha256"]) is None
    assert register.lookup(exit_pack["content_sha256"]) is None


def test_a_stored_pack_is_recorded_only_when_its_digest_and_signature_check_out(_isolated):
    genuine = _legacy({"kind": PARITY_PACK_KIND, "job_id": "job-ok", "verdict": "proven"})
    edited = {**_legacy({"kind": PARITY_PACK_KIND, "job_id": "job-ed", "verdict": "proven"}),
              "verdict": "edited in the store"}
    resigned = _legacy({"kind": PARITY_PACK_KIND, "job_id": "job-sig", "verdict": "proven"})
    resigned["signature"] = {**resigned["signature"], "value": "0" * 64}
    for job_id, pack in (("job-ok", genuine), ("job-ed", edited), ("job-sig", resigned)):
        _isolated._jobs[job_id] = {"job_id": job_id, "workspace_id": "",
                                   "parity": {"signed": {"pack": pack, "signed_at": "2026-01-01"}}}
    register.snapshot(_cutover())
    assert register.lookup(genuine["content_sha256"]) is not None
    assert register.lookup(edited["content_sha256"]) is None
    assert register.lookup(resigned["content_sha256"]) is None


def test_an_unreadable_source_is_recorded_and_retried_bounded(_isolated, monkeypatch):
    calls = {"audit": 0, "exit": 0}
    real_audit = register._audit_records

    def counted_audit(cutover):
        calls["audit"] += 1
        return real_audit(cutover)

    def broken_exit(cutover):
        calls["exit"] += 1
        raise OSError("exit store down")

    monkeypatch.setattr(register, "_audit_records", counted_audit)
    monkeypatch.setattr(register, "_exit_records", broken_exit)
    forged = _legacy({"version": "1", "job_id": "job-x", "assurance": {}, "gate8": {"passed": True}})
    for _ in range(5):
        assert verify_signed_proof_pack(forged)["ok"] is False
    # The marker was written on the first verify; nothing was rescanned after it.
    assert register.snapshot_taken()
    assert calls == {"audit": 1, "exit": 1}
    # Past the retry interval only the unread source is read again, and at most
    # _MAX_RETRIES times.
    monkeypatch.setattr(register, "_RETRY_SECONDS", 0)
    for _ in range(10):
        verify_signed_proof_pack(forged)
    assert calls["audit"] == 1
    assert calls["exit"] == 1 + register._MAX_RETRIES


def test_a_pack_recorded_from_an_export_without_a_workspace_stays_in_its_jobs_workspace(_isolated):
    pack = _legacy({"version": "1", "job_id": "job-a", "assurance": {}, "gate8": {"passed": True}})
    _isolated._jobs["job-a"] = {"_id": "job-a", "job_id": "job-a", "workspace_id": "ws-a"}
    # An export record filed by the earlier build: no workspace on it.
    audit_log.append_audit_event(action="proof_pack.export", resource="job:job-a",
                                 details={"content_sha256": pack["content_sha256"]})
    out = verify_signed_proof_pack(dict(pack), may_use_workspace=lambda ws: ws == "ws-b")
    assert out["ok"] is False
    assert any("another workspace" in e for e in out["errors"]), out
    assert verify_signed_proof_pack(dict(pack), may_use_workspace=lambda ws: ws == "ws-a")["ok"] is True


def test_the_export_routes_file_the_jobs_workspace(_isolated, monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    import src.routers.transfer_router as router
    from src.main import app

    job = {"_id": "job-w", "id": "job-w", "job_id": "job-w", "workspace_id": "ws-a", "status": "completed"}
    _isolated._jobs["job-w"] = job
    monkeypatch.setattr(router, "_can_access_job", lambda request, j: True)
    monkeypatch.setattr(router, "export_proof_pack_for_job",
                        lambda j, actor=None: {"content_sha256": "c" * 64, "version": "1"}, raising=False)
    monkeypatch.setattr("services.signed_proof_pack.export_proof_pack_for_job",
                        lambda j, actor=None: {"content_sha256": "c" * 64, "version": "1"})
    monkeypatch.setattr("services.migration_certificate.build_migration_certificate",
                        lambda j, actor=None: {"content_sha256": "d" * 64, "verdict": {}})
    client = TestClient(app)
    client.get("/api/v1/transfer/job-w/proof-pack")
    client.get("/api/v1/transfer/job-w/certificate")
    events = [e for e in register._audit_events({"proof_pack.export", "migration_certificate.export"})]
    assert {e["action"] for e in events} == {"proof_pack.export", "migration_certificate.export"}, events
    assert all(e.get("workspace_id") == "ws-a" for e in events), events
