"""Re-review findings 12, 13(a) and 13(c) (LOW-MEDIUM): what is sealed, shown and verified.

12. ``content_sha256`` (what approvals attest) left the explanation out, but
    the sealed pack carried it: wording nobody attested was signed. It is no
    longer sealed.
13a. After signing, GET showed the live re-read's verdict beside "Signed" with
    nothing saying it had moved: ``signoff.signed_verdict`` and
    ``signoff.drift_since_signing`` now say so.
13c. The parity pack was signed with the proof-pack key over the same subject
    (the job id); only ``body.kind`` told them apart, and verify answered ok as
    if it were the proof pack. The parity pack signs ``parity_report:<job id>``
    and verify names the kind it verified.
"""

from __future__ import annotations

import pytest

from services.mongodb_service import MemoryMongoDBService


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log

    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    monkeypatch.delenv("DATAFLOW_PARITY_SIGNOFF_AUTHORITY", raising=False)
    return svc


def _signed(store) -> str:
    from services.parity_explain import explain_job
    from services.parity_signoff import sign_off

    job_id = store.create_transfer_job({
        "name": "clean", "workspace_id": "w",
        "reconciliation": {"passed": True, "source_rows": 10, "target_rows": 10, "culprit_census_clean": True},
        "destination_summary": {"table": "t", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
    })
    store.update_job_status(job_id, "completed")
    explain_job(job_id, actor="ops@example.com")
    for role, user in (("engineering", "e@x"), ("data_owner", "d@x"), ("compliance", "c@x")):
        sign_off(job_id, role=role, actor=user, signer_role="admin")
    return job_id


def test_the_explanation_is_not_sealed(store):
    from services.parity_signoff import signed_pack

    pack = signed_pack(store.get_job(_signed(store)))
    assert pack["parity"]["explanation"] is None


def test_a_signed_report_flags_drift_since_signing(store):
    from services.parity_report import build_parity_report

    job_id = _signed(store)
    report = build_parity_report(store.get_job(job_id), reread=False)
    assert report["signoff"]["status"] == "signed"
    assert report["signoff"]["signed_verdict"] == "proven"
    assert report["signoff"]["drift_since_signing"] is False
    # The destination lost rows after signing: the evidence the live report reads has moved.
    store._jobs[job_id]["reconciliation"] = {**store._jobs[job_id]["reconciliation"], "target_rows": 7,
                                             "passed": False}
    moved = build_parity_report(store.get_job(job_id), reread=False)
    assert moved["signoff"]["status"] == "signed" and moved["signoff"]["drift_since_signing"] is True
    assert moved["signoff"]["signed_verdict"] == "proven" and moved["verdict"] != "proven"


def test_the_parity_pack_is_domain_separated_from_the_proof_pack(store):
    from services.parity_signoff import signed_pack
    from services.signed_proof_pack import sign_body, verify_signed_proof_pack

    job_id = _signed(store)
    pack = signed_pack(store.get_job(job_id))
    verified = verify_signed_proof_pack(pack)
    assert verified["ok"] is True and verified["kind"] == "parity_report"
    # The same body signed as a proof pack (subject = the job id) does not verify as a parity pack.
    body = {k: v for k, v in pack.items() if k not in ("content_sha256", "signature")}
    as_proof = sign_body(body, subject=job_id)
    assert verify_signed_proof_pack(as_proof)["ok"] is False
    # And a parity pack relabelled as a proof pack breaks its hash.
    relabelled = {**pack, "kind": "proof_pack"}
    assert verify_signed_proof_pack(relabelled)["ok"] is False
