"""Review finding 5: a sign-off must be bound to real authority, to content, and final.

(a) Any reader could sign any role (the route was job.read and the role came
    from the request body). Now the route needs job.manage, each sign-off role
    is bound to the signer's gate role in the job's workspace by an explicit,
    configurable mapping, and the approval records that role.
(b) Approvals carried no version or content: a decision after the first
    approval changed what was signed. Each approval now records the report
    version and the content digest; a change invalidates it.
(c) Nothing locked after signing: approve / dismiss / explain on a signed
    report now answer 409.
"""

from __future__ import annotations

import json
import uuid

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
    monkeypatch.delenv("DATAWRAP_PARITY_SIGNOFF_AUTHORITY", raising=False)
    return svc


def _job(svc, mismatches: int = 2) -> str:
    rows = [{"key": str(i), "source": "amount", "target": "amount", "source_value": f"{i}.1049",
             "target_value": f"{i}.10"} for i in range(1, mismatches + 1)]
    job_id = svc.create_transfer_job({
        "_id": "sign" + uuid.uuid4().hex[:10], "workspace_id": "ws", "status": "completed",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100, "culprit_census_clean": True,
                           "sample_compare": {"rows_compared": 100, "mismatches": rows}},
        "destination_summary": {"table": "payments", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    })
    svc.update_job_status(job_id, "completed")
    return job_id


def _open_for_signing(svc) -> str:
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report

    job_id = _job(svc, mismatches=1)
    report = build_parity_report(svc.get_job(job_id), reread=False, job_id=job_id)
    accept = next(f for f in report["fixes"] if f["action"] == "accept")
    approve_fix(job_id, accept["id"], actor="owner@example.com", note="known rounding")
    return job_id


def test_the_route_needs_job_manage():
    from services.rbac import Permission, _required_permission

    assert _required_permission("POST", "/api/v1/jobs/j1/parity-report/signoff") == Permission.JOB_MANAGE


@pytest.mark.parametrize("role, signer_role, allowed", [
    ("engineering", "viewer", False), ("engineering", "operator", True), ("engineering", "editor", True),
    ("data_owner", "operator", False), ("data_owner", "editor", True),
    ("compliance", "editor", False), ("compliance", "admin", True),
])
def test_each_role_is_bound_to_workspace_authority(store, role, signer_role, allowed):
    from services.parity_signoff import sign_off
    from services.parity_state import ParityForbidden

    job_id = _open_for_signing(store)
    if not allowed:
        with pytest.raises(ParityForbidden):
            sign_off(job_id, role=role, actor="someone@example.com", signer_role=signer_role)
        assert not store.get_job(job_id)["parity"]["approvals"]
        return
    report = sign_off(job_id, role=role, actor="someone@example.com", signer_role=signer_role)
    approval = report["signoff"]["approvals"][0]
    assert approval["signer_role"] == signer_role
    assert approval["authority"] == f"workspace {signer_role}"
    assert approval["content_sha256"] and approval["version"] >= 1


def test_the_mapping_is_configurable_and_fails_closed(store, monkeypatch):
    from services.parity_signoff import sign_off
    from services.parity_state import ParityForbidden

    job_id = _open_for_signing(store)
    monkeypatch.setenv("DATAFLOW_PARITY_SIGNOFF_AUTHORITY", json.dumps({
        "engineering": {"roles": ["editor"]}, "data_owner": {"roles": ["editor"], "members": ["dana@example.com"]},
        "compliance": {"roles": ["editor"]},
    }))
    with pytest.raises(ParityForbidden, match="named approvers"):
        sign_off(job_id, role="data_owner", actor="eve@example.com", signer_role="editor")
    sign_off(job_id, role="data_owner", actor="dana@example.com", signer_role="editor")
    sign_off(job_id, role="compliance", actor="carl@example.com", signer_role="editor")  # mapped to editor here
    monkeypatch.setenv("DATAFLOW_PARITY_SIGNOFF_AUTHORITY", "{not json")
    with pytest.raises(ParityForbidden, match="misconfigured"):
        sign_off(job_id, role="engineering", actor="ed@example.com", signer_role="admin")


def test_a_change_after_an_approval_invalidates_it(store):
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report
    from services.parity_signoff import sign_off

    job_id = _job(store, mismatches=1)
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    approve_fix(job_id, next(f["id"] for f in report["fixes"] if f["action"] == "accept"),
                actor="owner@example.com", note="known rounding")
    sign_off(job_id, role="engineering", actor="eng@example.com", signer_role="editor")
    # The evidence changes under the approval: a new, accepted difference changes the content.
    job = store.get_job(job_id)
    recon = dict(job["reconciliation"])
    recon["sample_compare"] = {**recon["sample_compare"], "mismatches": recon["sample_compare"]["mismatches"] + [
        {"key": "9", "source": "note", "target": "note", "source_value": "a ", "target_value": "a"}]}
    store.update_job_fields(job_id, {"reconciliation": recon})
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    approve_fix(job_id, next(f["id"] for f in report["fixes"] if f["action"] == "accept" and f["status"] == "proposed"),
                actor="owner@example.com", note="trailing space is fine")
    after = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    assert after["signoff"]["approvals"] == []
    assert [a["user"] for a in after["signoff"]["invalidated_approvals"]] == ["eng@example.com"]
    # The same person signs the changed content again; the stale approval is kept on the record.
    again = sign_off(job_id, role="engineering", actor="eng@example.com", signer_role="editor")
    assert [a["user"] for a in again["signoff"]["approvals"]] == ["eng@example.com"]


def test_a_signed_report_is_final(store):
    from services.parity_explain import explain_job
    from services.parity_fixes import ParityConflict, approve_fix, dismiss_fix
    from services.parity_report import build_parity_report
    from services.parity_signoff import sign_off

    job_id = _job(store, mismatches=2)
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    accepts = [f for f in report["fixes"] if f["action"] == "accept"]
    approve_fix(job_id, accepts[0]["id"], actor="owner@example.com", note="known rounding")
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    assert report["signoff"]["status"] == "open", report["signoff"]
    for role, who, gate in (("engineering", "e@x.io", "editor"), ("data_owner", "d@x.io", "editor"),
                            ("compliance", "c@x.io", "admin")):
        signed = sign_off(job_id, role=role, actor=who, signer_role=gate)
    assert signed["signoff"]["status"] == "signed"
    fix_id = accepts[0]["id"]
    with pytest.raises(ParityConflict, match="final"):
        approve_fix(job_id, fix_id, actor="owner@example.com", note="late")
    with pytest.raises(ParityConflict, match="final"):
        dismiss_fix(job_id, fix_id, actor="owner@example.com")
    with pytest.raises(ParityConflict, match="final"):
        explain_job(job_id, actor="owner@example.com")
