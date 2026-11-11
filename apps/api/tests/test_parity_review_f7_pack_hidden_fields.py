"""Review finding 7: internal fix fields never reach the signed pack or an export.

``public_report`` stripped ``_`` fields from tables and differences but not from
fixes, so ``_keys`` (up to 1000 key values), ``_key_column`` and ``_new_type``
were serialized into the signed pack and the JSON export.
"""

from __future__ import annotations

import uuid

import pytest

from services.mongodb_service import MemoryMongoDBService


def _walk_keys(value):
    if isinstance(value, dict):
        for k, v in value.items():
            yield k
            yield from _walk_keys(v)
    elif isinstance(value, list):
        for v in value:
            yield from _walk_keys(v)


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log

    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    return svc


def _job(svc) -> str:
    job_id = svc.create_transfer_job({
        "_id": "pack" + uuid.uuid4().hex[:10], "workspace_id": "ws", "status": "completed",
        "reconciliation": {"passed": False, "source_rows": 100, "target_rows": 100, "culprit_census_clean": True,
                           "sample_compare": {"rows_compared": 100, "mismatches": [
                               {"key": "7", "source": "amount", "target": "amount", "source_value": "7.1249",
                                "target_value": "7.12"}]}},
        "destination_summary": {"table": "payments", "primary_key_columns": ["id"],
                                "sync_mode": "full_refresh_overwrite"},
    })
    svc.update_job_status(job_id, "completed")
    return job_id


def test_public_report_strips_internal_fix_fields(store):
    from services.parity_report import build_parity_report, public_report

    job_id = _job(store)
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    assert any(k.startswith("_") for f in report["fixes"] for k in f), "the proposals carry internal fields"
    assert not [k for k in _walk_keys(public_report(report)) if str(k).startswith("_")]


def test_the_signed_pack_and_exports_carry_no_internal_fields(store):
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report
    from services.parity_signoff import export_markdown, sign_off, signed_pack

    job_id = _job(store)
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    # A write fix that failed keeps its internal re-check fields on the stored record.
    resync = next(f for f in report["fixes"] if f["action"] == "resync_keys")
    store.update_job_fields(job_id, {"parity.fixes": {resync["id"]: {
        **{k: v for k, v in resync.items() if not k.startswith("_")}, "_keys": resync["_keys"],
        "_table": resync["_table"], "status": "failed", "result": "destination locked"}}})
    report = build_parity_report(store.get_job(job_id), reread=False, job_id=job_id)
    accept = next(f for f in report["fixes"] if f["action"] == "accept" and f["status"] == "proposed")
    approve_fix(job_id, accept["id"], actor="owner@example.com", note="known rounding")
    for role, who, gate in (("engineering", "e@x.io", "editor"), ("data_owner", "d@x.io", "editor"),
                            ("compliance", "c@x.io", "admin")):
        sign_off(job_id, role=role, actor=who, signer_role=gate)
    pack = signed_pack(store.get_job(job_id))
    assert pack, "the report was signed"
    hidden = [k for k in _walk_keys(pack) if str(k).startswith("_")]
    assert hidden == [], hidden
    assert "_keys" not in export_markdown(store.get_job(job_id), job_id)
