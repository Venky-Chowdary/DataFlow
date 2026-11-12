"""Audit findings 2 + 7 — every consumer of a stored Gate-8 digest names its version.

A fingerprint rule change makes digests of the two versions incomparable. The
consumers that keep a digest (or a verdict built on one) beyond the run:

* the signed proof pack — a pack signed before the change still verifies as
  signed, and every pack states the rules its two checksums share;
* Parity Studio — a report states the rules it compared under, the rules that
  proved its resolved keys, and never reports "drift since signing" from a
  rule change;
* the append continuity baseline — tests/test_audit02_continuity_fingerprint_version.py.

Digests that do not use the Gate-8 fingerprint are untouched by it and are
pinned here so a later change cannot couple them silently.
"""

from __future__ import annotations

import json

import pytest

from services.fingerprint_rules import FINGERPRINT_VERSION
from services.mongodb_service import MemoryMongoDBService
from services.signed_proof_pack import (
    build_signed_proof_pack,
    sign_body,
    verify_signed_proof_pack,
)

_OLD_RUN = {
    "passed": True, "phase": "post_write", "coverage": "full_checksum",
    "source_checksum": "abc", "target_checksum": "abc",
    "source_checksum_provenance": "independent_source_reread",
    "identity_hash_aligned": True,
}


@pytest.fixture(autouse=True)
def _secret(monkeypatch):
    monkeypatch.setattr("services.signed_proof_pack._platform_secret", lambda: b"audit02-secret")


def _pack(report: dict) -> dict:
    return json.loads(json.dumps(build_signed_proof_pack(
        job_id="job-fp", reconciliation=report, mapping_hash="m",
        connector_versions={"source": "postgresql-16.4", "destination": "sqlite-3.45"},
    )))


def test_a_pack_signed_before_the_version_stamp_still_verifies_as_signed():
    """An old pack has no ``fingerprint_version`` anywhere; its signature is over what it holds."""
    new = _pack(_OLD_RUN)
    old_body = {k: v for k, v in new.items() if k not in ("content_sha256", "signature")}
    old_body["hashes"] = {k: v for k, v in old_body["hashes"].items() if k != "fingerprint_version"}
    old_pack = json.loads(json.dumps(sign_body(old_body, subject="job-fp")))
    assert "fingerprint_version" not in old_pack["hashes"]
    assert "fingerprint_version" not in old_pack["gate8"]

    verified = verify_signed_proof_pack(old_pack)

    assert verified["ok"] is True and verified["errors"] == []


def test_a_pack_exported_now_for_a_run_before_the_stamp_says_version_1():
    pack = _pack(_OLD_RUN)
    assert pack["hashes"]["fingerprint_version"] == 1
    assert pack["assurance"]["checksum_match"] is True
    assert verify_signed_proof_pack(pack)["ok"] is True


def test_a_new_run_carries_the_current_version_into_its_pack():
    from services.reconciliation import stamp_post_write_phase

    report = stamp_post_write_phase(dict(_OLD_RUN))
    assert report["fingerprint_version"] == FINGERPRINT_VERSION
    pack = _pack(report)
    assert pack["hashes"]["fingerprint_version"] == FINGERPRINT_VERSION
    assert pack["gate8"]["fingerprint_version"] == FINGERPRINT_VERSION
    assert verify_signed_proof_pack(pack)["ok"] is True


def test_stamping_never_relabels_a_report_that_already_names_its_version():
    from services.reconciliation import stamp_post_write_phase

    assert stamp_post_write_phase({**_OLD_RUN, "fingerprint_version": 1})["fingerprint_version"] == 1


# ── Parity Studio ───────────────────────────────────────────────────────────


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


def test_a_parity_pack_names_the_rules_it_was_signed_under(store):
    from services.parity_report import build_parity_report
    from services.parity_signoff import signed_pack

    job_id = _signed(store)
    pack = signed_pack(store.get_job(job_id))
    assert pack["fingerprint_version"] == FINGERPRINT_VERSION
    # The API report keeps its shape (the web contract is typed against it).
    report = build_parity_report(store.get_job(job_id), reread=False)
    assert "fingerprint" not in report["evidence"]


def test_resolved_keys_record_the_rules_that_proved_them():
    """parity_apply stamps the version beside the keys a fix re-check resolved."""
    import inspect

    from services import parity_apply

    source = inspect.getsource(parity_apply)
    assert 'state["resolved_fingerprint_version"] = FINGERPRINT_VERSION' in source


def test_a_parity_pack_signed_under_older_rules_verifies_and_reports_no_false_drift(store):
    from services.parity_report import build_parity_report

    job_id = _signed(store)
    # Signed now: the sealed report names the current rules, drift is decided normally.
    fresh = build_parity_report(store.get_job(job_id), reread=False)
    assert fresh["signoff"]["drift_since_signing"] is False
    assert "drift_not_comparable" not in fresh["signoff"]

    # What a build before the stamp sealed: a pack that names no rules version.
    job = store._jobs[job_id]
    job["parity"]["signed"]["pack"].pop("fingerprint_version", None)
    # A rule change re-reads the stored cells differently: the live report moved.
    job["reconciliation"] = {**job["reconciliation"], "target_rows": 7, "passed": False}

    moved = build_parity_report(store.get_job(job_id), reread=False)

    assert moved["signoff"]["status"] == "signed"
    assert moved["signoff"]["signed_verdict"] == "proven"
    assert moved["signoff"]["drift_since_signing"] is None
    assert moved["signoff"]["drift_not_comparable"] == {
        "reason": "fingerprint_version", "signed_under": 1, "current": FINGERPRINT_VERSION,
    }


def test_a_sealed_parity_pack_still_verifies_as_signed(store):
    from services.parity_signoff import signed_pack

    pack = signed_pack(store.get_job(_signed(store)))
    verified = verify_signed_proof_pack(pack)
    assert verified["ok"] is True and verified["kind"] == "parity_report"


# ── digests that never used the Gate-8 fingerprint ──────────────────────────


def test_copy_range_proof_and_continuity_engine_digest_do_not_use_the_gate8_fingerprint():
    import inspect

    from services import continuity_digest, copy_range_proof, scd2_engine

    for module in (copy_range_proof, continuity_digest):
        source = inspect.getsource(module)
        assert "normalize_cell" not in source and "fingerprint_for_reconcile" not in source
    # The SCD2 stored row hash has its own canonical form too: a rule change
    # here must never read every current row as changed.
    assert "conflict_key_wire" in inspect.getsource(scd2_engine._row_hash)
