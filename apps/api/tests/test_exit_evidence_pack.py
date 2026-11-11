"""Signed Exit Evidence Pack: Parity Studio's multi-role sign-off, its own signature domain, the anchor.

Pins: only a proven rehearsal can be signed; each role is bound to authority
(services.parity_authority), anonymous signers are refused, one person signs
one role and a signed rehearsal is final; the pack is signed over
``exit_evidence:<system>:<rehearsal>`` (never verifies as another kind or
another rehearsal); the Merkle root is re-checked from the parts; the anchor
is read back (S3 Object Lock under moto) and a failed anchor is visible and
fails verification, while the stub reads "not anchored".
"""

from __future__ import annotations

import copy

import pytest

from tests.test_exit_rehearsal_e2e import _run, _system, world  # noqa: F401

SIGNERS = [("engineering", "eng@example.com", "operator"),
           ("data_owner", "owner@example.com", "editor"),
           ("compliance", "audit@example.com", "admin")]


@pytest.fixture()
def proven(world):  # noqa: F811
    out = _run(_system(tables=[{"name": "orders"}]))
    assert out["verdict"] == "proven", out["verdict_reason"]
    return out


def _sign_all(rehearsal_id, signers=SIGNERS):
    from services.exit_evidence import sign_off

    doc = None
    for role, user, gate_role in signers:
        doc = sign_off(rehearsal_id, role=role, actor=user, signer_role=gate_role, note=f"{role} ok")
    return doc


def test_three_roles_sign_a_proven_rehearsal_and_the_pack_verifies(proven, world):  # noqa: F811
    from services import exit_store
    from services.exit_evidence import signed_pack, verify_pack
    from services.exit_rehearsal import public
    from services.signed_proof_pack import verify_signed_proof_pack

    doc = _sign_all(proven["id"])
    view = public(doc)
    assert view["signoff"]["status"] == "signed" and view["signoff"]["signature"].startswith("hmac-sha256:")
    assert [a["role"] for a in view["signoff"]["approvals"]] == ["engineering", "data_owner", "compliance"]
    assert {a["authority"] for a in view["signoff"]["approvals"]} == {
        "workspace operator", "workspace editor", "workspace admin"}
    assert {s["stage"]: s["status"] for s in view["timeline"]}["sign"] == "done"
    assert view["evidence"]["anchor"]["status"] == "not_anchored"  # the stub never claims an anchor

    pack = signed_pack(exit_store.get(exit_store.REHEARSALS, proven["id"]))
    assert pack["kind"] == "exit_evidence" and pack["merkle_root"] == proven["seal"]["merkle_root"]
    assert pack["rehearsal"]["verdict"] == "proven" and pack["parts"] == proven["seal"]["parts"]
    checked = verify_pack(copy.deepcopy(pack))
    assert checked["ok"] is True and checked["signature_ok"] and checked["merkle_ok"]
    assert checked["anchor"]["status"] == "not_anchored"
    # The shared proof-pack verify route names the kind, and agrees.
    shared = verify_signed_proof_pack(copy.deepcopy(pack))
    assert shared["ok"] is True and shared["kind"] == "exit_evidence"


def test_the_signature_is_bound_to_its_own_domain_and_rehearsal(proven):
    from services import exit_store
    from services.exit_evidence import signed_pack, verify_pack
    from services.signed_proof_pack import verify_signed_proof_pack

    _sign_all(proven["id"])
    pack = signed_pack(exit_store.get(exit_store.REHEARSALS, proven["id"]))
    for field, value in (("rehearsal_id", "exr_other"), ("system_id", "exs_other"), ("kind", "parity_report")):
        forged = {**copy.deepcopy(pack), field: value}
        assert verify_signed_proof_pack(forged)["ok"] is False, field
    relabelled = verify_pack({**copy.deepcopy(pack), "kind": "parity_report"})
    assert relabelled["ok"] is False and "not an Exit Evidence Pack" in relabelled["errors"][0]
    tampered = copy.deepcopy(pack)
    tampered["parts"][0]["sha256"] = "0" * 64
    result = verify_pack(tampered)
    assert result["ok"] is False and result["merkle_ok"] is False and result["signature_ok"] is False


def test_signoff_rules_are_parity_studios(proven):
    from services.exit_evidence import sign_off
    from services.exit_inventory import ExitConflict, ExitForbidden, ExitInvalid

    rid = proven["id"]
    with pytest.raises(ExitInvalid):
        sign_off(rid, role="auditor", actor="x@example.com", signer_role="admin")
    with pytest.raises(ExitForbidden, match="authentication"):
        sign_off(rid, role="engineering", actor="anonymous", signer_role="admin")
    with pytest.raises(ExitForbidden, match="needs a workspace admin"):
        sign_off(rid, role="compliance", actor="viewer@example.com", signer_role="viewer")
    sign_off(rid, role="engineering", actor="eng@example.com", signer_role="operator")
    with pytest.raises(ExitConflict, match="already signed"):
        sign_off(rid, role="engineering", actor="other@example.com", signer_role="admin")
    with pytest.raises(ExitConflict, match="One person signs one role"):
        sign_off(rid, role="data_owner", actor="eng@example.com", signer_role="editor")
    with pytest.raises(ExitConflict):
        sign_off(rid, role="data_owner", actor="owner@example.com", signer_role="editor", expected_version=1)
    sign_off(rid, role="data_owner", actor="owner@example.com", signer_role="editor")
    sign_off(rid, role="compliance", actor="audit@example.com", signer_role="admin")
    with pytest.raises(ExitConflict, match="final"):
        sign_off(rid, role="compliance", actor="later@example.com", signer_role="admin")


def test_refusals_and_signatures_are_audited(proven, monkeypatch):
    from services import exit_evidence
    from services.exit_inventory import ExitForbidden

    seen: list[tuple[str, str]] = []
    import services.exit_inventory as inventory

    monkeypatch.setattr(inventory, "audit", lambda action, **kw: seen.append((action, kw.get("outcome", ""))))
    with pytest.raises(ExitForbidden):
        exit_evidence.sign_off(proven["id"], role="compliance", actor="v@example.com", signer_role="viewer")
    _sign_all(proven["id"])
    assert seen[0] == ("exit.rehearsal.signoff_refused", "denied")
    assert [a for a, _ in seen].count("exit.rehearsal.signed_off") == 3
    assert seen[-1] == ("exit.rehearsal.signed", "succeeded")


def test_only_a_proven_rehearsal_can_be_signed(world, monkeypatch):  # noqa: F811
    from services import exit_completeness
    from services.exit_evidence import sign_off
    from services.exit_inventory import ExitConflict

    monkeypatch.setattr(exit_completeness, "source_columns_and_rows",
                        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("down")))
    out = _run(_system(tables=[{"name": "orders"}]))
    assert out["verdict"] == "not_proven" and out["signoff"]["status"] == "blocked"
    with pytest.raises(ExitConflict, match="Only a proven rehearsal"):
        sign_off(out["id"], role="engineering", actor="eng@example.com", signer_role="admin")


@pytest.fixture()
def lock_bucket(monkeypatch):
    import boto3
    from moto import mock_aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    for name in ("DATAFLOW_S3_ENDPOINT", "DATAWRAP_S3_ENDPOINT", "AWS_ENDPOINT_URL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", "s3")
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_S3_BUCKET", "exit-anchors")
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="exit-anchors", ObjectLockEnabledForBucket=True)
        yield s3


def test_the_signed_pack_is_anchored_in_object_lock_and_verified_on_read(proven, lock_bucket, monkeypatch):
    from services import exit_store
    from services.exit_evidence import signed_pack, verify_pack

    doc = _sign_all(proven["id"])
    anchor = doc["signed"]["anchor"]
    assert anchor["status"] == "anchored" and anchor["retention"]["mode"] == "COMPLIANCE"
    assert anchor["subject"] == f"exit_evidence:{proven['system_id']}:{proven['id']}"
    pack = signed_pack(exit_store.get(exit_store.REHEARSALS, proven["id"]))
    result = verify_pack(pack)
    assert result["ok"] is True and result["anchor"]["status"] == "anchored"
    # The anchor can no longer be confirmed on read (e.g. the store is unreachable): verification fails.
    from services import audit_anchor_s3

    monkeypatch.setattr(audit_anchor_s3, "verify_location", lambda *_a, **_k: {
        "status": "failed", "retention": None, "reason": "the anchor object could not be read back"})
    failed = verify_pack(pack)
    assert failed["ok"] is False and failed["anchor"]["status"] == "failed"
    assert any("anchor could not be confirmed" in e for e in failed["errors"])


def test_an_anchor_that_cannot_be_written_is_visible(proven, monkeypatch):
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", "s3")
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_S3_BUCKET", "")
    doc = _sign_all(proven["id"])
    assert doc["signed"]["anchor"]["status"] == "failed"
    from services.exit_rehearsal import public

    assert public(doc)["evidence"]["anchor"]["anchored"] is False


def test_markdown_export_states_evidence_not_compliance(proven):
    from services import exit_store
    from services.exit_evidence import render_markdown

    _sign_all(proven["id"])
    text = render_markdown(exit_store.get(exit_store.REHEARSALS, proven["id"]))
    assert "Verdict: **proven**" in text and "| orders | proven |" in text
    assert "not a statement of regulatory compliance" in text
    lowered = text.lower()
    for claim in ("one-click", "zero data loss", "zero-knowledge", "autonomous", "compliant with"):
        assert claim not in lowered
