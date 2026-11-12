"""Dual signing of the three evidence packs: the HMAC stays, a public-key signature and a ledger receipt are added.

Runs the real signing paths (the proof pack export, Parity Studio's sign-off,
the Exit Evidence sign-off). Pins: each exported pack carries ``ledger``, its
HMAC and content hash are unchanged by it, and every cryptographic check
passes; a pack without a receipt verifies exactly as before and is reported
"HMAC only"; a failed issuance never blocks signing; ``verified`` is never
reported when a check was skipped; a tampered pack or receipt fails; a
withdrawn pack reads "issued, later withdrawn" and its entry stays; no message
claims compliance.
"""

from __future__ import annotations

import copy
import itertools
import json
import re

import pytest

from tests.test_exit_evidence_pack import _sign_all, proven  # noqa: F401
from tests.test_exit_rehearsal_e2e import world  # noqa: F401
from tests.test_ledger_log import ledger  # noqa: F401
from tests.test_parity_review2_f12_f13_seal_and_verify import _signed, store  # noqa: F401
from tools import datawrap_verify as verifier

CRYPTO_CHECKS = ("content_digest", "statement", "signatures", "inclusion", "checkpoint_signature")


def _statuses(evidence: dict) -> dict[str, str]:
    return {c["id"]: c["status"] for c in evidence["checks"]}


def _assert_sound(pack: dict, kind: str) -> dict:
    """The pack verifies by HMAC as before, and every check the service can make passed."""
    from services.signed_proof_pack import pack_subject, verify_signed_proof_pack

    receipt = pack["ledger"]
    assert receipt["type"] == "datawrap.evidence.receipt.v1"
    statement = receipt["statement"]
    assert statement["kind"] == kind and statement["subject"] == pack_subject(pack) == verifier.pack_subject(pack)
    assert statement["content_sha256"] == pack["content_sha256"] == verifier.pack_content_sha256(pack)
    assert pack["signature"]["alg"] == "HMAC-SHA256"  # the HMAC is still there
    # Inside the hash, the pack says which signatures it was issued with; the statement binds the same.
    assert pack["ledger_commitment"] == {"type": "datawrap.evidence.commitment.v1",
                                         "signature_algs": sorted(statement["signer_algs"])}
    assert len(statement["signer_algs"]) == len(statement["signer_key_ids"]) == len(receipt["signatures"])
    checked = verify_signed_proof_pack(copy.deepcopy(pack))
    assert checked["ok"] is True and checked["errors"] == [] and checked["kind"] == kind
    evidence = checked["evidence"]
    statuses = _statuses(evidence)
    for cid in (*CRYPTO_CHECKS, "consistency", "withdrawal"):
        assert statuses[cid] == "passed", (cid, evidence["checks"])
    # The stub anchor is not a write-once store: "not checked", so the verdict is not "verified".
    assert statuses["anchor"] == "not_checked" and evidence["verdict"] == "incomplete"
    assert evidence["issuance"] == "issued" and evidence["mode"] == "online"
    assert evidence["signature_scheme"] == receipt["signature_scheme"]
    assert evidence["signature_scheme"] == ("hybrid" if verifier.ml_dsa_available() else "classical")
    assert [s["status"] for s in evidence["signatures"]] == ["passed"] * len(statement["signer_key_ids"])
    return evidence


def _job(job_id: str = "job-ledger-1", workspace_id: str = "w1") -> dict:
    return {
        "_id": job_id, "workspace_id": workspace_id, "status": "completed",
        "source": {"format": "postgresql"}, "destination": {"format": "postgresql"},
        "reconciliation": {"passed": True, "source_rows": 5, "target_rows": 5, "phase": "post_write",
                           "source_checksum": "abc", "target_checksum": "abc", "checksum_match": True},
        "destination_summary": {"table": "t"},
    }


# ----------------------------------------------------------------- the three paths


def test_the_proof_pack_export_is_dual_signed_and_survives_the_wire(ledger):  # noqa: F811
    from services.signed_proof_pack import export_proof_pack_for_job, verify_signed_proof_pack

    pack = export_proof_pack_for_job(_job(), actor="ops@example.com")
    _assert_sound(pack, "proof_pack")
    entry = ledger.find_entry(pack["content_sha256"], "proof_pack")
    assert entry["workspace_id"] == "w1" and entry["issued_by"] == "ops@example.com"
    assert pack["ledger"]["leaf_index"] == entry["index"]
    # Across the wire (JSON out, JSON back) the pack still verifies, receipt included.
    wired = json.loads(json.dumps(pack))
    assert verify_signed_proof_pack(wired)["ok"] is True
    assert _statuses(verify_signed_proof_pack(wired)["evidence"])["signatures"] == "passed"
    # The receipt is outside the content hash: with or without it the HMAC answer is the same.
    bare = {k: v for k, v in pack.items() if k != "ledger"}
    assert verify_signed_proof_pack(bare)["ok"] is True
    assert verify_signed_proof_pack(bare)["content_sha256"] == pack["content_sha256"]


def test_the_parity_sign_off_issues_once_and_the_export_carries_the_receipt(ledger, store):  # noqa: F811
    from services.parity_signoff import exported_pack, signed_pack

    job_id = _signed(store)
    job = store.get_job(job_id)
    stored = signed_pack(job)
    assert "ledger" not in stored  # the stored pack is the signed body; the receipt is attached on export
    # The stored copy says it has signatures, so verifying it looks the receipt up: never "HMAC only".
    from services.signed_proof_pack import verify_signed_proof_pack

    assert verify_signed_proof_pack(stored)["ok"] is True
    assert verify_signed_proof_pack(stored)["evidence"]["verdict"] == "incomplete"
    entry = ledger.find_entry(stored["content_sha256"], "parity_report")
    assert entry and entry["subject"] == f"parity_report:{job_id}" and entry["workspace_id"] == "w"
    size = ledger.tree_size()
    pack = exported_pack(job, actor="viewer@example.com")
    _assert_sound(pack, "parity_report")
    assert exported_pack(job, actor="viewer@example.com")["ledger"]["leaf_index"] == entry["index"]
    assert ledger.tree_size() == size  # exporting again issues nothing new
    assert {k: v for k, v in pack.items() if k != "ledger"} == stored


def test_the_exit_sign_off_issues_and_the_export_carries_the_receipt(ledger, proven):  # noqa: F811
    from services import exit_store
    from services.exit_evidence import exported_pack, signed_pack, verify_pack

    _sign_all(proven["id"])
    doc = exit_store.get(exit_store.REHEARSALS, proven["id"])
    stored = signed_pack(doc)
    entry = ledger.find_entry(stored["content_sha256"], "exit_evidence")
    assert entry and entry["subject"] == f"exit_evidence:{doc['system_id']}:{proven['id']}"
    assert entry["workspace_id"] == "ws1"
    pack = exported_pack(doc, actor="audit@example.com")
    _assert_sound(pack, "exit_evidence")
    checked = verify_pack(copy.deepcopy(pack))
    assert checked["ok"] is True and checked["signature_ok"] and checked["merkle_ok"]
    assert checked["evidence"]["verdict"] == "incomplete"
    assert _statuses(checked["evidence"])["inclusion"] == "passed"


# ------------------------------------------------------------- old packs, failures


def test_a_pack_without_a_receipt_verifies_as_before_and_reads_hmac_only(ledger):  # noqa: F811
    from services.signed_proof_pack import build_signed_parity_pack, sign_body, verify_signed_proof_pack

    old = sign_body({"version": 1, "job_id": "job-old", "assurance": {"claim_level": "sample"}}, subject="job-old")
    checked = verify_signed_proof_pack(old)
    assert checked["ok"] is True and checked["errors"] == [] and checked["kind"] == "proof_pack"
    evidence = checked["evidence"]
    assert evidence["verdict"] == "hmac_only" and evidence["signature_scheme"] == "hmac_only"
    assert set(_statuses(evidence).values()) == {"not_checked"} and len(evidence["checks"]) == 8
    assert "HMAC only" in evidence["summary"] and evidence["issuance"] == "unknown"
    parity = build_signed_parity_pack(job_id="job-old", report={"verdict": "proven"}, actor="a")
    assert verify_signed_proof_pack(parity)["evidence"]["verdict"] == "hmac_only"
    assert ledger.tree_size() == 0  # verifying never issues


def test_issuance_never_blocks_signing_and_the_failure_is_audited(ledger, monkeypatch):  # noqa: F811
    from services import ledger_keys, ledger_packs
    from services.signed_proof_pack import export_proof_pack_for_job, verify_signed_proof_pack

    events: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(ledger_packs, "_audit", lambda action, *, workspace_id, actor, details, outcome="succeeded":
                        events.append((action, outcome, details)))
    # Every key retired: nothing is created silently, the pack is exported HMAC only.
    for view in ledger_keys.create("ws:w1", actor="admin"):
        ledger_keys.retire("ws:w1", view["key_id"], actor="admin")
    pack = export_proof_pack_for_job(_job("job-nokey"), actor="ops")
    assert "ledger" not in pack and verify_signed_proof_pack(pack)["ok"] is True
    assert verify_signed_proof_pack(pack)["evidence"]["verdict"] == "hmac_only"
    assert events[-1][0:2] == ("evidence.issue.failed", "failed")
    assert events[-1][2]["reason"] == "the workspace has no active signing key"
    # The store is down: the same.
    monkeypatch.setattr(ledger_packs.ledger_log, "find_entry",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("disk on fire: secret-path")))
    pack = export_proof_pack_for_job(_job("job-down", "w2"), actor="ops")
    assert "ledger" not in pack and verify_signed_proof_pack(pack)["ok"] is True
    assert events[-1][0] == "evidence.issue.failed" and events[-1][2]["reason"] == "OSError"


def test_a_statement_is_never_issued_for_a_pack_this_deployment_did_not_sign(ledger):  # noqa: F811
    from services.ledger_packs import issue, with_ledger
    from services.signed_proof_pack import sign_body

    genuine = sign_body({"version": 1, "job_id": "job-x"}, subject="job-x")
    forged = {**genuine, "job_id": "job-y"}  # the HMAC is for another subject
    assert issue(forged, workspace_id="w1", actor="a") is None
    altered = {**genuine, "version": 2}
    assert issue(altered, workspace_id="w1", actor="a") is None
    assert with_ledger(altered, workspace_id="w1", actor="a") == altered
    assert ledger.tree_size() == 0  # refused before a key was even created
    issued = issue(genuine, workspace_id="w1", actor="a")
    assert issued["kind"] == "proof_pack" and issued["content_sha256"] == genuine["content_sha256"]


# ----------------------------------------------------------------------- tamper


def test_a_tampered_pack_or_receipt_fails_and_turns_ok_false(ledger):  # noqa: F811
    from services.signed_proof_pack import export_proof_pack_for_job, verify_signed_proof_pack

    pack = export_proof_pack_for_job(_job("job-tamper"), actor="ops")
    other = export_proof_pack_for_job(_job("job-other"), actor="ops")

    def verdict(mutate) -> tuple[bool, dict[str, str], str]:
        forged = copy.deepcopy(pack)
        mutate(forged)
        checked = verify_signed_proof_pack(forged)
        return checked["ok"], _statuses(checked["evidence"]), checked["evidence"]["verdict"]

    ok, statuses, v = verdict(lambda p: p.update(actor="mallory"))
    assert (ok, statuses["content_digest"], v) == (False, "failed", "failed")
    ok, statuses, v = verdict(lambda p: p["ledger"]["statement"].update(subject="job-elsewhere"))
    assert ok is False and statuses["statement"] == "failed" and statuses["signatures"] == "failed"
    ok, statuses, v = verdict(lambda p: p["ledger"]["signatures"].pop())
    assert ok is False and statuses["signatures"] == "failed"
    ok, statuses, v = verdict(lambda p: p["ledger"]["checkpoint"].update(timestamp="2001-01-01T00:00:00+00:00"))
    assert ok is False and statuses["checkpoint_signature"] == "failed"
    # Another pack's (valid) receipt does not vouch for this pack.
    ok, statuses, v = verdict(lambda p: p.update(ledger=copy.deepcopy(other["ledger"])))
    assert ok is False and statuses["content_digest"] == "failed" and statuses["statement"] == "failed"
    # A receipt whose proof was cut for another size is not in the live log at that root.
    ok, statuses, v = verdict(lambda p: p["ledger"]["checkpoint"].update(tree_size=99))
    assert ok is False and statuses["consistency"] == "failed"
    errors = verify_signed_proof_pack({**copy.deepcopy(pack), "ledger": {"statement": {}}})["errors"]
    assert any("Evidence Ledger receipt does not verify" in e for e in errors)


# -------------------------------------------------------------------- downgrade


def test_a_pack_stripped_of_its_receipt_fails_and_is_never_reported_hmac_only(ledger):  # noqa: F811
    from services import ledger_verify
    from services.ledger_packs import commitment
    from services.signed_proof_pack import build_signed_proof_pack, export_proof_pack_for_job, verify_signed_proof_pack

    pack = export_proof_pack_for_job(_job("job-strip"), actor="ops")
    bare = {k: v for k, v in pack.items() if k != "ledger"}
    promised = " and ".join(pack["ledger_commitment"]["signature_algs"])
    # Offline, and for a caller without a session: no receipt is a failure, with the reason.
    for evidence in (verifier.verify_evidence(bare, None, _trusted(pack["ledger"])),
                     ledger_verify.verify_online(bare, public=True)):
        statuses = _statuses(evidence)
        assert evidence["verdict"] == "failed" and statuses["signatures"] == "failed"
        assert evidence["signature_scheme"] == pack["ledger"]["signature_scheme"] != "hmac_only"
        detail = next(c["detail"] for c in evidence["checks"] if c["id"] == "signatures")
        assert detail == (f"the pack says it was issued with public-key signatures ({promised}) and carries "
                          "none: its receipt was removed")
    # The service, asked about its own pack, finds the receipt in the ledger instead.
    looked_up = verify_signed_proof_pack(bare)
    assert looked_up["ok"] is True and looked_up["evidence"]["verdict"] == "incomplete"
    assert _statuses(looked_up["evidence"])["signatures"] == "passed"
    # A pack that says it has signatures and is not in the ledger at all does not verify.
    never_issued = build_signed_proof_pack(job_id="job-ghost", reconciliation=None, mapping_proof=None,
                                           ledger_commitment=commitment("w1"))
    checked = verify_signed_proof_pack(never_issued)
    assert checked["ok"] is False and checked["evidence"]["verdict"] == "failed"
    assert any("Evidence Ledger receipt does not verify (signatures)" in e for e in checked["errors"])
    # The statement cannot be edited to agree with fewer signatures, and the commitment cannot be
    # taken out of the pack: both are under a hash that is signed.
    uncommitted = {k: v for k, v in pack.items() if k != "ledger_commitment"}
    assert _statuses(verify_signed_proof_pack(uncommitted)["evidence"])["content_digest"] == "failed"
    # The ledger cannot be read: nothing is claimed, and it is still not called "HMAC only".
    import pytest as _pytest

    with _pytest.MonkeyPatch.context() as patch:
        patch.setattr(ledger_verify, "stored_receipt", lambda *_a, **_k: (_ for _ in ()).throw(OSError("down")))
        unread = ledger_verify.verify_online(bare, search=True)
    assert unread["verdict"] == "incomplete" and unread["signature_scheme"] != "hmac_only"


def test_one_signature_of_a_hybrid_pair_removed_or_never_made_is_a_failed_downgrade(ledger, monkeypatch):  # noqa: F811
    if not verifier.ml_dsa_available():
        pytest.skip("this build signs classical; there is no pair to break")
    from services import ledger_keys
    from services.ledger_packs import commitment, with_ledger
    from services.signed_proof_pack import build_signed_proof_pack, export_proof_pack_for_job, verify_signed_proof_pack

    pack = export_proof_pack_for_job(_job("job-pair"), actor="ops")
    assert pack["ledger_commitment"]["signature_algs"] == ["Ed25519", "ML-DSA-65"]
    assert pack["ledger"]["statement"]["signer_algs"].count("ML-DSA-65") == 1
    # Remove the post-quantum signature: failed, not "classical".
    stripped = copy.deepcopy(pack)
    stripped["ledger"]["signatures"] = [s for s in stripped["ledger"]["signatures"] if s["alg"] == "Ed25519"]
    checked = verify_signed_proof_pack(stripped)
    assert checked["ok"] is False and _statuses(checked["evidence"])["signatures"] == "failed"
    assert checked["evidence"]["signature_scheme"] == "hybrid"  # what it was issued with
    offline = verifier.verify_evidence(stripped, None, _trusted(pack["ledger"]))
    assert offline["verdict"] == "failed" and offline["signature_scheme"] == "hybrid"
    # A pack promised hybrid whose statement was then signed with Ed25519 alone (the post-quantum
    # signature switched off between signing and issuance): the receipt is internally sound, and
    # the pack still fails, naming the downgrade.
    promised = commitment("w1")
    late = build_signed_proof_pack(job_id="job-late", reconciliation=None, mapping_proof=None,
                                   ledger_commitment=promised)
    monkeypatch.setenv("DATAWRAP_EVIDENCE_PQ", "off")
    ledger_keys.rotate("ws:w1", actor="admin")  # the new key set is classical
    downgraded = with_ledger(late, workspace_id="w1", actor="ops")
    assert downgraded["ledger"]["statement"]["signer_algs"] == ["Ed25519"]
    checked = verify_signed_proof_pack(downgraded)
    detail = next(c["detail"] for c in checked["evidence"]["checks"] if c["id"] == "signatures")
    assert checked["ok"] is False and checked["evidence"]["verdict"] == "failed"
    assert detail == ("the pack was issued with Ed25519 and ML-DSA-65; the receipt carries Ed25519: "
                      "a signature was removed or replaced")
    assert _statuses(checked["evidence"])["inclusion"] == "passed"
    # A pack signed while the workspace signs classical promises classical, and verifies as that.
    honest = export_proof_pack_for_job(_job("job-classical"), actor="ops")
    assert honest["ledger_commitment"]["signature_algs"] == ["Ed25519"]
    assert verify_signed_proof_pack(honest)["evidence"]["signature_scheme"] == "classical"
    assert _statuses(verify_signed_proof_pack(honest)["evidence"])["signatures"] == "passed"


# -------------------------------------------------------------------- the verdict


def test_verified_is_reported_only_when_every_check_passed():
    statuses = ("passed", "failed", "not_checked")
    seen = set()
    for combo in itertools.product(statuses, repeat=len(verifier.CHECK_IDS)):
        checks = {cid: {"status": s, "detail": "d"} for cid, s in zip(verifier.CHECK_IDS, combo)}
        result = verifier.conclude(checks, scheme="classical", mode="offline")
        expected = ("failed" if "failed" in combo else "incomplete" if "not_checked" in combo else "verified")
        assert result["verdict"] == expected, combo
        assert result["counts"] == {s: combo.count(s) for s in statuses}
        assert [c["id"] for c in result["checks"]] == list(verifier.CHECK_IDS)
        if "not_checked" in combo:
            assert result["verdict"] != "verified"
        seen.add(result["verdict"])
        withdrawn = verifier.conclude(checks, scheme="classical", mode="online",
                                      withdrawal={"withdrawn_at": "t", "reason": "r"})
        assert withdrawn["verdict"] == ("failed" if "failed" in combo else "withdrawn")
    assert seen == {"failed", "incomplete", "verified"}
    all_passed = {cid: {"status": "passed", "detail": "d"} for cid in verifier.CHECK_IDS}
    assert verifier.conclude(all_passed, scheme="hmac_only", mode="online")["verdict"] == "hmac_only"


def test_a_check_the_service_cannot_run_reads_not_checked_never_passed(ledger, monkeypatch):  # noqa: F811
    from services import ledger_verify
    from services.signed_proof_pack import export_proof_pack_for_job

    pack = export_proof_pack_for_job(_job("job-skip"), actor="ops")
    monkeypatch.setattr(ledger_verify.ledger_log, "root_hash",
                        lambda *_a, **_k: (_ for _ in ()).throw(OSError("store down")))
    monkeypatch.setattr(ledger_verify.ledger_log, "withdrawal_of",
                        lambda *_a, **_k: (_ for _ in ()).throw(OSError("store down")))
    evidence = ledger_verify.verify_online(pack)
    statuses = _statuses(evidence)
    assert statuses["consistency"] == statuses["withdrawal"] == "not_checked"
    assert evidence["verdict"] == "incomplete" and evidence["issuance"] == "unknown"
    assert "OSError" in evidence["checks"][5]["detail"] and "store down" not in json.dumps(evidence)
    # No trusted key registry: the signatures are not checked either.
    monkeypatch.setattr(ledger_verify.ledger_keys, "trusted_keys",
                        lambda *_a, **_k: (_ for _ in ()).throw(OSError("store down")))
    statuses = _statuses(ledger_verify.verify_online(pack))
    assert statuses["signatures"] == statuses["checkpoint_signature"] == "not_checked"


# --------------------------------------------------------------------- withdrawal


def test_a_withdrawn_pack_reads_issued_later_withdrawn_and_is_never_erased(ledger):  # noqa: F811
    from services.ledger_packs import withdraw
    from services.signed_proof_pack import export_proof_pack_for_job, verify_signed_proof_pack

    pack = export_proof_pack_for_job(_job("job-wd"), actor="ops")
    replacement = export_proof_pack_for_job(_job("job-wd"), actor="ops")
    foreign = export_proof_pack_for_job(_job("job-foreign", "w2"), actor="ops")
    entry = ledger.find_entry(pack["content_sha256"])
    for bad in ("", "x" * 501):
        with pytest.raises(ledger.LedgerInvalid):
            withdraw(entry, reason=bad, superseded_by=None, actor="lead@example.com")
    for bad in ("0" * 64, pack["content_sha256"], foreign["content_sha256"]):
        with pytest.raises(ledger.LedgerInvalid):
            withdraw(entry, reason="wrong run", superseded_by=bad, actor="lead@example.com")
    size = ledger.tree_size()
    out = withdraw(entry, reason="Superseded by the re-run.", superseded_by=replacement["content_sha256"],
                   actor="lead@example.com")
    assert ledger.tree_size() == size + 1  # a new leaf; nothing removed
    assert out["entry"]["status"] == "withdrawal" and out["entry"]["kind"] == "withdrawal"
    assert out["withdrawn"]["status"] == "withdrawn" and out["withdrawn"]["index"] == entry["index"]
    assert out["withdrawn"]["withdrawal"] == {
        "leaf_index": out["entry"]["index"], "withdrawn_at": out["entry"]["issued_at"],
        "reason": "Superseded by the re-run.", "superseded_by": replacement["content_sha256"]}
    statement = out["receipt"]["statement"]
    assert statement["kind"] == "withdrawal" and statement["content_sha256"] == pack["content_sha256"]
    assert statement["withdrawal"]["withdrawn_kind"] == "proof_pack"
    assert ledger.get_entry(entry["index"])["leaf_hash"] == entry["leaf_hash"]  # the original is untouched

    checked = verify_signed_proof_pack(copy.deepcopy(pack))
    evidence = checked["evidence"]
    assert checked["ok"] is True  # the signatures are genuine; the verdict carries the withdrawal
    assert evidence["verdict"] == "withdrawn" and evidence["issuance"] == "issued_later_withdrawn"
    assert evidence["withdrawal"]["reason"] == "Superseded by the re-run."
    assert evidence["summary"].startswith("Issued, later withdrawn at ")
    assert _statuses(evidence)["withdrawal"] == "passed"
    # The replacement is unaffected.
    assert verify_signed_proof_pack(replacement)["evidence"]["issuance"] == "issued"
    with pytest.raises(ledger.LedgerConflict):
        withdraw(ledger.get_entry(entry["index"]), reason="again", superseded_by=None, actor="lead@example.com")
    with pytest.raises(ledger.LedgerInvalid):
        withdraw(ledger.get_entry(out["entry"]["index"]), reason="undo", superseded_by=None, actor="lead")
    # A tampered pack that was also withdrawn is "failed", not "withdrawn".
    forged = {**copy.deepcopy(pack), "actor": "mallory"}
    assert verify_signed_proof_pack(forged)["evidence"]["verdict"] == "failed"
    # Offline, with the withdrawal receipt in hand, the tool says the same.
    keys = _trusted(pack["ledger"], out["receipt"])
    offline = verifier.verify_evidence(pack, None, keys, withdrawal_receipt=out["receipt"])
    assert offline["verdict"] == "withdrawn" and offline["issuance"] == "issued_later_withdrawn"
    unrelated = verifier.verify_evidence(replacement, None, _trusted(replacement["ledger"], out["receipt"]),
                                         withdrawal_receipt=out["receipt"])
    assert _statuses(unrelated)["withdrawal"] == "failed"  # a withdrawal of another pack proves nothing here


def _trusted(*receipts) -> verifier.KeySet:
    from services import ledger_keys

    ids: list[str] = []
    for r in receipts:
        ids += list(r["statement"]["signer_key_ids"]) + list(r["checkpoint"]["signer_key_ids"])
    return ledger_keys.trusted_keys(ids)


def test_a_pack_signed_by_a_retired_key_still_verifies(ledger):  # noqa: F811
    from services import ledger_keys
    from services.signed_proof_pack import export_proof_pack_for_job, verify_signed_proof_pack

    pack = export_proof_pack_for_job(_job("job-rot"), actor="ops")
    ledger_keys.rotate("ws:w1", actor="admin")
    evidence = verify_signed_proof_pack(pack)["evidence"]
    assert _statuses(evidence)["signatures"] == "passed"
    assert {s["key_status"] for s in evidence["signatures"]} == {"retired"}
    later = export_proof_pack_for_job(_job("job-rot"), actor="ops")
    assert not set(later["ledger"]["statement"]["signer_key_ids"]) & set(pack["ledger"]["statement"]["signer_key_ids"])


# -------------------------------------------------------------------------- words

#: Claims no description may make (doc strings included; route doc strings are the API descriptions).
OVERCLAIM = re.compile(r"scitt|sigstore|rekor|quantum[- ]proof|tamper[- ]proof"
                       r"|\b(?:the|is the|as the) (?:first|only)\b|\bunique", re.I)
#: And, in anything shown to a reader, no word of compliance at all. (Doc strings may name S3 Object
#: Lock's "compliance mode", which is that product's name for a retention mode, not a claim.)
BANNED = re.compile(OVERCLAIM.pattern + r"|complian|regulat|legal|certif|qualified|guarantee", re.I)


def test_no_message_claims_compliance_or_more_than_was_checked(ledger):  # noqa: F811
    """Every string the ledger can show a reader: verdict summaries, check details, CLI text, key notes."""
    import ast
    from pathlib import Path

    from services import (
        ledger_anchor,
        ledger_keys,
        ledger_log,
        ledger_merkle,
        ledger_packs,
        ledger_store,
        ledger_verify,
    )

    def shown_strings(path: str) -> list[str]:
        tree = ast.parse(Path(path).read_text(encoding="utf-8"))
        docstrings = {id(n.body[0].value) for n in ast.walk(tree)
                      if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef)) and n.body
                      and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant)}
        return [n.value for n in ast.walk(tree)
                if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docstrings]

    from src.ai.copilot import ledger_tools
    from src.routers import evidence_router

    modules = (verifier, ledger_anchor, ledger_keys, ledger_log, ledger_merkle, ledger_packs, ledger_store,
               ledger_verify, ledger_tools, evidence_router)
    for module in modules:
        for text in shown_strings(module.__file__):
            assert not BANNED.search(text), (module.__name__, text)
        # Doc strings too: the route ones are the API descriptions.
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                doc = ast.get_docstring(node) or ""
                assert not OVERCLAIM.search(doc), (module.__name__, getattr(node, "name", "module"), doc)
                if module in (evidence_router, ledger_tools):
                    assert not BANNED.search(doc), (module.__name__, getattr(node, "name", "module"), doc)
    # And what a run actually prints, for each verdict.
    checks = {cid: {"status": "passed", "detail": "ok"} for cid in verifier.CHECK_IDS}
    for result in (verifier.conclude(checks, scheme="hybrid", mode="online"),
                   verifier.conclude({**checks, "anchor": {"status": "not_checked", "detail": "x"}},
                                     scheme="classical", mode="offline"),
                   verifier.conclude({**checks, "inclusion": {"status": "failed", "detail": "x"}},
                                     scheme="classical", mode="offline"),
                   verifier.conclude(checks, scheme="hybrid", mode="online",
                                     withdrawal={"withdrawn_at": "t", "reason": "r"}),
                   verifier.hmac_only_verdict()):
        text = verifier.render_text(result)
        assert not BANNED.search(text), text
        assert "It does not show that the recorded facts are true." in text
    assert "post-quantum signature (ML-DSA-65)" in ledger_keys.PQ_NOTE and "hybrid" in ledger_keys.PQ_NOTE.lower()
    assert "post-quantum signature (ML-DSA-65)" in verifier.SCHEME_LABELS["hybrid"]
    verified = verifier.conclude(checks, scheme="hybrid", mode="online")["summary"]
    assert "independently verifiable with the published key" in verified
