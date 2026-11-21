"""AI decisions ledger: every AI proposal on a job, who decided, which check verified it — signed.

The ledger is built from what the job and its evidence already record (the
mapping it ran, the Mapping Witness report for that mapping, Parity Studio
decisions, the AI-egress chain), carries no cell values, and rides inside the
signed proof pack and the signed Parity Studio pack, so editing a row breaks
the signature.
"""

from __future__ import annotations

import copy
import json

import pytest

from services.ai_decision_ledger import (
    KIND_MAPPING_PROPOSAL,
    KIND_MODEL_CALL,
    KIND_MODEL_SUGGESTION,
    KIND_PARITY_EXPLANATION,
    KIND_PARITY_FIX,
    KIND_WITNESS_FLAG,
    build_ledger,
    markdown_lines,
)
from services.mapping_witness import witness_mappings
from tests.helpers import witness_fixtures as F

DEST = {"kind": "database", "format": "postgresql", "database": "crm", "schema": "public", "table": "customers"}


@pytest.fixture()
def data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    return tmp_path


def _mappings():
    maps = copy.deepcopy(F.SWAPPED_NAMES)
    for m in maps:
        m.update({"method": "hungarian", "confidence": 0.91, "source_type": "VARCHAR", "target_type": "VARCHAR"})
    # A column the operator remapped by hand.
    maps[4]["user_override"] = True
    # The model suggested another target; the deterministic pick ran.
    maps[1]["engine_suggestion"] = {"target": "first_name", "transform": None, "confidence": 0.8,
                                    "llm_provider": "anthropic", "llm_model": "claude-test",
                                    "suppressed": True, "reason": "llm_suggest_only_deterministic_decides"}
    return maps


def _job(job_id="job-ledger-1", **over):
    job = {
        "_id": job_id,
        "status": "completed",
        "started_at": "2026-11-21T10:00:00+00:00",
        "transfer_request": {"workspace_id": "ws1", "destination": DEST, "mappings": _mappings(),
                             "triggered_by": "ana@example.com"},
        "parity": {
            "explanation": {"text": "Two causes.", "source": "llm", "generated_at": "2026-11-21T11:00:00+00:00"},
            "fixes": {
                "fx1": {"title": "Widen email", "action": "widen_column", "status": "applied",
                        "approved_by": "bo@example.com", "decided_at": "2026-11-21T11:05:00+00:00",
                        "difference_ids": ["d1"]},
                "fx2": {"title": "Accept rounding", "action": "accept", "status": "dismissed",
                        "approved_by": "cy@example.com", "decided_at": "2026-11-21T11:06:00+00:00"},
                "fx3": {"title": "Re-run", "action": "rerun", "status": "proposed"},
            },
            "rounds": [{"n": 1, "fix_id": "fx1", "unexplained_before": 4, "unexplained_after": 0,
                        "at": "2026-11-21T11:07:00+00:00"}],
        },
    }
    job.update(over)
    return job


CALLS = [
    {"job_id": "job-ledger-1", "purpose": "mapping_refinement", "channel": "generate", "provider": "anthropic",
     "payload_sha256": "a" * 64, "metadata_only": True, "cell_values_included": False,
     "crossed_customer_boundary": True, "event_hash": "e1", "sealed_at": "2026-11-21T09:59:00+00:00"},
    {"job_id": "job-ledger-1", "purpose": "parity_explanation", "channel": "prose", "provider": "anthropic",
     "payload_sha256": "b" * 64, "metadata_only": True, "cell_values_included": False,
     "crossed_customer_boundary": True, "event_hash": "e2", "sealed_at": "2026-11-21T11:00:00+00:00"},
]


@pytest.fixture()
def calls(monkeypatch):
    import services.ai_egress as egress

    monkeypatch.setattr(egress, "manifests_for_job", lambda job_id: [c for c in CALLS if c["job_id"] == job_id])
    return CALLS


def _witness(data_dir, review: str | None = None):
    from services.mapping_witness_service import mapping_digest, record_review, save_report

    src, dst = F.customers()
    digest = mapping_digest(F.SWAPPED_NAMES, DEST)
    save_report(witness_mappings(F.SWAPPED_NAMES, src, dst, key=F.KEY), workspace_id="ws1", digest=digest)
    if review:
        record_review(workspace_id="ws1", digest=digest, source="given", decision=review,
                      actor="dee@example.com", new_target="first_name")
    return src


def _by_kind(ledger, kind):
    return [e for e in ledger["entries"] if e["kind"] == kind]


def test_every_ai_involvement_is_a_row_with_origin_decision_and_check(data_dir, calls):
    _witness(data_dir, review="changed")
    ledger = build_ledger(_job())
    proposals = {e["subject"]: e for e in _by_kind(ledger, KIND_MAPPING_PROPOSAL)}
    assert len(proposals) == len(F.SWAPPED_NAMES)
    swapped = proposals["given → last_name"]
    assert swapped["origin"] == {"type": "deterministic", "engine": "semantic_mapper", "method": "hungarian"}
    assert swapped["decision"]["status"] == "accepted" and swapped["decision"]["by"] == "ana@example.com"
    assert swapped["verification"]["check"] == "mapping_witness"
    assert swapped["verification"]["status"] == "flagged"
    assert proposals["login → username"]["decision"]["status"] == "changed"
    assert proposals["mail → email"]["verification"]["status"] == "witnessed"

    [suggestion] = _by_kind(ledger, KIND_MODEL_SUGGESTION)
    assert suggestion["origin"] == {"type": "model", "provider": "anthropic", "model": "claude-test"}
    assert suggestion["decision"]["status"] == "rejected"
    # The witness found the values in the model's target: the suggestion was right.
    assert suggestion["verification"]["status"] == "supported"

    flags = {e["subject"]: e for e in _by_kind(ledger, KIND_WITNESS_FLAG)}
    assert set(flags) == {"given → last_name", "family → first_name"}
    assert flags["given → last_name"]["decision"] == {
        "status": "changed", "by": "dee@example.com", "at": flags["given → last_name"]["decision"]["at"],
        "how": "The reviewer changed the target to first_name.",
    }
    assert flags["family → first_name"]["decision"]["status"] == "unreviewed"

    [explanation] = _by_kind(ledger, KIND_PARITY_EXPLANATION)
    assert explanation["origin"]["type"] == "model" and explanation["origin"]["provider"] == "anthropic"
    assert explanation["inputs_sha256"] and explanation["decision"]["status"] == "informational"
    fixes = {e["subject"]: e for e in _by_kind(ledger, KIND_PARITY_FIX)}
    assert set(fixes) == {"Widen email", "Accept rounding"}  # a still-proposed fix is not a decision yet
    assert fixes["Widen email"]["decision"]["by"] == "bo@example.com"
    assert fixes["Widen email"]["verification"] == {
        "check": "parity_recheck", "status": "passed", "detail": "Unexplained 4 → 0 on re-check."}
    assert fixes["Accept rounding"]["decision"]["status"] == "rejected"


def test_every_model_call_on_the_job_is_in_the_ledger(data_dir, calls):
    ledger = build_ledger(_job())
    assert len(_by_kind(ledger, KIND_MODEL_CALL)) == ledger["counts"]["model_calls"] == len(CALLS)
    assert {e["inputs_sha256"] for e in _by_kind(ledger, KIND_MODEL_CALL)}.isdisjoint({"a" * 64, "b" * 64})
    assert ledger["counts"]["by_origin"]["model"] == 2 + 1 + 1  # calls, suggestion, explanation


def test_without_a_witness_report_nothing_is_called_verified(data_dir, calls):
    ledger = build_ledger(_job())
    for e in _by_kind(ledger, KIND_MAPPING_PROPOSAL):
        assert e["verification"]["check"] == "none"
        assert e["verification"]["status"] == "not_checked"
    assert ledger["witness"] is None
    assert not _by_kind(ledger, KIND_WITNESS_FLAG)


def test_the_ledger_carries_no_cell_values(data_dir, calls):
    src = _witness(data_dir, review="kept")
    text = json.dumps(build_ledger(_job()))
    for row in src:
        for col in ("given", "family", "mail", "login", "joined", "balance"):
            assert f'"{row[col]}"' not in text, (col, row[col])
            assert f" {row[col]} " not in text, (col, row[col])


def test_same_job_same_ledger(data_dir, calls):
    _witness(data_dir)
    a, b = build_ledger(_job()), build_ledger(_job())
    assert a == b and len(a["ledger_sha256"]) == 64


def test_the_signed_proof_pack_seals_the_ledger(data_dir, calls, monkeypatch):
    from services.signed_proof_pack import export_proof_pack_for_job, verify_signed_proof_pack

    _witness(data_dir, review="changed")
    pack = export_proof_pack_for_job(_job())
    assert pack["ai_decisions"]["counts"]["total"] == len(pack["ai_decisions"]["entries"]) > 0
    assert verify_signed_proof_pack(pack)["ok"], verify_signed_proof_pack(pack)
    tampered = copy.deepcopy(pack)
    tampered["ai_decisions"]["entries"][0]["decision"]["by"] = "mallory@example.com"
    result = verify_signed_proof_pack(tampered)
    assert not result["ok"] and "content_sha256 mismatch" in result["errors"]


def test_the_signed_parity_pack_seals_the_ledger_and_the_markdown_shows_it(data_dir, calls, monkeypatch):
    import services.parity_state as parity_state
    from services.parity_signoff import render_markdown
    from services.signed_proof_pack import build_signed_parity_pack, verify_signed_proof_pack

    _witness(data_dir)
    job = _job()
    monkeypatch.setattr(parity_state, "load_job", lambda job_id: job)
    pack = build_signed_parity_pack(job_id="job-ledger-1", report={"job_id": "job-ledger-1"}, actor="ana")
    assert pack["ai_decisions"]["job_id"] == "job-ledger-1" and pack["ai_decisions"]["entries"]
    assert verify_signed_proof_pack(pack)["ok"]
    tampered = copy.deepcopy(pack)
    tampered["ai_decisions"]["entries"].pop()
    assert not verify_signed_proof_pack(tampered)["ok"]

    lines = "\n".join(markdown_lines(pack["ai_decisions"]))
    assert "## AI involvement" in lines
    assert "model mapping suggestion: given → first_name | anthropic (claude-test) | rejected by ana@example.com" in lines
    assert "Ledger SHA-256: " + pack["ai_decisions"]["ledger_sha256"] in lines
    assert "## AI involvement" in render_markdown({}, {**pack, "parity": _minimal_parity_body()})


def test_a_ledger_that_cannot_be_built_is_stated_in_the_pack_not_dropped(monkeypatch):
    import services.parity_state as parity_state
    from services.signed_proof_pack import build_signed_parity_pack

    def boom(job_id):
        raise RuntimeError("store down")

    monkeypatch.setattr(parity_state, "load_job", boom)
    pack = build_signed_parity_pack(job_id="j9", report={"job_id": "j9"}, actor="ana")
    assert pack["ai_decisions"]["status"] == "unavailable"
    assert "could not be built" in "\n".join(markdown_lines(pack["ai_decisions"]))


def test_ledger_route_is_workspace_scoped(data_dir, calls, monkeypatch):
    from fastapi.testclient import TestClient

    import services.parity_state as parity_state
    from src.main import app

    job = _job()

    class Store:
        def get_job(self, job_id):
            return job if job_id == "job-ledger-1" else None

    monkeypatch.setattr(parity_state, "job_store", lambda: Store())
    client = TestClient(app)
    ok = client.get("/api/v1/jobs/job-ledger-1/ai-decisions")
    assert ok.status_code == 200, ok.text
    assert ok.json()["counts"]["by_kind"][KIND_MAPPING_PROPOSAL] == len(F.SWAPPED_NAMES)
    assert client.get("/api/v1/jobs/nope/ai-decisions").status_code == 404


def _minimal_parity_body():
    return {
        "job_id": "job-ledger-1", "verdict": "proven", "verdict_reason": "All rows match.",
        "confidence": {"statement": "Full checksum."},
        "parity": {"rows_source": 1, "rows_destination": 1, "rows_differ": 0, "explained": 0, "unexplained": 0,
                   "proven_equal_pct": 100},
        "tables": [], "differences": [], "fixes": [], "rounds": [],
        "signoff": {"approvals": [], "signed_at": None},
    }
