"""AI decisions ledger: every AI proposal on a job, who decided, which check verified it — signed.

The ledger reads only the evidence snapshot frozen onto the job at submit
(``services.mapping_witness_service.snapshot_for_request``) and the job's own
Parity Studio state and AI-egress chain. "Who" is the person who approved or
changed the pair in Map (recorded server-side from the authenticated caller),
never the account that started the run; a scheduled run says so separately.
Anything unreadable makes the ledger "partial", never silently shorter.
"""

from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import pytest

from services.ai_decision_ledger import (
    KIND_MAPPING_PROPOSAL,
    KIND_MODEL_CALL,
    KIND_MODEL_SUGGESTION,
    KIND_PARITY_EXPLANATION,
    KIND_PARITY_FIX,
    KIND_WITNESS_FLAG,
    attested_digest_for_job,
    build_ledger,
    markdown_lines,
)
from services.mapping_witness import witness_mappings
from tests.helpers import witness_fixtures as F

WS = "ws1"
SESSION = "plan-0001-session"
SOURCE = "file|customers.csv"


def _dest(**over):
    from src.transfer.models import EndpointConfig

    base = dict(kind="database", format="postgresql", host="db.internal.test", port=5432, database="crm",
                schema="public", table="customers")
    base.update(over)
    return EndpointConfig(**base)


@pytest.fixture()
def data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    return tmp_path


def _rows(maps, **marks):
    out = copy.deepcopy(maps)
    for m in out:
        m.update({"method": "hungarian", "confidence": 0.91, "source_type": "VARCHAR", "target_type": "VARCHAR"})
        m.update(marks.get(m["source"], {}))
    return out


def _submit(mappings, *, triggered_by="ana@example.com", dest=None, session=SESSION, filename="customers.csv"):
    """What create_pending_job stamps: the snapshot for this mapping, destination, source and session."""
    from services.mapping_witness_service import snapshot_for_request

    request = SimpleNamespace(workspace_id=WS, destination=dest or _dest(), mappings=mappings,
                              triggered_by=triggered_by, witness_session_id=session,
                              source=SimpleNamespace(kind="file", table="", collection=""), source_filename=filename)
    return snapshot_for_request(request)


def _job(mappings, snapshot, job_id="job-ledger-1", **over):
    job = {
        "_id": job_id,
        "workspace_id": WS,
        "status": "completed",
        "started_at": "2026-11-21T10:00:00+00:00",
        "transfer_request": {"workspace_id": WS, "mappings": mappings,
                             "triggered_by": over.pop("triggered_by", "ana@example.com")},
        "mapping_witness": snapshot,
    }
    job.update(over)
    return job


PARITY = {
    "explanation": {"text": "Two causes.", "source": "llm", "generated_at": "2026-11-21T11:00:00+00:00"},
    "fixes": {
        "fx1": {"title": "Widen email", "action": "widen_column", "status": "applied",
                "approved_by": "bo@example.com", "decided_at": "2026-11-21T11:05:00+00:00", "difference_ids": ["d1"]},
        "fx2": {"title": "Accept rounding", "action": "accept", "status": "dismissed",
                "approved_by": "cy@example.com", "decided_at": "2026-11-21T11:06:00+00:00"},
        "fx3": {"title": "Re-run", "action": "rerun", "status": "proposed"},
    },
    "rounds": [{"n": 1, "fix_id": "fx1", "unexplained_before": 4, "unexplained_after": 0,
                "at": "2026-11-21T11:07:00+00:00"}],
}

CALLS = [
    {"job_id": "job-ledger-1", "purpose": "parity_explanation", "channel": "prose", "provider": "anthropic",
     "payload_sha256": "b" * 64, "metadata_only": True, "cell_values_included": False,
     "crossed_customer_boundary": True, "event_hash": "e2", "sealed_at": "2026-11-21T11:00:00+00:00"},
]


@pytest.fixture()
def calls(monkeypatch):
    import services.ai_egress as egress

    monkeypatch.setattr(egress, "manifests_for_job", lambda job_id: [c for c in CALLS if c["job_id"] == job_id])
    return CALLS


def _witness_on_map(maps, *, actor="map-user@example.com", session=SESSION, source=SOURCE):
    """What the Map step does: run the witness for this mapping (destination already holds the rows)."""
    from services.mapping_witness_service import destination_identity, mapping_digest, save_report

    src, dst = F.customers()
    identity = destination_identity(_dest(), WS)
    digest = mapping_digest(maps, identity)
    saved = save_report(witness_mappings(maps, src, dst, key=F.KEY), workspace_id=WS, session=session,
                        identity=identity, digest=digest, source=source, actor=actor)
    return src, identity, digest, saved["report"]


def _flag_id(report, source):
    return next(p["flag"]["id"] for p in report["pairs"] if p["source"] == source)


def _by_kind(ledger, kind):
    return [e for e in ledger["entries"] if e["kind"] == kind]


def test_who_is_the_person_who_approved_in_map_not_who_started_the_run(data_dir, calls):
    from services.mapping_witness_service import record_map_decisions

    maps = _rows(F.SWAPPED_NAMES)
    _src, identity, _, _ = _witness_on_map(maps)
    record_map_decisions(workspace_id=WS, session=SESSION, identity=identity, actor="dee@example.com", source=SOURCE, mappings=[
        {"source": "mail", "target": "email", "approved": True},
        {"source": "login", "target": "username", "approved": True, "changed": True},
    ])
    job = _job(maps, _submit(maps), parity=PARITY, triggered_by="ana@example.com")
    rows = {e["subject"]: e for e in _by_kind(build_ledger(job), KIND_MAPPING_PROPOSAL)}
    assert rows["mail → email"]["decision"]["status"] == "approved"
    assert rows["mail → email"]["decision"]["by"] == "dee@example.com"
    assert rows["mail → email"]["decision"]["run_by"] == "ana@example.com"
    assert rows["login → username"]["decision"]["status"] == "changed"
    # A pair nobody approved does not borrow the run's account as its approver.
    assert rows["given → last_name"]["decision"]["status"] == "unreviewed"
    assert rows["given → last_name"]["decision"]["by"] is None
    assert rows["mail → email"]["verification"]["status"] == "witnessed"
    assert rows["given → last_name"]["verification"]["status"] == "flagged"


def test_a_scheduled_run_shows_the_map_approver_and_the_schedule_separately(data_dir, calls):
    from services.mapping_witness_service import record_map_decisions

    maps = _rows(F.CORRECT_MAPPING)
    _src, identity, _, _ = _witness_on_map(maps)
    record_map_decisions(workspace_id=WS, session=SESSION, identity=identity, actor="dee@example.com", source=SOURCE,
                         mappings=[{"source": "mail", "target": "email", "approved": True}])
    job = _job(maps, _submit(maps, triggered_by="schedule:nightly"), triggered_by="schedule:nightly")
    ledger = build_ledger(job)
    row = next(e for e in _by_kind(ledger, KIND_MAPPING_PROPOSAL) if e["subject"] == "mail → email")
    assert row["decision"]["by"] == "dee@example.com"
    assert row["decision"]["run_kind"] == "schedule" and row["decision"]["run_by"] == "nightly"
    assert "run by schedule nightly" in "\n".join(markdown_lines(ledger))


def test_only_a_named_provider_makes_a_model_suggestion(data_dir, calls):
    from services.mapping_witness_service import destination_identity, record_model_suggestions

    maps = _rows(F.SWAPPED_NAMES, family={"engine_suggestion": {"target": "last_name", "suppressed": True}})
    _witness_on_map(maps)
    # What /transfer/map received from the model, recorded server-side (the run payload drops it).
    record_model_suggestions(workspace_id=WS, session=SESSION, identity=destination_identity(_dest(), WS), llm={
        "ai_egress": {"payload_sha256": "a" * 64, "event_hash": "e1"}}, mappings=[
        {"source": "given", "target": "last_name", "engine_suggestion": {
            "target": "first_name", "llm_provider": "anthropic", "llm_model": "claude-test"}}])
    ui_rows = [{k: v for k, v in m.items() if k != "engine_suggestion"} for m in maps]
    ledger = build_ledger(_job(maps, _submit(ui_rows)))
    [suggestion] = _by_kind(ledger, KIND_MODEL_SUGGESTION)  # the deterministic alternative is not a model's
    assert suggestion["subject"] == "given → first_name"
    assert suggestion["origin"] == {"type": "model", "provider": "anthropic", "model": "claude-test"}
    assert suggestion["decision"]["status"] == "rejected"
    assert suggestion["verification"]["status"] == "supported"


def test_a_change_that_was_reverted_is_not_reported_as_changed(data_dir, calls):
    from services.mapping_witness_service import record_review

    maps = _rows(F.LOGIN_INTO_EMAIL_ONLY)
    _src, identity, digest, report = _witness_on_map(maps)
    # Map auto-records "changed" when the flagged column is remapped…
    record_review(workspace_id=WS, session=SESSION, identity=identity, digest=digest, source="login", decision="changed",
                  actor="dee@example.com", new_target="username")
    # …then the operator reverts and runs the flagged mapping.
    [flag] = _by_kind(build_ledger(_job(maps, _submit(maps))), KIND_WITNESS_FLAG)
    assert flag["subject"] == "login → email"
    assert flag["decision"]["status"] == "unreviewed"
    assert "changed" not in json.dumps(flag["decision"])


def test_the_corrected_job_shows_the_flag_it_resolved(data_dir, calls):
    from services.mapping_witness_service import record_review

    flagged = _rows(F.LOGIN_INTO_EMAIL_ONLY)
    _src, identity, digest, report = _witness_on_map(flagged)
    record_review(workspace_id=WS, session=SESSION, identity=identity, digest=digest, source="login", decision="changed",
                  actor="dee@example.com", new_target="username")
    fixed = _rows([m if m["source"] != "login" else {"source": "login", "target": "username"}
                   for m in F.LOGIN_INTO_EMAIL_ONLY])
    _witness_on_map(fixed)
    flags = _by_kind(build_ledger(_job(fixed, _submit(fixed))), KIND_WITNESS_FLAG)
    assert [(f["subject"], f["decision"]["status"], f["decision"]["by"]) for f in flags] == [
        ("login → email", "changed", "dee@example.com")]


def test_a_later_map_session_cannot_rewrite_what_a_finished_job_says(data_dir, calls):
    from services.mapping_witness_service import destination_identity, mapping_digest, save_report

    maps = _rows(F.LOGIN_INTO_EMAIL_ONLY)
    _witness_on_map(maps)
    job = _job(maps, _submit(maps))
    before = build_ledger(job)
    assert any(e["kind"] == KIND_WITNESS_FLAG for e in before["entries"])
    # Six fabricated rows for the same pairs and table: same digest, a clean report.
    fake = [{"cust_id": i, "login": f"u{i}@example.com", "given": "A", "family": "B", "joined": "01/01/2020",
             "balance": "1.00"} for i in range(6)]
    identity = destination_identity(_dest(), WS)
    save_report(witness_mappings(maps, fake), workspace_id=WS, session=SESSION, identity=identity,
                digest=mapping_digest(maps, identity), source=SOURCE)
    after = build_ledger(job)
    assert after["ledger_sha256"] == before["ledger_sha256"]
    assert after["entries"] == before["entries"]


def test_a_store_failure_at_submit_or_an_unreadable_chain_makes_the_ledger_partial(data_dir, monkeypatch):
    import services.ai_egress as egress
    import services.mapping_witness_service as svc

    maps = _rows(F.CORRECT_MAPPING)

    def store_down(**kwargs):
        raise RuntimeError("store down")

    monkeypatch.setattr(svc, "build_snapshot", store_down)
    snap = _submit(maps)
    assert snap["status"] == "unavailable"

    def chain_down(job_id):
        raise OSError("chain down")

    monkeypatch.setattr(egress, "manifests_for_job", chain_down)
    ledger = build_ledger(_job(maps, snap))
    assert ledger["status"] == "partial"
    assert any("could not be read when the job was submitted" in p for p in ledger["problems"])
    assert any("AI-egress chain could not be read" in p for p in ledger["problems"])
    assert "**Partial:**" in "\n".join(markdown_lines(ledger))


def test_a_tampered_snapshot_is_not_used(data_dir, calls):
    maps = _rows(F.LOGIN_INTO_EMAIL_ONLY)
    _witness_on_map(maps)
    snap = _submit(maps)
    snap["report"]["pairs"] = [p for p in snap["report"]["pairs"] if p["status"] != "flagged"]
    ledger = build_ledger(_job(maps, snap))
    assert ledger["status"] == "partial" and not _by_kind(ledger, KIND_WITNESS_FLAG)


def test_every_model_call_and_parity_decision_is_a_row(data_dir, calls):
    maps = _rows(F.CORRECT_MAPPING)
    _witness_on_map(maps)
    ledger = build_ledger(_job(maps, _submit(maps), parity=PARITY))
    assert ledger["status"] == "complete", ledger["problems"]
    assert len(_by_kind(ledger, KIND_MODEL_CALL)) == ledger["counts"]["model_calls"] == 1
    [explanation] = _by_kind(ledger, KIND_PARITY_EXPLANATION)
    assert explanation["origin"]["provider"] == "anthropic"
    fixes = {e["subject"]: e for e in _by_kind(ledger, KIND_PARITY_FIX)}
    assert set(fixes) == {"Widen email", "Accept rounding"}
    assert fixes["Widen email"]["verification"]["status"] == "passed"


def test_the_ledger_carries_no_cell_values(data_dir, calls):
    maps = _rows(F.SWAPPED_NAMES)
    src, _identity, _, _ = _witness_on_map(maps)
    text = json.dumps(build_ledger(_job(maps, _submit(maps), parity=PARITY)))
    for row in src:
        for col in ("given", "family", "mail", "login", "joined", "balance"):
            assert f'"{row[col]}"' not in text, (col, row[col])


def test_the_signed_packs_seal_the_ledger_and_approvers_attest_it(data_dir, calls, monkeypatch):
    import services.parity_state as parity_state
    from services.signed_proof_pack import (
        build_signed_parity_pack,
        export_proof_pack_for_job,
        verify_signed_proof_pack,
    )

    maps = _rows(F.SWAPPED_NAMES)
    _witness_on_map(maps)
    job = _job(maps, _submit(maps), parity=PARITY)
    pack = export_proof_pack_for_job(job)
    assert verify_signed_proof_pack(pack)["ok"]
    tampered = copy.deepcopy(pack)
    tampered["ai_decisions"]["entries"][0]["decision"]["by"] = "mallory@example.com"
    assert "content_sha256 mismatch" in verify_signed_proof_pack(tampered)["errors"]

    monkeypatch.setattr(parity_state, "load_job", lambda job_id: job)
    report = {"job_id": "job-ledger-1", "ai_decisions_attested_sha256": attested_digest_for_job(job)}
    ppack = build_signed_parity_pack(job_id="job-ledger-1", report=report, actor="ana")
    assert verify_signed_proof_pack(ppack)["ok"]
    assert ppack["ai_decisions"]["attested_sha256"] == ppack["parity"]["ai_decisions_attested_sha256"]


def test_v2_approvals_attest_the_ledger_and_earlier_approvals_keep_their_version():
    from services.parity_signoff import CONTENT_VERSION, content_digests, content_sha256, valid_approvals

    report = {"job_id": "j", "verdict": "proven", "parity": {}, "tables": [], "differences": [], "fixes": [],
              "ai_decisions_attested_sha256": "1" * 64}
    v1 = {"role": "engineering", "content_sha256": content_sha256(report, 1)}  # given before v2 existed
    v2 = {"role": "compliance", "content_sha256": content_sha256(report), "content_version": CONTENT_VERSION}
    state = {"approvals": [v1, v2]}
    assert valid_approvals(state, content_digests(report))[0] == [v1, v2]
    changed = {**report, "ai_decisions_attested_sha256": "2" * 64}
    valid, invalidated = valid_approvals(state, content_digests(changed))
    assert valid == [v1] and invalidated == [v2]


def test_ledger_routes_are_workspace_scoped(data_dir, calls, monkeypatch):
    from fastapi.testclient import TestClient

    import importlib

    import services.parity_state as parity_state
    from src.main import app

    connectors_router = importlib.import_module("src.routers.connectors_router")

    maps = _rows(F.CORRECT_MAPPING)
    _witness_on_map(maps)
    jobs = {"mine": _job(maps, _submit(maps), job_id="mine"),
            "theirs": {**_job(maps, _submit(maps), job_id="theirs"), "workspace_id": "ws2"}}
    monkeypatch.setattr(parity_state, "job_store", lambda: SimpleNamespace(get_job=jobs.get))
    monkeypatch.setattr(connectors_router, "tenant_scope",
                        lambda request, *a: SimpleNamespace(allows=lambda ws: ws == WS))
    client = TestClient(app)
    for path in ("ai-decisions", "mapping-witness"):
        assert client.get(f"/api/v1/jobs/mine/{path}").status_code == 200
        assert client.get(f"/api/v1/jobs/theirs/{path}").status_code == 404, path
        assert client.get(f"/api/v1/jobs/nope/{path}").status_code == 404
    assert client.get("/api/v1/jobs/mine/mapping-witness").json()["witness"]["report"]["mode"] == "keyed"


def test_map_records_the_models_suggestions_for_ui_run_jobs(data_dir, calls, monkeypatch):
    """Review 4e: Execute's payload drops engine_suggestion, so /transfer/map records them server-side."""
    import importlib

    from fastapi.testclient import TestClient

    import services.mapping_pipeline as pipeline
    from src.main import app

    transfer_router = importlib.import_module("src.routers.transfer_router")

    async def no_keys(body, request):
        return [], "none"

    monkeypatch.setattr(transfer_router, "_map_key_columns", no_keys)
    monkeypatch.setattr(pipeline, "run_mapping_pipeline", lambda *a, **kw: {
        "mappings": [
            {"source": "given", "target": "last_name", "confidence": 0.9,
             "engine_suggestion": {"target": "first_name", "llm_provider": "anthropic", "llm_model": "claude-test"}},
            {"source": "family", "target": "first_name", "confidence": 0.9,
             "engine_suggestion": {"target": "last_name"}},  # a deterministic alternative
        ],
        "validation": {"passed": True, "issues": []}, "classification": {},
        "llm": {"llm_used": True, "ai_egress": {"payload_sha256": "c" * 64}},
    })
    dest = {"kind": "database", "format": "postgresql", "host": "db.internal.test", "port": 0, "database": "crm",
            "schema": "", "table": "customers"}
    res = TestClient(app).post("/api/v1/transfer/map", headers={"X-Workspace-Id": WS}, json={
        "source_columns": ["given", "family"], "target_columns": ["first_name", "last_name"], "destination": dest,
        "witness_session_id": SESSION})
    assert res.status_code == 200, res.text
    ran = [{"source": "given", "target": "last_name"}, {"source": "family", "target": "first_name"}]
    ledger = build_ledger(_job(ran, _submit(ran)))
    [suggestion] = _by_kind(ledger, KIND_MODEL_SUGGESTION)
    assert suggestion["subject"] == "given → first_name"
    assert suggestion["inputs_sha256"] and suggestion["origin"]["model"] == "claude-test"


# ── Round 2: evidence belongs to the Map session it was made in (N3, N4) ─────


def test_evidence_from_another_session_or_no_session_is_not_checked(data_dir, calls):
    maps = _rows(F.LOGIN_INTO_EMAIL_ONLY)
    _witness_on_map(maps, session="someone-elses-session")
    for snap in (_submit(maps), _submit(maps, session="")):
        ledger = build_ledger(_job(maps, snap))
        rows = _by_kind(ledger, KIND_MAPPING_PROPOSAL)
        assert {r["verification"]["status"] for r in rows} == {"not_checked"}
        assert not _by_kind(ledger, KIND_WITNESS_FLAG)
    assert "not submitted from a Map session" in _submit(maps, session="")["not_checked_reason"]


def test_evidence_from_another_file_or_too_old_is_not_checked(data_dir, calls):
    from services.mapping_witness_service import build_snapshot, destination_identity

    maps = _rows(F.LOGIN_INTO_EMAIL_ONLY)
    _witness_on_map(maps)
    other_file = _submit(maps, filename="customers_v2.csv")
    assert other_file["report"] is None and other_file["not_checked_reason"]
    later = build_snapshot(workspace_id=WS, session=SESSION, identity=destination_identity(_dest(), WS),
                           source=SOURCE, mappings=maps, taken_at="2099-01-01T00:00:00+00:00")
    assert later["report"] is None


def test_evidence_saved_by_a_node_with_a_slightly_fast_clock_still_counts(data_dir, calls):
    """Two API nodes: the check was saved on one whose clock runs a little ahead of the submitting node."""
    from datetime import datetime, timedelta, timezone

    from services.mapping_witness_service import build_snapshot, destination_identity

    maps = _rows(F.LOGIN_INTO_EMAIL_ONLY)
    saved = _witness_on_map(maps)[3]
    assert saved
    identity = destination_identity(_dest(), WS)
    slow = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    assert build_snapshot(workspace_id=WS, session=SESSION, identity=identity, source=SOURCE, mappings=maps,
                          taken_at=slow)["report"] is not None
    days_before = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    assert build_snapshot(workspace_id=WS, session=SESSION, identity=identity, source=SOURCE, mappings=maps,
                          taken_at=days_before)["report"] is None


def test_fabricated_rows_in_another_session_do_not_clear_this_sessions_flag(data_dir, calls):
    from services.mapping_witness_service import destination_identity, mapping_digest, save_report

    maps = _rows(F.LOGIN_INTO_EMAIL_ONLY)
    _witness_on_map(maps)
    fake = [{"cust_id": i, "login": f"u{i}@example.com"} for i in range(6)]
    identity = destination_identity(_dest(), WS)
    save_report(witness_mappings(maps, fake), workspace_id=WS, session="another-session-id", identity=identity,
                digest=mapping_digest(maps, identity), source=SOURCE)
    assert len(_by_kind(build_ledger(_job(maps, _submit(maps))), KIND_WITNESS_FLAG)) == 1


def test_an_approval_posted_in_another_session_never_attaches(data_dir, calls):
    from services.mapping_witness_service import record_map_decisions

    maps = _rows(F.CORRECT_MAPPING)
    _src, identity, _, _ = _witness_on_map(maps)
    record_map_decisions(workspace_id=WS, session="a-different-session", identity=identity, source=SOURCE,
                         actor="mallory@example.com", mappings=[{"source": "mail", "target": "email", "approved": True}])
    row = next(e for e in _by_kind(build_ledger(_job(maps, _submit(maps))), KIND_MAPPING_PROPOSAL)
               if e["subject"] == "mail → email")
    assert row["decision"]["by"] is None and row["decision"]["status"] == "unreviewed"


def test_kept_needs_the_explicit_keep_on_that_flag_and_continuing_is_recorded(data_dir, calls):
    from services.mapping_witness_service import record_map_decisions, record_review

    maps = _rows(F.LOGIN_INTO_EMAIL_ONLY)
    _src, identity, digest, report = _witness_on_map(maps)
    # Approving the row (Continue) after the flag was shown is not a review of the flag.
    record_map_decisions(workspace_id=WS, session=SESSION, identity=identity, actor="dee@example.com", source=SOURCE,
                         mappings=[{"source": "login", "target": "email", "approved": True}])
    [flag] = _by_kind(build_ledger(_job(maps, _submit(maps))), KIND_WITNESS_FLAG)
    assert flag["decision"]["status"] == "unreviewed"
    with pytest.raises(LookupError):
        record_review(workspace_id=WS, session=SESSION, identity=identity, digest=digest, source="login",
                      decision="kept", actor="dee@example.com", flag_id="not-this-flag")
    record_review(workspace_id=WS, session=SESSION, identity=identity, digest=digest, source="login",
                  decision="continued", actor="dee@example.com", flag_id=_flag_id(report, "login"))
    [flag] = _by_kind(build_ledger(_job(maps, _submit(maps))), KIND_WITNESS_FLAG)
    assert flag["decision"]["status"] == "not_reviewed_continued" and flag["decision"]["by"] == "dee@example.com"
    record_review(workspace_id=WS, session=SESSION, identity=identity, digest=digest, source="login",
                  decision="kept", actor="eve@example.com", flag_id=_flag_id(report, "login"))
    [flag] = _by_kind(build_ledger(_job(maps, _submit(maps))), KIND_WITNESS_FLAG)
    assert flag["decision"]["status"] == "kept" and flag["decision"]["by"] == "eve@example.com"


def test_the_ledger_says_who_ran_the_check_and_when(data_dir, calls):
    maps = _rows(F.CORRECT_MAPPING)
    _witness_on_map(maps, actor="map-user@example.com")
    ledger = build_ledger(_job(maps, _submit(maps)))
    assert ledger["witness"]["checked_by"] == "map-user@example.com" and ledger["witness"]["checked_at"]
    row = _by_kind(ledger, KIND_MAPPING_PROPOSAL)[0]
    assert row["verification"]["checked_by"] == "map-user@example.com"


# ── Round 2: content versions (N8) ───────────────────────────────────────────


def test_an_approval_with_an_unknown_version_or_no_digest_attests_nothing():
    from services.parity_signoff import content_digests, valid_approvals

    report = {"job_id": "j", "verdict": "proven", "parity": {}, "tables": [], "differences": [], "fixes": []}
    digests = content_digests(report)
    state = {"approvals": [{"role": "a", "content_version": 9, "content_sha256": None},
                           {"role": "b", "content_version": 7, "content_sha256": digests[2]},
                           {"role": "c", "content_version": "x", "content_sha256": digests[1]}]}
    assert valid_approvals(state, digests)[0] == []


def test_a_mixed_version_set_never_seals_and_newer_versions_win():
    from services.parity_signoff import one_content_version, signing_version

    v1 = {"role": "engineering", "content_sha256": "a", "content_version": 1}
    v2 = {"role": "compliance", "content_sha256": "b", "content_version": 2}
    keep, superseded = one_content_version([v1, v2])
    assert keep == [v2] and superseded == [v1]
    # A report approved before v2 existed keeps collecting v1, so its set seals as v1.
    assert signing_version([{"role": "engineering", "content_sha256": "a"}]) == 1
    assert signing_version([]) == 2
