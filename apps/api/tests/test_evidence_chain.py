"""Verification of the append-only evidence chain.

The chain existed before this suite; nothing re-walked it, so an altered or
deleted record was undetectable. These tests are about the detection, and about
the two ways a verifier can be dishonest: calling retention "tampering", and
letting a proof pack claim a chain position it never occupied.
"""

from __future__ import annotations

import json

import pytest

from services import audit_log as audit
from services import evidence_chain as chain


@pytest.fixture()
def file_chain(tmp_path, monkeypatch):
    """An isolated file-backed chain with its own retention checkpoint store."""
    monkeypatch.setattr(audit, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit, "_mongo_collection", lambda: None)
    monkeypatch.setattr(chain, "truncation_store_path", lambda: tmp_path / "truncations.jsonl")
    return tmp_path / "audit.jsonl"


def _lines(path) -> list[dict]:
    return [json.loads(ln) for ln in path.read_text(encoding="utf-8").strip().splitlines() if ln.strip()]


def _rewrite(path, events: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")


def test_an_untouched_chain_verifies(file_chain):
    for i in range(4):
        audit.append_audit_event(action=f"a{i}", resource="r", actor="t")

    report = chain.verify_chain()

    assert report["verified"] is True
    assert report["checked"] == 4
    assert report["findings"] == []
    assert report["chain_head"] == audit.latest_event_hash()


def test_an_edited_record_is_named_by_index_and_id(file_chain):
    audit.append_audit_event(action="created", resource="connector:1", actor="t")
    audit.append_audit_event(action="deleted", resource="connector:1", actor="mallory")
    audit.append_audit_event(action="exported", resource="job:9", actor="t")

    events = _lines(file_chain)
    events[1]["actor"] = "someone-else"
    _rewrite(file_chain, events)

    report = chain.verify_chain()

    assert report["verified"] is False
    mismatches = [f for f in report["findings"] if f["kind"] == "event_hash_mismatch"]
    assert [f["index"] for f in mismatches] == [1]
    assert mismatches[0]["event_id"] == events[1]["id"]


def test_a_deleted_record_breaks_the_link_it_used_to_fill(file_chain):
    for i in range(3):
        audit.append_audit_event(action=f"a{i}", resource="r", actor="t")

    events = _lines(file_chain)
    _rewrite(file_chain, [events[0], events[2]])

    report = chain.verify_chain()

    assert report["verified"] is False
    assert [f["kind"] for f in report["findings"]] == ["broken_link"]
    assert report["findings"][0]["index"] == 1


def test_reordering_records_does_not_pass_as_intact_history(file_chain):
    for i in range(3):
        audit.append_audit_event(action=f"a{i}", resource="r", actor="t")

    events = _lines(file_chain)
    _rewrite(file_chain, [events[0], events[2], events[1]])

    report = chain.verify_chain()

    assert report["verified"] is False
    assert {f["kind"] for f in report["findings"]} == {"broken_link"}


def test_two_records_claiming_one_predecessor_report_a_fork(file_chain):
    audit.append_audit_event(action="a", resource="r", actor="t")
    audit.append_audit_event(action="b", resource="r", actor="t")

    events = _lines(file_chain)
    # A replayed segment: the same record filed twice under one predecessor.
    _rewrite(file_chain, [events[0], events[1], dict(events[1])])

    kinds = {f["kind"] for f in chain.verify_chain()["findings"]}

    assert "fork" in kinds


def test_a_record_without_a_hash_cannot_be_called_verified(file_chain):
    audit.append_audit_event(action="a", resource="r", actor="t")

    events = _lines(file_chain)
    events[0].pop("event_hash")
    _rewrite(file_chain, events)

    report = chain.verify_chain()

    assert report["verified"] is False
    assert [f["kind"] for f in report["findings"]] == ["event_hash_missing"]


def test_retention_is_reported_as_retention_not_as_a_broken_chain(file_chain, monkeypatch):
    """Trimming the oldest records must not look like a deletion attack.

    Without the checkpoint the first surviving record points at a hash that is
    no longer in the store, which is exactly what a deleted record looks like.
    """
    monkeypatch.setattr(audit, "MAX_EVENTS", 3)
    for i in range(6):
        audit.append_audit_event(action=f"a{i}", resource="r", actor="t")

    report = chain.verify_chain()

    assert report["verified"] is True, report["findings"]
    assert report["checked"] == 3
    checkpoints = report["retention_checkpoints"]
    assert sum(c["removed_count"] for c in checkpoints) == 3
    assert checkpoints[-1]["first_kept_event_hash"] == _lines(file_chain)[0]["event_hash"]


def test_a_missing_prefix_with_no_checkpoint_stays_unexplained(file_chain):
    """A deletion must not be excusable just because it hit the oldest record."""
    for i in range(3):
        audit.append_audit_event(action=f"a{i}", resource="r", actor="t")

    _rewrite(file_chain, _lines(file_chain)[1:])

    report = chain.verify_chain()

    assert report["verified"] is False
    assert [f["kind"] for f in report["findings"]] == ["unexplained_prefix"]


def test_an_edited_retention_checkpoint_cannot_excuse_a_deletion(file_chain):
    """The checkpoint is HMAC-signed, so forging one needs the platform secret."""
    for i in range(3):
        audit.append_audit_event(action=f"a{i}", resource="r", actor="t")
    kept = _lines(file_chain)[1:]
    _rewrite(file_chain, kept)

    forged = {
        "kind": "chain_truncation",
        "at": "2026-01-01T00:00:00+00:00",
        "removed_count": 1,
        "last_removed_event_hash": kept[0]["prev_hash"],
        "first_kept_event_hash": kept[0]["event_hash"],
        "hash_alg": "HMAC-SHA256",
        "checkpoint_hmac": "0" * 64,
    }
    path = chain.truncation_store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(forged) + "\n", encoding="utf-8")

    report = chain.verify_chain()

    assert chain.list_truncations() == []
    assert [f["kind"] for f in report["findings"]] == ["unexplained_prefix"]


def test_an_empty_store_does_not_claim_to_prove_history(file_chain):
    report = chain.verify_chain()

    assert report["verified"] is True
    assert report["checked"] == 0
    assert "proves nothing" in report["honesty"]


def test_anchoring_files_the_digest_into_the_chain_and_can_be_found(file_chain):
    anchor = chain.anchor_evidence(
        evidence_kind="signed_proof_pack",
        evidence_sha256="a" * 64,
        job_id="job-1",
        actor="auditor@example.com",
    )

    assert anchor["anchored"] is True
    assert anchor["event_hash"] == audit.latest_event_hash()

    record = chain.find_anchor("a" * 64)

    assert record is not None
    assert record["action"] == chain.EVIDENCE_ACTION
    assert record["details"]["job_id"] == "job-1"
    assert chain.find_anchor("b" * 64) is None
    assert chain.verify_chain()["verified"] is True


def test_an_unavailable_store_reports_unanchored_rather_than_pretending(file_chain, monkeypatch):
    def boom(**_kwargs):
        raise RuntimeError("audit store down")

    monkeypatch.setattr(audit, "append_audit_event", boom)

    anchor = chain.anchor_evidence(evidence_kind="signed_proof_pack", evidence_sha256="c" * 64)

    assert anchor["anchored"] is False
    assert "audit store down" in anchor["reason"]


def test_run_completion_leaves_a_chained_record_without_a_pack_export(file_chain):
    from services import lineage_telemetry

    lineage_telemetry.emit_run_completed(
        run_id="run-1",
        job_id="job-7",
        records_transferred=5,
        destination_summary={
            "reconciliation": {"passed": True, "phase": "post_write", "coverage": "full_checksum"}
        },
    )

    sealed = [e for e in chain.read_chain() if e["action"] == chain.RUN_EVIDENCE_ACTION]

    assert len(sealed) == 1
    assert sealed[0]["details"]["run_id"] == "run-1"
    assert sealed[0]["details"]["records_transferred"] == 5
    assert sealed[0]["details"]["gate8_coverage"] == "full_checksum"
    assert chain.verify_chain()["verified"] is True
