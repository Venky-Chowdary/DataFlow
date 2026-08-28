"""Migration Certificate: row conservation, quarantine reporting, signature."""

from __future__ import annotations

from typing import Any

import pytest

from services.migration_certificate import (
    RowAccountingError,
    build_migration_certificate,
    quarantine_reason_breakdown,
    render_certificate_markdown,
    row_accounting,
    verify_migration_certificate,
)

JOB_ID = "a" * 24


def _job(**overrides: Any) -> dict[str, Any]:
    job: dict[str, Any] = {
        "id": JOB_ID,
        "status": "completed",
        "records_processed": 9000,
        "sync_mode": "overwrite",
        "source": {"format": "postgresql"},
        "destination": {"format": "mysql"},
        "reconciliation": {
            "passed": True,
            "phase": "post_write_verified",
            "assurance_level": "full_checksum",
            "checksum_match": True,
            "source_rows": 10000,
            "target_rows": 9000,
            "rejected_rows": 1000,
            "rows_skipped": 0,
            "source_checksum": "abc",
            "target_checksum": "abc",
            "message": "Verified",
        },
        "destination_summary": {"rejected_details": []},
    }
    job.update(overrides)
    return job


def test_certificate_requires_a_job_id() -> None:
    with pytest.raises(RowAccountingError):
        build_migration_certificate({"status": "completed"})


def test_row_accounting_balances_when_every_row_is_explained() -> None:
    ledger = row_accounting(_job())
    assert ledger["rows_read"] == 10000
    assert ledger["rows_written"] == 9000
    assert ledger["rows_written_source"] == "gate8_dest_readback"
    assert ledger["rows_quarantined"] == 1000
    assert ledger["unaccounted"] == 0
    assert ledger["balanced"] is True
    assert ledger["writer_ack"] == 9000


def test_unexplained_shortfall_is_reported_as_potential_silent_loss() -> None:
    job = _job()
    job["reconciliation"]["target_rows"] = 8500
    ledger = row_accounting(job)
    assert ledger["rows_written"] == 8500
    assert ledger["unaccounted"] == 500
    assert ledger["balanced"] is False
    assert "silent loss" in ledger["note"]


def test_writer_ack_cannot_hide_a_dest_count_shortfall() -> None:
    """DMS Full Load success + MISSING_TARGET: writer claimed 10k, dest holds 9997."""
    job = _job(records_processed=10000)
    job["reconciliation"]["source_rows"] = 10000
    job["reconciliation"]["target_rows"] = 9997
    job["reconciliation"]["rejected_rows"] = 0
    ledger = row_accounting(job)
    assert ledger["rows_written"] == 9997
    assert ledger["writer_ack"] == 10000
    assert ledger["unaccounted"] == 3
    assert ledger["balanced"] is False
    assert ledger["writer_ack_delta"] == -3


def test_more_rows_accounted_than_read_is_also_unbalanced() -> None:
    job = _job()
    job["reconciliation"]["target_rows"] = 9500
    ledger = row_accounting(job)
    assert ledger["unaccounted"] == -500
    assert ledger["balanced"] is False
    assert "more row" in ledger["note"]


def test_unmeasured_source_count_never_claims_conservation() -> None:
    job = _job()
    job["reconciliation"].pop("source_rows")
    ledger = row_accounting(job)
    assert ledger["rows_read"] is None
    assert ledger["balanced"] is False
    assert ledger["unaccounted"] is None
    assert ledger["rows_read_source"] == "unmeasured"


def test_unbalanced_ledger_blocks_the_proven_claim() -> None:
    job = _job()
    job["reconciliation"]["target_rows"] = 8500
    cert = build_migration_certificate(job)
    verdict = cert["verdict"]
    assert verdict["migration_proven"] is False
    assert verdict["headline"] == "NOT PROVEN"
    assert any("silent loss" in b for b in verdict["blockers"])


def test_sample_assurance_completes_without_claiming_proof() -> None:
    job = _job()
    job["reconciliation"]["assurance_level"] = "sample"
    job["reconciliation"]["checksum_match"] = False
    job["reconciliation"]["source_checksum"] = ""
    job["reconciliation"]["target_checksum"] = ""
    cert = build_migration_certificate(job)
    assert cert["verdict"]["migration_proven"] is False
    assert cert["verdict"]["headline"] != "MIGRATION PROVEN"


def test_failed_job_is_never_proven() -> None:
    cert = build_migration_certificate(_job(status="failed"))
    assert cert["verdict"]["migration_proven"] is False
    assert any("not a completed run" in b for b in cert["verdict"]["blockers"])


def test_quarantine_reasons_are_grouped_with_their_columns() -> None:
    rows = [
        {"reason": "Invalid integer", "column": "year"},
        {"reason": "Invalid integer", "column": "qty"},
        {"reason": "Invalid integer", "column": "year"},
        {"reason": "Out of range", "column": "ts"},
        "not-a-dict",
    ]
    breakdown = quarantine_reason_breakdown(rows)
    assert breakdown[0] == {
        "reason": "Invalid integer",
        "rows": 3,
        "columns": ["qty", "year"],
    }
    assert breakdown[1]["rows"] == 1


def test_signature_verifies_and_tampering_is_detected() -> None:
    cert = build_migration_certificate(_job())
    assert verify_migration_certificate(cert)["ok"] is True

    tampered = {**cert}
    tampered["row_accounting"] = {**cert["row_accounting"], "rows_written": 10000}
    result = verify_migration_certificate(tampered)
    assert result["ok"] is False
    assert "content_sha256 mismatch" in result["errors"]


def test_verify_rejects_a_forged_proven_claim() -> None:
    """Re-signing a forged verdict must still fail on the claim rules."""
    from services.signed_proof_pack import sign_body

    job = _job()
    job["reconciliation"]["target_rows"] = 8500
    cert = build_migration_certificate(job)
    body = {k: v for k, v in cert.items() if k not in ("content_sha256", "signature")}
    body["verdict"] = {
        **body["verdict"],
        "migration_proven": True,
        "assurance_level": "full_checksum",
        "blockers": [],
    }
    forged = sign_body(body, subject=JOB_ID)
    result = verify_migration_certificate(forged)
    assert result["ok"] is False
    assert "migration_proven claimed while row accounting is unbalanced" in result["errors"]


def test_markdown_renders_the_numbers_an_operator_reads() -> None:
    job = _job(
        destination_summary={
            "rejected_details": [
                {"reason": "Invalid integer", "column": "year"} for _ in range(1000)
            ]
        }
    )
    text = render_certificate_markdown(build_migration_certificate(job))
    assert "# Migration Certificate" in text
    assert "| Read from source | 10,000 |" in text
    assert "| On destination (COUNT(*)) | 9,000 |" in text
    assert "| Quarantined (did not land) | 1,000 |" in text
    assert "gate8_dest_readback" in text
    assert "Invalid integer" in text
    assert "Not proven by this certificate" in text


def test_markdown_states_unmeasured_rather_than_zero() -> None:
    job = _job()
    job["reconciliation"].pop("source_rows")
    text = render_certificate_markdown(build_migration_certificate(job))
    assert "| Read from source | unmeasured |" in text


def test_truncated_job_doc_hydrates_reasons_from_the_dlq(monkeypatch) -> None:
    """A 10-row embedded sample must not be reported as the whole 1,000."""
    from services import quarantine_dlq

    monkeypatch.setattr(
        quarantine_dlq,
        "quarantine_details_from_dlq",
        lambda job_id, **kw: [
            {"reason": "Invalid integer", "column": "year"} for _ in range(1000)
        ],
    )
    job = _job(
        destination_summary={
            "rejected_details": [{"reason": "Invalid integer", "column": "year"}] * 10
        }
    )
    cert = build_migration_certificate(job)
    assert cert["quarantine"]["detail_rows_available"] == 1000
    assert cert["quarantine"]["by_reason"][0]["rows"] == 1000


def test_partial_reason_coverage_is_disclosed_not_hidden(monkeypatch) -> None:
    from services import quarantine_dlq

    monkeypatch.setattr(
        quarantine_dlq,
        "quarantine_details_from_dlq",
        lambda job_id, **kw: [{"reason": "Invalid integer", "column": "year"}] * 40,
    )
    job = _job(destination_summary={"rejected_details": []})
    text = render_certificate_markdown(build_migration_certificate(job))
    assert "covers 40 of 1,000 rows" in text


def test_unreadable_dlq_falls_back_to_embedded_rows(monkeypatch) -> None:
    from services import quarantine_dlq

    def _boom(job_id, **kw):
        raise RuntimeError("mongo down")

    monkeypatch.setattr(quarantine_dlq, "quarantine_details_from_dlq", _boom)
    job = _job(
        destination_summary={
            "rejected_details": [{"reason": "Invalid integer", "column": "year"}] * 3
        }
    )
    cert = build_migration_certificate(job)
    assert cert["quarantine"]["detail_rows_available"] == 3


def _call_certificate_endpoint(job: dict[str, Any] | None, **kwargs: Any) -> Any:
    import asyncio
    from unittest.mock import MagicMock, patch

    from src.routers.transfer_router import get_migration_certificate

    mongo = MagicMock()
    mongo.get_job.return_value = job
    with patch(
        "src.services.mongodb_service.get_mongodb_service", return_value=mongo
    ), patch(
        "src.routers.transfer_router._can_access_job",
        return_value=kwargs.pop("can_access", True),
    ), patch("services.audit_log.append_audit_event"), patch(
        "services.audit_log.actor_from_request", return_value="ops@example.com"
    ):
        return asyncio.run(get_migration_certificate(JOB_ID, MagicMock(), **kwargs))


def test_endpoint_returns_signed_certificate_and_markdown() -> None:
    cert = _call_certificate_endpoint(_job())
    assert verify_migration_certificate(cert)["ok"] is True

    response = _call_certificate_endpoint(_job(), format="markdown")
    assert response.media_type == "text/markdown"
    assert b"# Migration Certificate" in response.body


def test_endpoint_is_fail_closed_for_foreign_workspaces() -> None:
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as excinfo:
        _call_certificate_endpoint(_job(), can_access=False)
    assert excinfo.value.status_code == 404

    with pytest.raises(HTTPException) as missing:
        _call_certificate_endpoint(None)
    assert missing.value.status_code == 404


def _proven_job(**overrides: Any) -> dict[str, Any]:
    """A run whose rows reconcile — only the structure around them varies."""
    job = _job(records_processed=10000)
    job["reconciliation"]["target_rows"] = 10000
    job["reconciliation"]["rejected_rows"] = 0
    job["reconciliation"]["population_proof"] = True
    job.update(overrides)
    return job


def test_absent_destination_constraints_block_the_proven_claim() -> None:
    """Matching checksums prove rows, not the database around them."""
    job = _proven_job()
    job["reconciliation"]["physical_state"] = {
        "schema_objects": {
            "verified": False,
            "absent": ["foreign_keys", "check_constraints"],
            "aspects": {},
        }
    }
    verdict = build_migration_certificate(job)["verdict"]
    assert verdict["migration_proven"] is False
    assert verdict["headline"] == "NOT PROVEN"
    assert any("foreign key(s)" in b and "CHECK" in b for b in verdict["blockers"])


def test_unreadable_constraint_catalog_is_unknown_not_a_violation() -> None:
    """Unknown must never be reported as absent."""
    job = _proven_job()
    job["reconciliation"]["physical_state"] = {
        "schema_objects": {
            "verified": False,
            "absent": [],
            "unreadable": ["indexes"],
            "reason": "destination catalog unreadable",
        }
    }
    verdict = build_migration_certificate(job)["verdict"]
    assert not any("did not survive" in b for b in verdict["blockers"])


def test_failed_foreign_key_carry_vetoes_the_verdict() -> None:
    job = _proven_job()
    job["destination_summary"] = {
        "rejected_details": [],
        "foreign_keys": {
            "counts": {"carried": 2, "failed": 1},
            "integrity_violations": 1,
            "verdict": "referential_integrity_violated",
        },
    }
    verdict = build_migration_certificate(job)["verdict"]
    assert verdict["migration_proven"] is False
    assert any("child rows" in b and "without a parent" in b for b in verdict["blockers"])
    assert any("did not complete" in b and "1 failed" in b for b in verdict["blockers"])


def test_foreign_key_cycle_is_named_as_a_blocker() -> None:
    job = _proven_job()
    job["destination_summary"] = {
        "rejected_details": [],
        "foreign_keys": {
            "counts": {"carried": 2},
            "integrity_violations": 0,
            "cycle": ["orders", "customers"],
        },
    }
    verdict = build_migration_certificate(job)["verdict"]
    assert verdict["migration_proven"] is False
    assert any("cycle" in b and "orders" in b for b in verdict["blockers"])


def test_fully_carried_foreign_keys_do_not_block() -> None:
    job = _proven_job()
    job["destination_summary"] = {
        "rejected_details": [],
        "foreign_keys": {"counts": {"carried": 3}, "integrity_violations": 0},
    }
    verdict = build_migration_certificate(job)["verdict"]
    assert not any("foreign key" in b.lower() for b in verdict["blockers"])


def _shaped_job() -> dict[str, Any]:
    """A run where a source filter and an approved recipe removed rows."""
    job = _job(records_processed=2)
    job["reconciliation"].update(
        {
            "source_rows": 5,
            "target_rows": 2,
            "rejected_rows": 0,
            "rows_shaped_out": 2,
            "rows_source_filtered": 1,
            "shape_recipe_hash": "abc123def4567890",
        }
    )
    return job


def test_rows_a_recipe_removed_close_the_certificate_ledger() -> None:
    ledger = row_accounting(_shaped_job())
    assert ledger["rows_read"] == 5
    assert ledger["rows_written"] == 2
    assert ledger["rows_shaped_out"] == 2
    assert ledger["rows_source_filtered"] == 1
    assert ledger["shape_recipe_hash"] == "abc123def4567890"
    assert ledger["unaccounted"] == 0
    assert ledger["balanced"] is True


def test_certificate_page_names_each_removal_authority_and_the_recipe() -> None:
    md = render_certificate_markdown(build_migration_certificate(_shaped_job()))
    assert "| Removed by the declared source filter | 1 |" in md
    assert "| Removed by the approved transform recipe | 2 |" in md
    assert "abc123def4567890" in md
    assert "by instruction, not by loss or quarantine" in md


def test_certificate_names_views_and_triggers_to_recreate_without_blocking() -> None:
    job = _proven_job()
    job["reconciliation"]["physical_state"] = {
        "schema_objects": {
            "verified": True,
            "absent": [],
            "aspects": {
                "views": {
                    "status": "absent",
                    "missing": ["v_orders_open"],
                    "advisory": True,
                    "note": "Dependent views are not created by table transfer.",
                },
                "triggers": {
                    "status": "absent",
                    "missing": ["trg_audit (after insert)"],
                    "advisory": True,
                    "note": "Trigger bodies are not migrated.",
                },
            },
            "cutover_recreate": [
                {
                    "kind": "view",
                    "name": "v_orders_open",
                    "action": "recreate_before_cutover",
                },
                {
                    "kind": "trigger",
                    "name": "trg_audit (after insert)",
                    "action": "recreate_before_cutover",
                },
            ],
        }
    }
    cert = build_migration_certificate(job)
    md = render_certificate_markdown(cert)
    assert "Recreate before cutover" in md
    assert "v_orders_open" in md
    assert "trg_audit" in md
    assert not any("view" in b.lower() or "trigger" in b.lower() for b in cert["verdict"]["blockers"])


def test_certificate_page_of_a_plain_run_states_no_removals() -> None:
    md = render_certificate_markdown(build_migration_certificate(_job()))
    assert "Removed by" not in md
    assert "shaping recipe" not in md
