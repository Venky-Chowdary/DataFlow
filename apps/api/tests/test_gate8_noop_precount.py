"""Incremental no-op Gate-8 must not invent 'destination unchanged' without precount."""

from __future__ import annotations

from unittest.mock import patch

from services.dest_precount import PRECOUNT_KEY
from services.job_trust import has_full_checksum_proof
from services.reconciliation import stamp_post_write_phase
from src.transfer.models import EndpointConfig
from src.transfer.reconcile_step import run_reconciliation


def _noop_report(*, dest_summary: dict, dest_rows: int = 10, dest_digest: str = "abc"):
    endpoint = EndpointConfig(
        kind="database", format="postgresql", database="db", table="t"
    )
    with patch("src.transfer.reconcile_step.verify_target", return_value=(dest_rows, dest_digest)):
        with patch("src.transfer.reconcile_step.resolve_connector_config", return_value={}):
            return run_reconciliation(
                endpoint=endpoint,
                records=[],
                columns=["id"],
                rows_written=0,
                writer_checksum=dest_digest,
                dest_summary=dest_summary,
                validation_mode="balanced",
            )


def test_incremental_noop_without_precount_is_unproven() -> None:
    report = _noop_report(
        dest_summary={
            "source_row_count": 0,
            "source_row_count_source": "reader_count",
            "sync_mode": "incremental_append",
        }
    )
    assert report["passed"] is False
    assert report.get("unproven") is True
    assert report.get("migration_proven") is False
    assert report.get("assurance_level") == "none"
    assert "pre-write" in str(report.get("message") or "").lower()


def test_incremental_noop_with_precount_is_operational_not_full_checksum() -> None:
    report = _noop_report(
        dest_summary={
            "source_row_count": 0,
            "source_row_count_source": "reader_count",
            "sync_mode": "incremental_append",
            PRECOUNT_KEY: 10,
        }
    )
    assert report["passed"] is True
    assert report.get("assurance_level") == "no_op_destination_unchanged"
    assert report.get("migration_proven") is False
    assert report.get("phase") == "post_write_no_op"
    assert not has_full_checksum_proof(report)


def test_noop_matching_digests_cannot_upgrade_to_full_checksum() -> None:
    stamped = stamp_post_write_phase(
        {
            "passed": True,
            "assurance_level": "no_op_destination_unchanged",
            "source_checksum": "same",
            "target_checksum": "same",
            "message": "No new source rows",
        }
    )
    assert stamped["assurance_level"] == "no_op_destination_unchanged"
    assert stamped.get("migration_proven") is False
    assert stamped["phase"] == "post_write_no_op"
    assert not has_full_checksum_proof(stamped)
