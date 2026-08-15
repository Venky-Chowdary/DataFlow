"""``full_checksum`` must mean two independent digests agreed.

A streaming pass hands no rows to reconciliation, so the only source digest
available is the writer's own account of what it wrote. Comparing that to a
read-back of the destination compares a write to itself, and it was being
reported as ``full_checksum`` — the label that claims independent agreement.
That is the ordinary path for large tables, which is exactly where the claim
carries the most weight.

Provenance now travels with the digest so the label can be earned rather than
assumed.
"""

from __future__ import annotations

from services.reconcile_coverage import (
    SOURCE_DIGEST_ENGINE_POPULATION,
    SOURCE_DIGEST_REMAPPED_ROWS,
    SOURCE_DIGEST_WRITER_ACK,
    is_writer_ack_only,
)
from services.reconciliation import stamp_post_write_phase


def _report(provenance: str) -> dict:
    return stamp_post_write_phase(
        {
            "passed": True,
            "message": "Reconciliation passed",
            "source_rows": 100,
            "target_rows": 100,
            "source_checksum": "abc123",
            "target_checksum": "abc123",
            "checksum_match": True,
            "source_checksum_provenance": provenance,
        }
    )


def test_writer_ack_provenance_cannot_claim_full_checksum():
    report = _report(SOURCE_DIGEST_WRITER_ACK)
    assert report["assurance_level"] == "writer_ack"
    assert report["coverage"] == "writer_ack"
    assert report["phase"] == "post_write_writer_ack"


def test_write_pass_fingerprint_is_not_full_checksum():
    from services.reconcile_coverage import SOURCE_DIGEST_WRITE_PASS

    report = _report(SOURCE_DIGEST_WRITE_PASS)
    assert report["assurance_level"] == "write_pass_dest_readback"
    assert report["coverage"] == "write_pass_dest_readback"
    assert report.get("migration_proven") is False
    assert report["phase"] == "post_write_write_pass"


def test_buffered_remapped_rows_and_engine_population_earn_full_checksum():
    for provenance in (SOURCE_DIGEST_REMAPPED_ROWS, SOURCE_DIGEST_ENGINE_POPULATION):
        report = _report(provenance)
        assert report["assurance_level"] == "full_checksum"
        assert report["phase"] == "post_write_verified"


def test_provenance_beats_the_message_text():
    """The caller knows where the digest came from; the message can only guess."""
    assert is_writer_ack_only(
        "Reconciliation passed", "target-digest",
        source_provenance=SOURCE_DIGEST_WRITER_ACK,
    )
    assert not is_writer_ack_only(
        "Reconciliation passed", "target-digest",
        source_provenance=SOURCE_DIGEST_REMAPPED_ROWS,
    )


def test_absent_provenance_keeps_the_previous_message_heuristic():
    """Callers that do not report provenance must behave exactly as before."""
    assert is_writer_ack_only("verified by writer", "target-digest")
    assert not is_writer_ack_only("Reconciliation passed", "target-digest")
    assert is_writer_ack_only("Reconciliation passed", "")


def test_streaming_transfer_reports_writer_ack(monkeypatch):
    """The shape that mattered: no records supplied, so no independent digest."""
    from src.transfer.reconcile_step import _compute_source_checksum

    digest, provenance = _compute_source_checksum(
        [],
        ["id"],
        [{"source": "id", "target": "id"}],
        {"id": "BIGINT"},
        "writer-digest-abc",
    )
    assert digest == "writer-digest-abc"
    assert provenance == SOURCE_DIGEST_WRITER_ACK


def test_buffered_transfer_reports_remapped_rows():
    from src.transfer.reconcile_step import _compute_source_checksum

    digest, provenance = _compute_source_checksum(
        [{"id": 1}, {"id": 2}],
        ["id"],
        [{"source": "id", "target": "id"}],
        {"id": "BIGINT"},
        "writer-digest-abc",
    )
    assert digest != "writer-digest-abc"
    assert provenance == SOURCE_DIGEST_REMAPPED_ROWS
