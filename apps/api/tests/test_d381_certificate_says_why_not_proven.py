"""D381 (QA batch 1): the certificate said NOT PROVEN while the certificate, the
Gate-8 checksum (436e8dda28e6) and the job message agreed on 10 of 10 rows.

The verdict is correct by the rules: MIGRATION PROVEN needs a full checksum
whose *source* side came from an independent re-read of the source after the
write (``independent_source_reread``). A source digest computed from the rows
as Datawrap read them for the write (``write_pass_fingerprints``) agrees with
the destination, but a value the source read itself got wrong would be on both
sides. What was wrong: the certificate said "Reconciliation assurance is
'none' … per-cell fidelity is not claimed" — it never said what agreed or why
that is not a proof. It now says both, in plain words.
"""

from __future__ import annotations

from typing import Any

import pytest

from services.migration_certificate import build_migration_certificate
from services.signed_proof_pack import (
    classify_post_write_assurance,
    full_checksum_missing_reasons,
)

QA_SUM = "436e8dda28e6" + "0" * 52


def _job(provenance: str, **recon: Any) -> dict[str, Any]:
    return {
        "id": "b" * 24,
        "status": "completed",
        "records_processed": 10,
        "sync_mode": "full_refresh_overwrite",
        "source": {"format": "postgresql"},
        "destination": {"format": "snowflake"},
        "reconciliation": {
            "passed": True,
            "phase": "post_write_verified",
            "checksum_match": True,
            "source_rows": 10,
            "target_rows": 10,
            "rejected_rows": 0,
            "rows_skipped": 0,
            "source_checksum": QA_SUM,
            "target_checksum": QA_SUM,
            "source_checksum_provenance": provenance,
            "message": "Row fidelity verified — source and target checksums match (10 rows)",
            **recon,
        },
        "destination_summary": {"rejected_details": []},
    }


def test_write_pass_digest_is_not_proven_and_the_certificate_says_why() -> None:
    cert = build_migration_certificate(_job("write_pass_fingerprints"))
    verdict = cert["verdict"]
    assert verdict["migration_proven"] is False
    assert verdict["headline"] == "COMPLETED — NOT PROVEN"
    detail = verdict["detail"]
    assert "Source and destination checksums agree on 10 of 10 rows (checksum 436e8dda28e6)" in detail
    assert "Not proven because:" in detail
    assert "not from a second, independent read of the source after the write" in detail
    assert "Reconciliation assurance is" not in detail


def test_the_same_agreement_from_an_independent_reread_reaches_full_checksum() -> None:
    recon = _job("independent_source_reread")["reconciliation"]
    assert full_checksum_missing_reasons(recon) == []
    claim = classify_post_write_assurance(recon)
    assert claim["claim_level"] == "full_checksum" and claim["migration_proven"] is True


@pytest.mark.parametrize(
    "recon, expected",
    [
        ({"checksum_scope": "written_batch_keys"}, "covered written batch keys, not the whole table"),
        ({"coverage": "sample"}, None),
        ({"source_checksum_provenance": ""}, "no record says how the source checksum was obtained"),
        ({"source_checksum_provenance": "writer_ack"}, "checks the write against itself"),
    ],
)
def test_each_missing_condition_is_named(recon: dict[str, Any], expected: str | None) -> None:
    job = _job("write_pass_fingerprints", **recon)
    reasons = " ".join(full_checksum_missing_reasons(job["reconciliation"]))
    if expected:
        assert expected.lower() in reasons.lower(), reasons
    cert = build_migration_certificate(job)
    assert cert["verdict"]["migration_proven"] is False
    assert "Not proven because:" in cert["verdict"]["detail"]


def test_assurance_note_names_the_reason_not_a_generic_refusal() -> None:
    claim = classify_post_write_assurance(_job("write_pass_fingerprints")["reconciliation"])
    assert claim["claim_level"] == "none"
    assert claim["note"].startswith("Not proven: ")
    assert "lacks eligible whole-transfer scope" not in claim["note"]


def test_open_findings_lead_with_the_first_one() -> None:
    job = _job("independent_source_reread")
    job["reconciliation"]["target_rows"] = 8
    cert = build_migration_certificate(job)
    verdict = cert["verdict"]
    assert verdict["headline"] == "NOT PROVEN"
    assert verdict["detail"].startswith("Source and destination checksums agree")
    assert "Not proven because: " + verdict["blockers"][0].rstrip(".") in verdict["detail"]
