"""Audit PDF: a faithful, reproducible rendering of the signed certificate."""

from __future__ import annotations

from io import BytesIO
from typing import Any

import pytest
from pypdf import PdfReader

from services.certificate_pdf import render_certificate_pdf
from services.migration_certificate import build_migration_certificate

pytest.importorskip("reportlab")

JOB_ID = "b" * 24


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
            "physical_state": {
                "schema_objects": {
                    "verified": False,
                    "absent": ["check_constraints"],
                    "aspects": {
                        "check_constraints": {
                            "status": "absent",
                            "missing": ["qty>0"],
                        },
                        "triggers": {
                            "status": "absent",
                            "missing": ["before insert"],
                            "advisory": True,
                            "note": "Trigger bodies are not migrated.",
                        },
                    },
                }
            },
        },
        "destination_summary": {
            "rejected_details": [
                {"reason": "Invalid integer", "column": "qty", "row": 7}
            ]
        },
    }
    job.update(overrides)
    return job


def _text(pdf: bytes) -> str:
    return "\n".join(
        page.extract_text() or "" for page in PdfReader(BytesIO(pdf)).pages
    )


def test_pdf_carries_the_verdict_ledger_and_signature() -> None:
    cert = build_migration_certificate(_job())
    text = _text(render_certificate_pdf(cert))
    assert "Migration Certificate" in text
    assert cert["verdict"]["headline"] in text
    # The ledger an auditor reads: read = written + quarantined + skipped.
    for figure in ("10,000", "9,000", "1,000"):
        assert figure in text
    assert cert["content_sha256"][:16] in text


def test_pdf_shows_blocking_and_advisory_physical_state() -> None:
    text = _text(render_certificate_pdf(build_migration_certificate(_job())))
    assert "qty>0" in text
    assert "advisory" in text.lower()


def test_pdf_never_prints_zero_for_an_unmeasured_count() -> None:
    job = _job()
    job["reconciliation"] = {**job["reconciliation"], "source_rows": None}
    job["reconciliation"].pop("source_rows")
    text = _text(render_certificate_pdf(build_migration_certificate(job)))
    assert "unmeasured" in text


def test_same_certificate_renders_byte_identical() -> None:
    """An auditor must be able to re-render and diff the deliverable."""
    cert = build_migration_certificate(_job())
    assert render_certificate_pdf(cert) == render_certificate_pdf(cert)


def test_an_empty_certificate_is_refused_not_rendered_blank() -> None:
    with pytest.raises(ValueError):
        render_certificate_pdf({})
