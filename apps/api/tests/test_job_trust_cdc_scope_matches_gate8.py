"""QA2 — a CDC key-change job's Trust and Gate-8 tile read one verdict.

The CDC reconcile proves the source image by value fingerprints
(``checksum_scope='cdc_source_image_values'``); the signed proof pack files it
under that narrower claim, ``migration_proven=False``, and the Gate-8 tile says
"Unproven". ``job_trust.has_full_checksum_proof`` only compared the two digests,
so Trust on the same page said "Gate-8 full checksum reconcile passed".
"""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import pytest  # noqa: E402

from services.job_trust import compute_job_trust, has_full_checksum_proof  # noqa: E402
from services.signed_proof_pack import classify_post_write_assurance  # noqa: E402

CDC_KEY_CHANGE_RECON = {
    "passed": True,
    "phase": "post_write",
    "checksum_scope": "cdc_source_image_values",
    "source_checksum": "f1e2d3",
    "target_checksum": "f1e2d3",
    "source_rows": 4,
    "target_rows": 5,
    "message": "CDC source-image values present on destination (key change: old key retained).",
}


@pytest.mark.parametrize("assurance", [None, "full_checksum"])
def test_cdc_source_image_reconcile_is_not_a_full_checksum_on_trust(assurance):
    recon = dict(CDC_KEY_CHANGE_RECON)
    if assurance:
        recon["assurance_level"] = assurance
    claim = classify_post_write_assurance(recon)
    assert claim["claim_level"] == "cdc_source_image_values"
    assert claim["migration_proven"] is False

    assert has_full_checksum_proof(recon) is False
    trust = compute_job_trust({"status": "completed", "sync_mode": "cdc", "reconciliation": recon})
    reconcile = next(f for f in trust["factors"] if f["id"] == "reconcile")
    assert "full checksum reconcile passed" not in reconcile["note"].lower(), reconcile
    assert trust["grade"] != "A", trust


def test_whole_table_independent_reread_still_full_checksum():
    recon = {
        "passed": True,
        "assurance_level": "full_checksum",
        "source_checksum": "aa",
        "target_checksum": "aa",
        "source_checksum_provenance": "independent_source_reread",
    }
    assert has_full_checksum_proof(recon) is True
