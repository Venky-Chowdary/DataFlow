"""Module 4 — Validation coverage honesty (sample ≠ population).

Enterprise promise: every validation surface must stamp layer + population_proof.
Gate-8 sample authority must never hide checksum mismatch as full fidelity.
FK orphan probe must fail closed when it cannot run against known FKs.
"""

from __future__ import annotations

from unittest.mock import patch

from services.reconciliation import reconcile, stamp_post_write_phase
from services.sample_orphan_probe import probe_sample_fk_orphans
from services.validation_coverage import (
    VALIDATION_LAYERS,
    assert_no_sample_population_lie,
    stamp_validation_coverage,
)


def test_coverage_layers_are_explicit():
    assert "schema" in VALIDATION_LAYERS
    assert "datatype" in VALIDATION_LAYERS
    assert "sample" in VALIDATION_LAYERS
    assert "population" in VALIDATION_LAYERS
    assert "execution" in VALIDATION_LAYERS
    assert "post_write" in VALIDATION_LAYERS


def test_sample_stamp_never_sets_population_proof():
    stamp = stamp_validation_coverage(
        layer="sample",
        rows_examined=50,
        estimated_population=1_000_000,
    )
    assert stamp["layer"] == "sample"
    assert stamp["population_proof"] is False
    assert "not" in stamp["non_guarantees"][0].lower() or "population" in " ".join(
        stamp["non_guarantees"]
    ).lower()
    assert_no_sample_population_lie(stamp)


def test_population_proof_requires_population_layer():
    """Cannot invent population_proof=True on a sample stamp."""
    try:
        stamp_validation_coverage(layer="sample", population_proof=True)
        raised = False
    except ValueError:
        raised = True
    assert raised is True


def test_gate8_checksum_mismatch_sample_never_claims_population():
    """P0/GA: checksum mismatch always fails — sample is diagnostic only."""
    r = reconcile(
        source_rows=10,
        target_rows=10,
        source_checksum="abc",
        target_checksum="xyz",
        strict_checksum=False,
        sample_compare={"passed": True, "compared": 5, "mismatches": []},
    )
    d = r.to_dict()
    assert d.get("passed") is False
    assert d.get("checksum_match") is False
    assert d.get("population_proof") is False
    assert d.get("assurance_level") == "none"
    msg = (d.get("message") or "").lower()
    assert "checksum mismatch" in msg
    assert "cannot override" in msg or "diagnostic" in msg
    assert "row fidelity verified" not in msg
    # Sample diagnostics remain attached for operators.
    assert (d.get("sample_compare") or {}).get("compared") == 5
    assert d.get("phase") == "post_write_failed"


def test_gate8_matching_checksum_is_full_checksum_not_sample_lie():
    r = reconcile(
        source_rows=10,
        target_rows=10,
        source_checksum="abc",
        target_checksum="abc",
    )
    d = r.to_dict()
    assert d.get("checksum_match") is True
    assert d.get("population_proof") is False  # checksum ≠ RI / constraint proof
    assert d.get("coverage") == "full_checksum"
    assert d.get("assurance_level") == "full_checksum"


def test_stamp_phase_preserves_checksum_honesty_fields():
    """Diverging digests force failed — sample cannot soft-verify."""
    out = stamp_post_write_phase(
        {
            "passed": True,
            "source_checksum": "aaa",
            "target_checksum": "bbb",
            "message": "Transfer completed successfully",
            "sample_compare": {"passed": True, "compared": 3, "mismatches": []},
            "checksum_match": False,
            "population_proof": False,
            "assurance_level": "sample",
        }
    )
    assert out["passed"] is False
    assert out["phase"] == "post_write_failed"
    assert out["coverage"] == "none"
    assert out["checksum_match"] is False
    assert out["population_proof"] is False
    assert out["assurance_level"] == "none"


def test_fk_probe_unavailable_emits_fail_closed_finding():
    """Known FKs + no connector must not silently soft-pass orphan detection."""
    report = probe_sample_fk_orphans(
        sample_rows=[{"customer_id": 1}],
        mappings=[{"source": "customer_id", "target": "customer_id"}],
        foreign_keys=[
            {
                "columns": ["customer_id"],
                "referenced_table": "customers",
                "referenced_columns": ["id"],
            }
        ],
        validation_mode="strict",
        fk_risk_acknowledged=False,
    )
    assert report["ran"] is False
    assert report["population_proof"] is False
    findings = report.get("findings") or []
    assert findings, "must surface fk_orphan_probe_unavailable"
    assert findings[0]["code"] == "fk_orphan_probe_unavailable"
    assert findings[0]["severity"] == "block"
    assert findings[0]["population_proof"] is False


def test_composite_fk_sample_probe_scans_the_tuple():
    with patch(
        "services.sample_orphan_probe._sql_existing_parent_tuples",
        return_value=[(1, 2)],
    ) as lookup:
        report = probe_sample_fk_orphans(
            sample_rows=[{"a": 1, "b": 2}, {"a": 9, "b": 9}],
            mappings=[
                {"source": "a", "target": "a"},
                {"source": "b", "target": "b"},
            ],
            foreign_keys=[
                {
                    "columns": ["a", "b"],
                    "referenced_table": "parents",
                    "referenced_columns": ["a", "b"],
                }
            ],
            source_config={"type": "postgresql", "host": "localhost", "database": "t"},
            validation_mode="strict",
            fk_risk_acknowledged=False,
        )
    assert lookup.called
    assert report["population_proof"] is False
    assert report["orphan_count"] == 1
    codes = [f.get("code") for f in (report.get("findings") or [])]
    assert "fk_orphan_in_sample" in codes
    assert "composite_fk_not_probed" not in codes
    finding = next(f for f in report["findings"] if f["code"] == "fk_orphan_in_sample")
    assert finding["severity"] == "block"
    assert finding["coverage"] == "sample_orphan_probe"


def test_composite_fk_arity_mismatch_still_not_probed():
    report = probe_sample_fk_orphans(
        sample_rows=[{"a": 1, "b": 2}],
        mappings=[{"source": "a", "target": "a"}, {"source": "b", "target": "b"}],
        foreign_keys=[
            {
                "columns": ["a", "b"],
                "referenced_table": "parents",
                "referenced_columns": ["id"],
            }
        ],
        source_config={"type": "postgresql", "host": "localhost", "database": "t"},
        validation_mode="strict",
        fk_risk_acknowledged=False,
    )
    codes = [f.get("code") for f in (report.get("findings") or [])]
    assert "composite_fk_not_probed" in codes
