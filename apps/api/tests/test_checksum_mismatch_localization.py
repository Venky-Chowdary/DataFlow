"""Two hashes are not a diagnosis.

The 710k-row append that failed Gate-8 reported a source hash, a target hash,
and a clean 500-row key-aligned sample — nothing an operator could act on. When
no differing cell can be found on a conserved population, the disagreement is in
how or over what the digests were taken, and the run must say so and stamp the
basis. The verdict does not move: the run still fails.
"""

from __future__ import annotations

from src.transfer.reconcile_step import _localize_checksum_mismatch


def _mismatch_report() -> dict[str, object]:
    return {
        "checksum_match": False,
        "message": "Checksum mismatch (strict): source aaa vs target bbb.",
        # ``compared`` is a cell count; the row count is reported separately.
        "sample_compare": {"passed": True, "compared": 500, "rows_compared": 50},
        "source_checksum_provenance": "independent_source_reread",
    }


def test_clean_sample_on_a_mismatch_is_classified_and_still_fails() -> None:
    out = _localize_checksum_mismatch(
        _mismatch_report(), {"checksum_mode": "source_reread"}
    )
    assert out["checksum_match"] is False
    assert out["mismatch_class"] == "comparison_basis_or_population_scope"
    assert out["digest_basis"]["source_digest"] == "independent_source_reread"
    assert out["digest_basis"]["keyed_sample_rows_without_mismatch"] == 50
    assert out["digest_basis"]["keyed_sample_cells_without_mismatch"] == 500
    assert "50 row(s) / 500 cell(s)" in str(out["message"])


def test_a_failing_sample_keeps_its_own_cell_level_evidence() -> None:
    """A sample that found a differing value is already localized — do not relabel."""
    report = _mismatch_report()
    report["sample_compare"] = {"passed": False, "compared": 500}
    out = _localize_checksum_mismatch(report, {})
    assert "mismatch_class" not in out
    assert "digest_basis" not in out


def test_a_matching_run_is_never_annotated() -> None:
    out = _localize_checksum_mismatch(
        {"checksum_match": True, "sample_compare": {"passed": True, "compared": 500}},
        {},
    )
    assert "mismatch_class" not in out
