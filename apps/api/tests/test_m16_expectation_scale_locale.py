"""M16 — preflight blocker wording and decimal-scale honesty.

1. g9 expectation-suite findings (``expect_column_not_null:order_id``) are
   data-quality results, not a type-path collapse — the root-cause engine must
   not absorb them into ``fidelity_collapse``.
2. TEXT → NUMERIC(15,7) with samples carrying 9 decimals: the writer
   quarantines (never rounds silently at INSERT), so the coercion probe must
   not report those cells as coercing cleanly.
3. JSON meta columns (dict cells, JSON-doc strings) are not locale numbers —
   no ``number_locale_assumed`` warning.

Every test fails on c6c16128.
"""
from __future__ import annotations


def test_g9_expectation_failure_is_not_fidelity_collapse():
    from services.root_cause_engine import _is_fidelity_signal, build_root_causes

    message = "Data integrity failed: expect_column_not_null:order_id: 3 failures"
    details = {"issues": ["expect_column_not_null:order_id: 3 failures"]}
    assert not _is_fidelity_signal(message, details, "g9_data_integrity")

    preflight = {
        "gates": [{
            "id": "g9_data_integrity",
            "status": "block",
            "message": message,
            "details": details,
        }],
        "blockers": [],
        "column_types": {"order_id": "TEXT", "amount": "TEXT"},
    }
    roots = build_root_causes(preflight)
    assert not any(r.kind == "fidelity_collapse" for r in roots), (
        f"expectation block must not be labeled fidelity collapse: "
        f"{[(r.kind, r.title) for r in roots]}"
    )


def test_g9_real_collapse_issues_still_absorb():
    """The expectation guard must not swallow a true type-path collapse."""
    from services.root_cause_engine import _is_fidelity_signal

    message = "Data integrity failed: DECIMAL(38,10) → DECIMAL(10,2) collapse"
    details = {"issues": ["DECIMAL(38,10) → DECIMAL(10,2)"]}
    assert _is_fidelity_signal(message, details, "g9_data_integrity")


def test_probe_flags_declared_scale_overflow_not_clean_coerce():
    from services.coercion_probe import analyze_coercion

    rep = analyze_coercion(
        sample_rows=[
            {"amount": "0.123456789"},
            {"amount": "9.999999991"},
        ],
        mappings=[{"source": "amount", "target": "amount"}],
        source_types={"amount": "TEXT"},
        dest_types={"amount": "NUMERIC(15,7)"},
        dest_db_type="postgresql",
        validation_mode="balanced",
    )
    col = rep["columns"][0]
    assert col["failed"] == 2, (
        "scale-9 values into NUMERIC(15,7) must not count as coercing — "
        "the writer quarantines them"
    )
    reasons = " ".join(f["reason"] for f in col["sample_failures"])
    assert "declared scale" in reasons or "NUMERIC(15,7)" in reasons


def test_probe_still_oks_values_inside_declared_scale():
    from services.coercion_probe import analyze_coercion

    rep = analyze_coercion(
        sample_rows=[{"amount": "0.1234567"}, {"amount": "9.99"}],
        mappings=[{"source": "amount", "target": "amount"}],
        source_types={"amount": "TEXT"},
        dest_types={"amount": "NUMERIC(15,7)"},
        dest_db_type="postgresql",
        validation_mode="balanced",
    )
    col = rep["columns"][0]
    assert col["failed"] == 0 and col["ok"] == 2


def test_json_meta_column_never_locale_assumed():
    from services.transform_engine import (
        ambiguous_number_columns,
        assumed_number_locale,
    )

    rows = [
        {"meta": {"k": 1, "ts": "2026-01-01"}, "amount": "1234.56"},
        {"meta": {"k": 2, "v": "x"}, "amount": "99.9"},
    ]
    assert ambiguous_number_columns(rows) == []
    assert assumed_number_locale(rows) == ""

    rows2 = [
        {"meta": '{"k": 1, "v": "a,b"}'},
        {"meta": '{"k": 2, "v": "c,d"}'},
    ]
    assert ambiguous_number_columns(rows2) == []
