"""Wave 54 — mapping engine superiority: stems, create-new risk, engine stamp."""

from __future__ import annotations

from services.semantic_mapper import (
    _entity_agreement,
    _light_stem,
    _qualifier_stems_overlap,
    ml_baseline_status,
)
from services.type_system import assess_create_new_type_risk


def test_light_stem_irregulars():
    assert _light_stem("paid") == _light_stem("payment") == "pay"
    assert _light_stem("shipping") == _light_stem("shipped") == "ship"
    assert _light_stem("created") == _light_stem("creation") == "create"


def test_qualifier_stems_overlap_paid_payment():
    assert _qualifier_stems_overlap({"paid"}, {"payment"})
    assert _qualifier_stems_overlap({"ship"}, {"shipping"})
    assert _entity_agreement("paid_amount", "payment_amount") > 0.5


def test_ml_baseline_status_is_honest():
    status = ml_baseline_status()
    assert "available" in status
    assert "note" in status
    assert status["role"] == "optional_boost"


def test_create_new_varchar_width_cap_oracle():
    risks = assess_create_new_type_risk(
        "VARCHAR(5000)",
        "VARCHAR2(4000)",
        destination_db_type="oracle",
    )
    kinds = {r["kind"] for r in risks}
    assert "varchar_width_cap" in kinds or "varchar_narrow" in kinds or "lossy_coercion" in kinds or "precision_collapse" in kinds
    assert any(r.get("severity") == "block" for r in risks)


def test_create_new_clob_clears_varchar_width_cap():
    """Honest unbounded create-new must not keep a false width-cap chip."""
    risks = assess_create_new_type_risk(
        "VARCHAR(5000)",
        "CLOB",
        destination_db_type="oracle",
    )
    kinds = {r["kind"] for r in risks}
    assert "varchar_width_cap" not in kinds
    assert "varchar_narrow" not in kinds


def test_create_new_timezone_polarity_risk():
    risks = assess_create_new_type_risk(
        "TIMESTAMPTZ",
        "TIMESTAMP",
        destination_db_type="postgresql",
    )
    kinds = {r["kind"] for r in risks}
    assert "timezone_polarity" in kinds or "precision_collapse" in kinds or "lossy_coercion" in kinds


def _create_new_row(inferred_type: str) -> dict:
    from services.mapping_pipeline import run_mapping_pipeline

    result = run_mapping_pipeline(
        source_columns=["created_at"],
        target_columns=[],
        source_schemas=[
            {
                "name": "created_at",
                "inferred_type": inferred_type,
                "samples": ["2024-01-01T00:00:00Z"],
            },
        ],
        destination_db_type="mysql",
        destination_table_exists=False,
        use_llm=False,
    )
    row = result["mappings"][0]
    assert row.get("create_new") is True
    return row


def test_mapping_pipeline_stamps_create_new_type_risks():
    # Nanosecond source into MySQL's 6-digit carrier — the truncation is real.
    row = _create_new_row("TIMESTAMPTZ(9)")
    risks = row.get("create_new_risks") or []
    assert risks, (
        f"expected create_new_risks for TIMESTAMPTZ(9)→MySQL TIMESTAMP(6), got {row}"
    )
    assert row.get("requires_review") is True
    kinds = {r.get("kind") for r in risks}
    assert kinds & {
        "timezone_polarity",
        "lossy_coercion",
        "precision_collapse",
    }


def test_mapping_pipeline_keeps_the_instant_carrier_without_a_contract():
    """MySQL ``TIMESTAMP(6)`` keeps the instant — its 2038 ceiling is the only cost.

    Polarity and precision survive, so no Risk Contract is demanded. The carrier
    still holds only 1970..2038 of the source's range, which is a review chip
    rather than the silence that let out-of-range rows fail at the write.
    """
    row = _create_new_row("TIMESTAMPTZ")
    assert row["target_type"].upper().startswith("TIMESTAMP(6)"), row["target_type"]
    assert {r.get("kind") for r in row.get("create_new_risks") or []} == {
        "instant_range_cap"
    }
    assert row.get("requires_risk_contract") is False
