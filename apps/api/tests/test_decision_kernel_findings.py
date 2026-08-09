"""Canonical Validation Finding model — Decision Kernel (Validate SSOT)."""

from __future__ import annotations

from services.coercion_probe import analyze_coercion
from services.decision_kernel import (
    FailureClass,
    classify_transform_failure,
    rank_suggested_target_type,
)
from services.transform_resolver import resolve_transform


def test_fractional_to_int_classifies_as_precision_loss():
    fc = classify_transform_failure(
        "Invalid integer: '94.5'",
        source_type="FLOAT",
        target_type="INT",
        source_value="94.5",
    )
    assert fc is FailureClass.FRACTIONAL_PRECISION_LOSS
    # Gate prose alone (no source_value kwarg) must still recover the fraction.
    assert (
        classify_transform_failure("Invalid integer: '94.5'")
        is FailureClass.FRACTIONAL_PRECISION_LOSS
    )


def test_empty_datetime_classifies_as_nullability_policy():
    fc = classify_transform_failure(
        "Empty value cannot coerce to datetime",
        source_type="TEXT",
        target_type="DATETIME",
        source_value="",
    )
    assert fc is FailureClass.EMPTY_VALUE_NOT_NULLABLE


def test_rank_suggests_double_before_longtext_for_float_to_int():
    suggested = rank_suggested_target_type(
        source_type="FLOAT",
        target_type="INT",
        dest_db="mysql",
        failure_class=FailureClass.FRACTIONAL_PRECISION_LOSS,
        failure_examples=["94.5", "96.77777777777777"],
    )
    upper = suggested.upper()
    assert "DOUBLE" in upper or "FLOAT" in upper or "DECIMAL" in upper
    assert "LONGTEXT" not in upper
    assert upper not in {"TEXT", "VARCHAR", "STRING", "LONGTEXT"}


def test_empty_value_does_not_suggest_text_widen():
    suggested = rank_suggested_target_type(
        source_type="TEXT",
        target_type="DATETIME",
        dest_db="mysql",
        failure_class=FailureClass.EMPTY_VALUE_NOT_NULLABLE,
        failure_examples=[""],
    )
    assert suggested == ""


def test_coercion_probe_suggests_numeric_widen_for_ats_score():
    report = analyze_coercion(
        sample_rows=[
            {"metadata_atsScore": "90"},
            {"metadata_atsScore": "94.5"},
            {"metadata_atsScore": "96.77777777777777"},
        ],
        mappings=[
            {
                "source": "metadata_atsScore",
                "target": "metadata_ats_score",
                "create_new": True,
                "target_type": "INT",
            }
        ],
        source_types={"metadata_atsScore": "FLOAT"},
        dest_types={},
        dest_db_type="mysql",
        table_exists=False,
    )
    col = report["by_source"]["metadata_atsScore"]
    assert col["severity"] == "block"
    assert col["failure_class"] == FailureClass.FRACTIONAL_PRECISION_LOSS.value
    suggested = (col.get("suggested_target_type") or "").upper()
    assert "DOUBLE" in suggested or "FLOAT" in suggested or "DECIMAL" in suggested
    assert "LONGTEXT" not in suggested


def test_widen_to_longtext_clears_stale_integer_transform_without_contract():
    """Operator Widen to LONGTEXT must not keep cast_integer (Validate lie)."""
    transform = resolve_transform(
        {
            "source": "metadata_atsScore",
            "target": "metadata_ats_score",
            "target_type": "LONGTEXT",
            "transform": "cast_integer",
            "user_override": True,
            "create_new": True,
        },
        column_types={"metadata_atsScore": "FLOAT"},
    )
    assert transform == "none"


def test_cast_and_continue_keeps_integer_on_text_stamp():
    from services.migration_risk_contract import create_migration_risk_contract

    rc = create_migration_risk_contract(
        column="amt",
        source_type="TEXT",
        destination_type="TEXT",
        approved_by="test@dataflow.app",
        reason="TEXT stamp with intentional integer cast under CAST_AND_CONTINUE",
        execution_policy="CAST_AND_CONTINUE",
        transform="integer",
    ).to_dict()
    transform = resolve_transform(
        {
            "source": "amt",
            "target": "amt",
            "target_type": "TEXT",
            "transform": "integer",
            "user_override": True,
            "risk_contract": rc,
        },
        column_types={"amt": "TEXT"},
    )
    assert transform == "integer"
